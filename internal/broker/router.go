// This file implements Task 8: post-handshake routing — direct send,
// broadcast fan-out, the offline/pending queue, ack handling, and
// reconnect redelivery — plus audit emission for send/deliver/ack.
//
// Delivery model (locked by the plan's Solution Overview "Delivery"
// bullet, restated here because this file is where it is enforced):
//
//   - at-least-once: a message is persisted via store.Enqueue BEFORE any
//     attempt to push it; an undelivered or delivered-but-unacked row
//     survives a broker restart and is (re)delivered on reconnect.
//   - dedupe-by-id: the store's UNIQUE(id) constraint drops a duplicate
//     enqueue (ErrDuplicateID); the broker treats that as a benign no-op.
//     Consumer-side dedupe of *redelivered* ids is Task 9 — NOT here.
//   - per-sender FIFO: ordering is the store's job (monotonic per-sender
//     seq); the router just drains store.PendingFor in the order given.
//   - broadcast, no backfill: a to:* message fans out to the peers
//     registered AT SEND TIME except the sender; each recipient gets its
//     OWN durable row (a per-recipient envelope id) and acks independently.
//     A peer that registers AFTER the broadcast does NOT receive it (there
//     is no backfill of a name into an already-sent broadcast).
//
// Audit/persist durability boundary (explicit, accepted): store.Enqueue
// and the "send" audit append are SEPARATE transactions (Enqueue commits
// the message row; auditEvent then appends one audit row through the
// single Appender). A broker crash in the window between the two leaves a
// durably-queued message with NO "send" audit row. This is an accepted
// boundary, not a bug: the audit chain stays hash-valid (it is append-only
// and never references the message row by FK), it may simply OMIT a send
// event for a message that was nonetheless delivered at-least-once. The
// alternative — one transaction spanning store + audit — would force the
// audit hash-chain write (internal/audit, a separate package that
// serialises its own single-writer Append) into store's Enqueue tx,
// coupling the two packages and the single-writer invariant in a way that
// is strictly worse than this narrow, documented gap. The delivery
// guarantee (at-least-once, durable) is unaffected; only audit
// completeness has this crash-window caveat. Documented in
// docs/wire-protocol.md as well.
//
// Audit single-writer invariant (load-bearing): the blake3 hash chain is
// only well defined if appends are serialised. Every send/deliver/ack
// audit event in this file goes through ONE broker-owned *audit.Appender
// (s.audit). audit.Appender already guards Append with its own mutex, so
// concurrent connection goroutines calling s.auditEvent here cannot fork
// the chain. Do NOT introduce a second Appender or write audit rows by any
// other path — that would break audit.Verify.

package broker

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/nnemirovsky/peerbus/internal/store"
	"github.com/nnemirovsky/peerbus/internal/wire"
)

// auditEvent appends one audit row through the single broker-owned
// Appender. kind is one of "send"/"deliver"/"ack". The event blob is a
// compact JSON object; it is hashed verbatim by the audit chain (the
// audit package owns the chain link, this is just the canonical payload).
// A nil Appender (no audit configured) is a no-op so the broker still
// routes when audit is disabled.
func (s *Server) auditEvent(kind, id, from, to string) {
	if s.audit == nil {
		return
	}
	// Fixed key order via a struct so the marshalled bytes are stable for
	// the hash chain. The Appender serialises the actual write.
	ev := struct {
		Event string `json:"event"`
		ID    string `json:"id"`
		From  string `json:"from"`
		To    string `json:"to"`
	}{Event: kind, ID: id, From: from, To: to}
	b, err := json.Marshal(ev)
	if err != nil {
		s.log.Warn("audit marshal failed", "event", kind, "id", id, "err", err)
		return
	}
	if _, err := s.audit.Append(b); err != nil {
		s.log.Warn("audit append failed", "event", kind, "id", id, "err", err)
	}
}

// deliverTo pushes one already-persisted message to a recipient if that
// recipient is currently connected, and marks it delivered. It returns
// true iff the frame was written to a live connection. If the recipient
// is offline the row is left undelivered (the offline/pending path);
// flushPending will pick it up on the recipient's next register.
func (s *Server) deliverTo(ctx context.Context, recipient string, m store.Message) bool {
	conn, ok := s.registry.Get(recipient)
	if !ok {
		return false
	}
	pc, ok := conn.(*peerConn)
	if !ok {
		return false
	}
	var env wire.Envelope
	if err := json.Unmarshal(m.Envelope, &env); err != nil {
		s.log.Warn("skip unparseable envelope", "id", m.ID, "err", err)
		return false
	}
	del := wire.Deliver{
		ProtocolVersion: wire.ProtocolVersion,
		Type:            wire.ControlDeliver,
		// DeliveryKey is the durable per-recipient row key (m.ID). For a
		// direct message it equals env.ID; for a broadcast copy it is
		// "<origID>|<recipient>" while env stays the sender's verbatim
		// signed envelope (to:"*", original id) so the recipient's HMAC
		// verifies. The recipient acks by this key.
		DeliveryKey: m.ID,
		Envelope:    env,
	}
	if err := pc.writeJSON(ctx, del); err != nil {
		s.log.Warn("deliver failed", "id", m.ID, "to", recipient, "err", err)
		return false
	}
	if err := s.store.MarkDelivered(m.ID); err != nil {
		s.log.Warn("mark delivered failed", "id", m.ID, "err", err)
	}
	s.auditEvent("deliver", m.ID, m.From, recipient)
	return true
}

// routeEnvelope handles a post-handshake data-channel Envelope from pc.
// from is the sender's bound (authenticated) name; the broker trusts the
// connection's bound identity, not env.From, for routing/audit purposes
// (env.From is still carried verbatim end-to-end and HMAC-protected).
//
//   - to == "*" AND kind == broadcast → broadcast: fan out to every
//     currently-registered peer except the sender, each with its OWN
//     durable row keyed per recipient.
//   - to != "*" AND kind == msg → direct: persist for env.To, deliver if
//     connected else leave queued (offline path).
//
// A mismatched combination (to=="*" with kind!=broadcast, or kind==
// broadcast with to!="*") is rejected: the two fields are redundant by
// design and an inconsistent pair is a malformed/forged frame, not a
// best-effort routing hint. Accepting it via OR-logic would let a
// kind==msg, to=="*" frame fan out (or a kind==broadcast, to=="bob" frame
// be treated direct) — both are protocol violations.
func (s *Server) routeEnvelope(ctx context.Context, from string, env wire.Envelope) {
	isBroadcast := env.To == "*"
	if isBroadcast != (env.Kind == wire.KindBroadcast) {
		s.log.Warn("inconsistent kind/to combination rejected",
			"id", env.ID, "to", env.To, "kind", env.Kind)
		return
	}
	if isBroadcast {
		s.routeBroadcast(ctx, from, env)
		return
	}
	s.routeDirect(ctx, from, env)
}

// routeDirect persists then (best-effort) delivers a direct message. The
// recipient need not be online: an unknown/offline name simply leaves a
// delivered=0 row that is flushed when that name registers. An unknown
// recipient name is therefore NOT an error — store.Enqueue rejects only a
// name that was never registered as a peer at all, which the broker
// tolerates by still auditing the send (the message is dropped only when
// the recipient was never a known peer; a known-but-offline peer queues).
func (s *Server) routeDirect(ctx context.Context, from string, env wire.Envelope) {
	raw, err := json.Marshal(env)
	if err != nil {
		s.log.Warn("marshal envelope failed", "id", env.ID, "err", err)
		return
	}
	msg := store.Message{ID: env.ID, From: from, To: env.To, Envelope: raw}
	err = s.store.Enqueue(msg)
	switch {
	case err == nil:
		s.auditEvent("send", env.ID, from, env.To)
	case errors.Is(err, store.ErrDuplicateID):
		// Dedupe-by-id: a re-sent id is a benign no-op (at-least-once
		// means senders may retry). The original row stands.
		s.log.Info("duplicate send id ignored", "id", env.ID)
		return
	case errors.Is(err, store.ErrUnknownPeer):
		// Recipient name was never a registered peer. The store cannot
		// hold a row for an unknown peer; nothing to deliver. This is the
		// only "send to unknown name" path that does not queue — a name
		// that registered once (even if now offline) DOES queue above.
		s.log.Info("send to unknown peer dropped", "id", env.ID, "to", env.To)
		return
	default:
		s.log.Warn("enqueue failed", "id", env.ID, "err", err)
		return
	}
	s.deliverTo(ctx, env.To, msg)
}

// routeBroadcast fans a to:* envelope out to every currently-registered
// peer EXCEPT the sender. Each recipient gets its OWN durable row keyed by
// a per-recipient row id ("<id>|<recipient>") so each can be
// acked/redelivered independently and the store's UNIQUE(id) does not
// collapse the N copies into one. NO backfill: the recipient set is
// snapshotted here, at send time; a peer that registers later does not
// receive this broadcast.
//
// CRITICAL (end-to-end HMAC, load-bearing): the broker does NOT mutate or
// re-marshal the signed wire.Envelope. The persisted/delivered envelope
// bytes are the sender's ORIGINAL signed envelope (to:"*", original id,
// original hmac). The per-recipient routing/dedupe/ack identity lives ONLY
// on the store row key (store.Message.ID) and is carried to the recipient
// on the wire.Deliver control frame's DeliveryKey field, which is NOT in
// the HMAC canonical subset. The recipient therefore verifies exactly what
// the sender signed (a compromised broker cannot forge a broadcast copy)
// and acks by the row key. Direct messages take the same shape with
// DeliveryKey == env.ID (routeDirect / deliverTo).
func (s *Server) routeBroadcast(ctx context.Context, from string, env wire.Envelope) {
	// The sender's verbatim signed envelope — marshalled ONCE, byte-stable,
	// identical for every recipient. Never re-marshalled per recipient.
	raw, err := json.Marshal(env)
	if err != nil {
		s.log.Warn("marshal broadcast envelope failed", "id", env.ID, "err", err)
		return
	}
	recipients := s.registry.List() // snapshot at send time — no backfill
	for _, r := range recipients {
		if r == from {
			continue // sender exclusion
		}
		// Per-recipient durable row key only — the envelope bytes are the
		// untouched signed original; the recipient name lives on the row
		// (To) and the Deliver frame's DeliveryKey, NOT inside the signed
		// envelope.
		rowID := env.ID + "|" + r
		msg := store.Message{ID: rowID, From: from, To: r, Envelope: raw}
		err = s.store.Enqueue(msg)
		switch {
		case err == nil:
			s.auditEvent("send", rowID, from, r)
		case errors.Is(err, store.ErrDuplicateID):
			continue
		case errors.Is(err, store.ErrUnknownPeer):
			// The recipient was never a registered peer, so the store
			// cannot durably hold this fan-out copy and it is dropped for
			// that recipient. With store.Register now ordered before
			// registry.Bind (see handshake, MODERATE-R6) a registry-listed
			// name always has a peer row, so this should not happen for a
			// genuinely-registered recipient; log it (NOT a silent continue)
			// so a real loss is observable rather than invisible.
			s.log.Warn("broadcast copy dropped: recipient not a known peer",
				"id", rowID, "to", r)
			continue
		default:
			s.log.Warn("broadcast enqueue failed", "id", rowID, "err", err)
			continue
		}
		s.deliverTo(ctx, r, msg)
	}
}

// handleAck marks the acked id consumed so RequeueUnacked never redelivers
// it. An ack for an unknown id is a graceful no-op (store.MarkAcked is
// already a no-op on unknown ids; at-least-once means a late/duplicate ack
// is expected). The ack is still audited for traceability.
func (s *Server) handleAck(ack wire.Ack, from string) {
	if ack.ID == "" {
		return
	}
	if err := s.store.MarkAcked(ack.ID); err != nil {
		s.log.Warn("mark acked failed", "id", ack.ID, "err", err)
		return
	}
	s.auditEvent("ack", ack.ID, from, "")
}

// handlePeers replies with the current registry list.
func (s *Server) handlePeers(ctx context.Context, pc *peerConn) {
	resp := wire.Peers{
		ProtocolVersion: wire.ProtocolVersion,
		Type:            wire.ControlPeers,
		Names:           s.registry.List(),
	}
	if err := pc.writeJSON(ctx, resp); err != nil {
		s.log.Warn("peers reply failed", "to", pc.name, "err", err)
	}
}

// routeFrame classifies and dispatches one post-handshake frame. A frame
// is either a typed control object (ack/peers/register) or a data-channel
// Envelope. A re-sent register on an established connection is ignored
// here (re-register is a fresh connection in this transport model; the
// pending-flush on (re)register lives in server.go's handshake path).
func (s *Server) routeFrame(ctx context.Context, pc *peerConn, data []byte) {
	ct, err := wire.ControlTypeOf(data)
	if err == nil && ct != "" {
		switch ct {
		case wire.ControlAck:
			var ack wire.Ack
			if err := json.Unmarshal(data, &ack); err != nil {
				s.log.Warn("malformed ack frame", "err", err)
				return
			}
			s.handleAck(ack, pc.name)
			return
		case wire.ControlPeers:
			s.handlePeers(ctx, pc)
			return
		case wire.ControlRegister:
			// Re-register on a live conn is a no-op in this model.
			return
		case wire.ControlDeliver:
			// Deliver is broker→peer only; ignore if a peer sends it.
			return
		}
	}
	// Not a recognised control frame → treat as a data-channel Envelope.
	var env wire.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		s.log.Warn("unroutable frame (neither control nor envelope)", "err", err)
		return
	}
	if env.ID == "" || env.To == "" {
		s.log.Warn("envelope missing id/to", "id", env.ID, "to", env.To)
		return
	}
	s.routeEnvelope(ctx, pc.name, env)
}
