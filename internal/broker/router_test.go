package broker

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/nnemirovsky/peerbus/internal/audit"
	bhmac "github.com/nnemirovsky/peerbus/internal/hmac"
	"github.com/nnemirovsky/peerbus/internal/wire"
)

// registerOK dials, registers under name/token and consumes the handshake
// peers ack. It returns the live connection and its context.
func registerOK(t *testing.T, url, name, token string) (*websocket.Conn, context.Context) {
	t.Helper()
	c, ctx := dial(t, url)
	sendJSON(t, ctx, c, regFrame(name, token))
	var ack wire.Peers
	readJSON(t, ctx, c, &ack)
	if ack.Type != wire.ControlPeers {
		t.Fatalf("%s: handshake ack type = %q, want peers", name, ack.Type)
	}
	return c, ctx
}

// mkEnv builds an HMAC-signed envelope from→to with the given id.
func mkEnv(t *testing.T, id, from, to string, kind wire.Kind) wire.Envelope {
	t.Helper()
	env := wire.Envelope{
		ProtocolVersion: wire.ProtocolVersion,
		ID:              id,
		From:            from,
		To:              to,
		TS:              "t",
		Source:          "peer-bus",
		Kind:            kind,
		Body:            json.RawMessage(`{"hi":1}`),
	}
	signed, err := bhmac.SignEnvelope([]byte(testSecret), env)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return signed
}

// readDeliver reads one frame and asserts it is a deliver.
func readDeliver(t *testing.T, ctx context.Context, c *websocket.Conn) wire.Deliver {
	t.Helper()
	var del wire.Deliver
	readJSON(t, ctx, c, &del)
	if del.Type != wire.ControlDeliver {
		t.Fatalf("frame type = %q, want deliver", del.Type)
	}
	return del
}

// TestRouter_DirectOnlineDelivery: a direct send to a connected peer is
// delivered immediately and is HMAC-verifiable end-to-end.
func TestRouter_DirectOnlineDelivery(t *testing.T) {
	url, _ := newTestServer(t, "tok")

	bob, _ := registerOK(t, url, "bob", "tok")
	alice, actx := registerOK(t, url, "alice", "tok")

	env := mkEnv(t, "m1", "bob", "alice", wire.KindMsg)
	sendJSON(t, actx, bob, env)

	del := readDeliver(t, actx, alice)
	if del.Envelope.ID != "m1" {
		t.Fatalf("delivered id = %q, want m1", del.Envelope.ID)
	}
	if err := bhmac.VerifyEnvelope([]byte(testSecret), del.Envelope); err != nil {
		t.Fatalf("delivered envelope failed HMAC verify: %v", err)
	}
}

// TestRouter_DirectOfflineThenReconnectDrains: a send to a registered but
// disconnected peer is queued and drained when it reconnects.
func TestRouter_DirectOfflineThenReconnectDrains(t *testing.T) {
	url, _ := newTestServer(t, "tok")

	// alice registers (so she is a known peer) then disconnects.
	alice1, _ := registerOK(t, url, "alice", "tok")
	_ = alice1.Close(websocket.StatusNormalClosure, "")
	// Give the broker a moment to observe the close and unbind.
	time.Sleep(100 * time.Millisecond)

	bob, bctx := registerOK(t, url, "bob", "tok")
	sendJSON(t, bctx, bob, mkEnv(t, "off1", "bob", "alice", wire.KindMsg))
	// Let the enqueue settle before alice reconnects.
	time.Sleep(100 * time.Millisecond)

	alice2, a2ctx := registerOK(t, url, "alice", "tok")
	del := readDeliver(t, a2ctx, alice2)
	if del.Envelope.ID != "off1" {
		t.Fatalf("reconnect drain delivered id = %q, want off1", del.Envelope.ID)
	}
}

// TestRouter_BroadcastFanoutSenderExcluded: a to:* broadcast reaches every
// other registered peer but NOT the sender.
func TestRouter_BroadcastFanoutSenderExcluded(t *testing.T) {
	url, _ := newTestServer(t, "tok")

	sender, sctx := registerOK(t, url, "sender", "tok")
	a, actx := registerOK(t, url, "a", "tok")
	b, bctx := registerOK(t, url, "b", "tok")

	sendJSON(t, sctx, sender, mkEnv(t, "bc1", "sender", "*", wire.KindBroadcast))

	da := readDeliver(t, actx, a)
	db := readDeliver(t, bctx, b)
	// The signed envelope is delivered VERBATIM (to:"*", original id) so
	// the sender's end-to-end HMAC verifies for every recipient. The
	// broker no longer rewrites it.
	if da.Envelope.To != "*" || db.Envelope.To != "*" {
		t.Fatalf("broadcast envelope must stay verbatim to:*; a=%q b=%q", da.Envelope.To, db.Envelope.To)
	}
	if da.Envelope.ID != "bc1" || db.Envelope.ID != "bc1" {
		t.Fatalf("broadcast signed id must stay verbatim; a=%q b=%q", da.Envelope.ID, db.Envelope.ID)
	}
	// Per-recipient routing/ack identity lives on the Deliver frame's
	// DeliveryKey (the durable per-recipient row key), OUTSIDE the HMAC.
	if da.DeliveryKey != "bc1|a" || db.DeliveryKey != "bc1|b" {
		t.Fatalf("per-recipient delivery_key wrong: a=%q b=%q", da.DeliveryKey, db.DeliveryKey)
	}

	// Sender must NOT receive its own broadcast: a short read times out.
	rctx, cancel := context.WithTimeout(sctx, 300*time.Millisecond)
	defer cancel()
	if _, _, err := sender.Read(rctx); websocket.CloseStatus(err) != -1 {
		t.Fatalf("sender received its own broadcast or conn closed: %v", err)
	}
}

// TestRouter_BroadcastNoBackfill: a peer that registers AFTER a broadcast
// does not receive it (no late-joiner backfill).
func TestRouter_BroadcastNoBackfill(t *testing.T) {
	url, _ := newTestServer(t, "tok")

	sender, sctx := registerOK(t, url, "sender", "tok")
	early, ectx := registerOK(t, url, "early", "tok")

	sendJSON(t, sctx, sender, mkEnv(t, "bc-nb", "sender", "*", wire.KindBroadcast))

	// early (registered before the broadcast) receives it.
	de := readDeliver(t, ectx, early)
	if de.Envelope.ID == "" {
		t.Fatalf("early peer did not receive the broadcast")
	}

	// late registers AFTER the broadcast — must receive nothing.
	late, lctx := registerOK(t, url, "late", "tok")
	rctx, cancel := context.WithTimeout(lctx, 400*time.Millisecond)
	defer cancel()
	if _, _, err := late.Read(rctx); websocket.CloseStatus(err) != -1 {
		t.Fatalf("late joiner received a backfilled broadcast or conn closed: %v", err)
	}
}

// TestRouter_RedeliverSameIDOnReconnect: the broker re-delivers the SAME id
// on reconnect when the first delivery was not acked. Consumer-side dedupe
// is Task 9 — here we only assert the broker resends the same id.
func TestRouter_RedeliverSameIDOnReconnect(t *testing.T) {
	url, _ := newTestServer(t, "tok")

	bob, _ := registerOK(t, url, "bob", "tok")
	alice1, a1ctx := registerOK(t, url, "alice", "tok")

	sendJSON(t, a1ctx, bob, mkEnv(t, "redel1", "bob", "alice", wire.KindMsg))
	d1 := readDeliver(t, a1ctx, alice1)
	if d1.Envelope.ID != "redel1" {
		t.Fatalf("first delivery id = %q, want redel1", d1.Envelope.ID)
	}

	// alice drops WITHOUT acking, then reconnects: the unacked message must
	// be redelivered with the same id (RequeueUnacked + PendingFor flush).
	_ = alice1.Close(websocket.StatusNormalClosure, "")
	time.Sleep(100 * time.Millisecond)

	alice2, a2ctx := registerOK(t, url, "alice", "tok")
	d2 := readDeliver(t, a2ctx, alice2)
	if d2.Envelope.ID != "redel1" {
		t.Fatalf("redelivered id = %q, want the SAME redel1", d2.Envelope.ID)
	}
}

// TestRouter_AckStopsRedelivery: once acked, a message is NOT redelivered
// on reconnect.
func TestRouter_AckStopsRedelivery(t *testing.T) {
	url, _ := newTestServer(t, "tok")

	bob, _ := registerOK(t, url, "bob", "tok")
	alice1, a1ctx := registerOK(t, url, "alice", "tok")

	sendJSON(t, a1ctx, bob, mkEnv(t, "ackme", "bob", "alice", wire.KindMsg))
	d1 := readDeliver(t, a1ctx, alice1)

	sendJSON(t, a1ctx, alice1, wire.Ack{
		ProtocolVersion: wire.ProtocolVersion,
		Type:            wire.ControlAck,
		ID:              d1.Envelope.ID,
	})
	time.Sleep(100 * time.Millisecond)
	_ = alice1.Close(websocket.StatusNormalClosure, "")
	time.Sleep(100 * time.Millisecond)

	alice2, a2ctx := registerOK(t, url, "alice", "tok")
	rctx, cancel := context.WithTimeout(a2ctx, 400*time.Millisecond)
	defer cancel()
	if _, _, err := alice2.Read(rctx); websocket.CloseStatus(err) != -1 {
		t.Fatalf("acked message was redelivered or conn closed: %v", err)
	}
}

// TestRouter_AckUnknownIDGraceful: acking an id that does not exist is a
// graceful no-op (the connection stays usable).
func TestRouter_AckUnknownIDGraceful(t *testing.T) {
	url, _ := newTestServer(t, "tok")

	bob, bctx := registerOK(t, url, "bob", "tok")
	_, _ = registerOK(t, url, "alice", "tok")

	sendJSON(t, bctx, bob, wire.Ack{
		ProtocolVersion: wire.ProtocolVersion,
		Type:            wire.ControlAck,
		ID:              "does-not-exist",
	})

	// Connection still works afterwards: a peers request still answers.
	sendJSON(t, bctx, bob, wire.Peers{
		ProtocolVersion: wire.ProtocolVersion,
		Type:            wire.ControlPeers,
	})
	var p wire.Peers
	readJSON(t, bctx, bob, &p)
	if p.Type != wire.ControlPeers {
		t.Fatalf("connection unusable after unknown-id ack: %+v", p)
	}
}

// TestRouter_SendToUnknownNameQueuedThenDelivered: a send addressed to a
// name that has registered (but is offline) queues and is delivered when
// that name (re)connects.
func TestRouter_SendToUnknownNameQueuedThenDelivered(t *testing.T) {
	url, _ := newTestServer(t, "tok")

	// "future" registers once so it is a known peer, then disconnects.
	f1, _ := registerOK(t, url, "future", "tok")
	_ = f1.Close(websocket.StatusNormalClosure, "")
	time.Sleep(100 * time.Millisecond)

	bob, bctx := registerOK(t, url, "bob", "tok")
	sendJSON(t, bctx, bob, mkEnv(t, "later1", "bob", "future", wire.KindMsg))
	time.Sleep(100 * time.Millisecond)

	// future reconnects later and receives the queued message.
	f2, f2ctx := registerOK(t, url, "future", "tok")
	del := readDeliver(t, f2ctx, f2)
	if del.Envelope.ID != "later1" {
		t.Fatalf("delivered id = %q, want later1 (queued for later register)", del.Envelope.ID)
	}
}

// TestRouter_AuditChainPerEventAndConcurrent asserts: each send/deliver/ack
// appends exactly-one chain-valid audit row, and concurrent sends do not
// break the chain (audit.Verify passes). The event counts are derived from
// the audit rows themselves so the assertion does not depend on timing.
func TestRouter_AuditChainPerEventAndConcurrent(t *testing.T) {
	url, st := newTestServer(t, "tok")

	// One recipient online so every send produces send+deliver, plus an ack.
	alice, actx := registerOK(t, url, "alice", "tok")
	sendersN := 6

	var wg sync.WaitGroup
	for i := 0; i < sendersN; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := "s" + string(rune('A'+i))
			c, ctx := registerOK(t, url, name, "tok")
			id := "cc-" + name
			sendJSON(t, ctx, c, mkEnv(t, id, name, "alice", wire.KindMsg))
		}(i)
	}
	wg.Wait()

	// Drain + ack everything alice receives so we get ack events too.
	acked := 0
	for acked < sendersN {
		rctx, cancel := context.WithTimeout(actx, time.Second)
		var del wire.Deliver
		_, b, err := alice.Read(rctx)
		cancel()
		if err != nil {
			break
		}
		if json.Unmarshal(b, &del) != nil || del.Type != wire.ControlDeliver {
			continue
		}
		sendJSON(t, actx, alice, wire.Ack{
			ProtocolVersion: wire.ProtocolVersion,
			Type:            wire.ControlAck,
			ID:              del.Envelope.ID,
		})
		acked++
	}
	// Let the final ack audit rows land.
	time.Sleep(200 * time.Millisecond)

	// Chain must be intact despite concurrent sends.
	if brk, err := audit.Verify(st); err != nil {
		t.Fatalf("audit.Verify error: %v", err)
	} else if brk != nil {
		t.Fatalf("audit chain broken: %v", brk)
	}

	// Exactly one row per event; classify by the "event" field.
	rows, err := st.AuditRows()
	if err != nil {
		t.Fatalf("audit rows: %v", err)
	}
	counts := map[string]int{}
	for _, r := range rows {
		var ev struct {
			Event string `json:"event"`
			ID    string `json:"id"`
		}
		if err := json.Unmarshal(r.Event, &ev); err != nil {
			t.Fatalf("audit row %d not JSON: %v", r.Seq, err)
		}
		counts[ev.Event]++
	}
	if counts["send"] != sendersN {
		t.Fatalf("send audit rows = %d, want %d", counts["send"], sendersN)
	}
	if counts["deliver"] != sendersN {
		t.Fatalf("deliver audit rows = %d, want %d", counts["deliver"], sendersN)
	}
	if counts["ack"] != acked || acked != sendersN {
		t.Fatalf("ack audit rows = %d, acked = %d, want %d each", counts["ack"], acked, sendersN)
	}
}
