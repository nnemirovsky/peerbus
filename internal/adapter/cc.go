package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nnemirovsky/peerbus/internal/channel"
	"github.com/nnemirovsky/peerbus/internal/wire"
)

// CC channel adapter (--adapter=cc).
//
// This mode IS the Claude Code claude/channel MCP server. Claude Code spawns
// one `peerbus adapter --adapter=cc` per session over stdio
// (--dangerously-load-development-channels server:peerbus). N sessions => N
// short-lived adapters,
// each a distinct peer — exactly the cc2cc orphan bug designed out (the
// adapter lifecycle is bound to its stdio session; the broker outlives all).
//
// It REUSES the Task 9 machinery and does not reimplement any of it:
//   - the broker WS client + reconnect/resume loop (ResumingClient),
//   - the SHARED consumer-side dedupe cache (inside ResumingClient),
//   - HMAC sign (outbound) / verify (inbound) — both inside Client.
//
// internal/channel owns ONLY the claude/channel mapping (initialize
// capability + the notifications/claude/channel push) and reuses
// internal/mcp's JSON-RPC core + bus.* tool plumbing (extended additively,
// not forked). This file is the single seam wiring the two together:
//   - inbound: ResumingClient HandlerFunc -> ccBus.handle -> channel push
//   - outbound: channel.OutboundBus -> ResumingClient broker client (signed)
//
// ── HMAC end-to-end, broadcast included (identical to the generic adapter) ──
//
// The broker no longer mutates the signed envelope on broadcast fan-out;
// the per-recipient routing/ack identity rides on wire.Deliver.DeliveryKey
// outside the HMAC subset (internal/broker/router.go routeBroadcast). Both
// direct and broadcast deliveries verify end-to-end at Client.Recv and
// push-wake the session normally — a compromised broker cannot forge or
// tamper with a peer's message.

// ccBus implements channel.OutboundBus over a ResumingClient. Outbound
// bus.send/bus.broadcast/bus.peers sign via the shared client; inbound
// (handle) maps a verified+deduped delivery to a claude/channel push.
type ccBus struct {
	rc  *ResumingClient
	srv *channel.Server // set before the resume loop starts; only read in handle
}

func (b *ccBus) Send(ctx context.Context, to string, body json.RawMessage) error {
	c := b.rc.Client()
	if c == nil {
		return ErrNotConnected
	}
	return c.Send(ctx, newMsgID(), to, nowTS(), defaultSource, body)
}

func (b *ccBus) Broadcast(ctx context.Context, body json.RawMessage) error {
	c := b.rc.Client()
	if c == nil {
		return ErrNotConnected
	}
	return c.Broadcast(ctx, newMsgID(), nowTS(), defaultSource, body)
}

// Peers mirrors the generic adapter's no-second-reader pattern: the resume
// loop is the SOLE WS reader, so install a one-shot sink the Recv loop
// forwards the peers reply to rather than reading frames here. Returns
// (self, peers, err); see channel.OutboundBus / mcp.Bus for the shape
// rationale.
func (b *ccBus) Peers(ctx context.Context) (string, []string, error) {
	self := b.rc.Name()
	c := b.rc.Client()
	if c == nil {
		return self, nil, ErrNotConnected
	}
	sink := make(chan []string, 1)
	c.SetPeersSink(sink)
	defer c.SetPeersSink(nil)
	if err := c.RequestPeers(ctx); err != nil {
		return self, nil, err
	}
	select {
	case names := <-sink:
		return self, filterSelf(names, self), nil
	case <-ctx.Done():
		return self, nil, ctx.Err()
	case <-time.After(peersReplyTimeout):
		// MAJOR-6: bound the wait. Without this a peers reply lost across
		// a reconnect would block until ctx cancel (forever for a
		// long-lived cc session).
		return self, nil, fmt.Errorf("adapter: peers reply timed out")
	}
}

// handle is the ResumingClient HandlerFunc. By the time it is called the
// envelope is already HMAC-verified (Client.Recv) and already deduped
// (ResumingClient.surface). It is mapped to a claude/channel push-wake and
// returning nil means "consumed" so the resume loop acks it immediately.
// Broadcast copies reach here too: the broker delivers the sender's
// verbatim signed envelope so they HMAC-verify at Recv and push-wake the
// session like any direct message (see the package-doc HMAC note).
func (b *ccBus) handle(_ context.Context, env wire.Envelope) error {
	b.srv.Deliver(channel.Inbound{
		ID:     env.ID,
		From:   env.From,
		Source: env.Source,
		Kind:   string(env.Kind),
		Body:   env.Body,
	})
	return nil
}

// ccMode is the Mode for --adapter=cc. It owns the resuming client's
// lifecycle and the claude/channel MCP server; Run blocks until ctx is
// cancelled or stdin closes (host gone) — the adapter lifecycle is bound to
// its stdio session, the design's core anti-orphan property.
type ccMode struct {
	cfg        ClientConfig
	dedupeSize int
}

func (m *ccMode) Name() string { return "cc" }

// nameCollisionRetries bounds the on-startup name-rotation attempts when a
// freshly-minted friendly name happens to be claimed under a different
// bearer token (an "essentially impossible" event given the keyspace; see
// channel.UniqueName). 6 attempts is a conservative safety backstop:
// independent collisions on each retry are vanishingly unlikely, but
// bounding the loop prevents a misconfigured environment (e.g. a hostile
// token-sharing setup) from spinning forever.
const nameCollisionRetries = 6

// Run wires the channel server to a fresh ResumingClient, auto-mints a
// friendly peer name when none was configured (channel.UniqueName), retries
// up to nameCollisionRetries times on the broker's name-claimed rejection
// (collision-safety backstop), then starts the resume/dedupe/HMAC loop in
// the background (each inbound delivery becomes a claude/channel push) and
// serves the stdio MCP protocol in the foreground. On every successful
// register the adapter emits ONE system-kind notification carrying the
// bound name (channel.Server.AnnounceSelf) so the consuming Claude session
// always knows its own bus identity from turn 1 — no separate bus.whoami
// round-trip required. When stdin closes the MCP server returns and the
// resume loop is cancelled.
func (m *ccMode) Run(ctx context.Context) error {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg := m.cfg
	if cfg.Name == "" {
		cfg.Name = channel.UniqueName()
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Probe-register up front with name rotation on ErrNameClaimed. Doing
	// this BEFORE wiring the resume loop avoids a forever-redial spin if
	// the chosen name is permanently rejected (different-token claim). We
	// close the probe connection on success: the resume loop redials and
	// performs its OWN register under the now-validated name. Same-token
	// takeover (this peer reconnecting) is the entire resume mechanism so
	// the probe close is harmless.
	for attempt := 0; ; attempt++ {
		probe := NewClient(cfg)
		err := probe.Connect(ctx)
		if err == nil {
			probe.Close()
			break
		}
		if !errors.Is(err, ErrNameClaimed) {
			return err
		}
		if attempt >= nameCollisionRetries {
			return fmt.Errorf("cc: %d friendly-name rotations all rejected (last name %q): %w",
				nameCollisionRetries+1, cfg.Name, err)
		}
		log.Warn("cc: name claimed under different token, rotating",
			"name", cfg.Name, "attempt", attempt+1)
		// Operator override (PEERBUS_NAME) is honoured verbatim and
		// MUST NOT be rotated — a permanent rejection there is a config
		// problem, not a collision the adapter can paper over.
		if os.Getenv("PEERBUS_NAME") != "" {
			return fmt.Errorf("cc: PEERBUS_NAME=%q rejected: %w", cfg.Name, err)
		}
		cfg.Name = channel.UniqueName()
	}

	rc := NewResumingClient(cfg, m.dedupeSize)
	bus := &ccBus{rc: rc}
	srv := channel.NewServer(bus, os.Stdin, os.Stdout)
	bus.srv = srv

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Self-announcement: emit ONE system-kind claude/channel push
		// carrying the bound name BEFORE the resume loop starts pumping
		// real deliveries. This appears in turn 1 of the Claude session
		// so the consuming agent immediately knows its identity on the
		// bus — the design's self-identification guarantee. A later
		// reconnect does NOT re-announce; the name is stable across
		// reconnects (same-name re-register is the resume mechanism) so
		// a re-announce would be redundant noise.
		srv.AnnounceSelf(cfg.Name)
		if err := rc.Run(ctx, bus.handle); err != nil && ctx.Err() == nil {
			log.Error("cc: resume loop exited", "err", err)
		}
	}()

	err := srv.Serve(ctx)
	cancel()
	<-done
	return err
}

func init() {
	// Overwrite the Task 9 placeholder with the real cc constructor.
	// Additive: no central switch edited (Register is last-wins) — exactly
	// the mode.go contract, mirroring generic.go.
	Register("cc", func(cfg ClientConfig, dedupeSize int) (Mode, error) {
		return &ccMode{cfg: cfg, dedupeSize: dedupeSize}, nil
	})
}
