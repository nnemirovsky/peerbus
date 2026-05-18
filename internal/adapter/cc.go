package adapter

import (
	"context"
	"encoding/json"
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
// one peerbus-adapter per session over stdio (--dangerously-load-development-
// channels server:peerbus-adapter). N sessions => N short-lived adapters,
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
// forwards the peers reply to rather than reading frames here.
func (b *ccBus) Peers(ctx context.Context) ([]string, error) {
	c := b.rc.Client()
	if c == nil {
		return nil, ErrNotConnected
	}
	sink := make(chan []string, 1)
	c.SetPeersSink(sink)
	defer c.SetPeersSink(nil)
	if err := c.RequestPeers(ctx); err != nil {
		return nil, err
	}
	select {
	case names := <-sink:
		return names, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(peersReplyTimeout):
		// MAJOR-6: bound the wait. Without this a peers reply lost across
		// a reconnect would block until ctx cancel (forever for a
		// long-lived cc session).
		return nil, fmt.Errorf("adapter: peers reply timed out")
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

// Run wires the channel server to a fresh ResumingClient, auto-registers a
// unique peer name when none was configured (cc2cc-parity ergonomics; see
// channel.UniqueName for the scheme), starts the resume/dedupe/HMAC loop in
// the background (each inbound delivery becomes a claude/channel push) and
// serves the stdio MCP protocol in the foreground. When stdin closes the
// MCP server returns and the resume loop is cancelled.
func (m *ccMode) Run(ctx context.Context) error {
	cfg := m.cfg
	if cfg.Name == "" {
		cfg.Name = channel.UniqueName()
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rc := NewResumingClient(cfg, m.dedupeSize)
	bus := &ccBus{rc: rc}
	srv := channel.NewServer(bus, os.Stdin, os.Stdout)
	bus.srv = srv

	done := make(chan struct{})
	go func() {
		defer close(done)
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
