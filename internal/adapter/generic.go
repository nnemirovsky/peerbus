package adapter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/nnemirovsky/peerbus/internal/mcp"
	"github.com/nnemirovsky/peerbus/internal/wire"
)

// Generic adapter (--adapter=generic).
//
// This is the plain stdio MCP server mode: a host agent spawns one
// `peerbus adapter --adapter=generic` per drain-agent, drives
// bus.send/bus.broadcast/bus.peers
// on demand, and calls bus.drain on its OWN schedule (there is no
// push-wake; that is the cc adapter's job, deferred under the generic-only
// plan variant).
//
// It REUSES the Task 9 machinery and does not reimplement any of it:
//   - the broker WS client + reconnect/resume loop (ResumingClient),
//   - the SHARED consumer-side dedupe cache (rc.Dedupe()),
//   - HMAC sign (outbound) / verify (inbound) — both inside Client.
//
// The MCP layer (internal/mcp) is purely the JSON-RPC tool surface; this
// file is the only place the two are wired together (genericBus implements
// mcp.Bus over the resuming client).
//
// ── HMAC end-to-end, broadcast included ──
//
// The broker no longer mutates the signed envelope on broadcast fan-out:
// the persisted/delivered envelope bytes are the sender's verbatim signed
// envelope (to:"*", original id, original hmac); the per-recipient routing
// + ack identity rides on wire.Deliver.DeliveryKey, OUTSIDE the HMAC
// canonical subset (see internal/broker/router.go routeBroadcast). Both
// direct and broadcast deliveries therefore verify end-to-end at
// Client.Recv (a compromised broker cannot forge or tamper with a peer's
// message), and broadcast copies surface to the host like any other
// message.

// genericBus implements mcp.Bus over a ResumingClient. Inbound messages the
// resume loop has verified + deduped are buffered here; bus.drain empties
// the buffer (the resume loop already acked each one via consume-then-ack).
type genericBus struct {
	rc *ResumingClient

	mu      sync.Mutex
	pending []mcp.InboundMessage
}

// handle is the ResumingClient HandlerFunc. By the time it is called the
// envelope is already HMAC-verified (Client.Recv) and already deduped
// (ResumingClient.surface). Buffering it and returning nil means "consumed"
// so the resume loop acks it immediately — bus.drain then just hands the
// buffered, already-acked messages to the host. Broadcast copies reach
// here too: they are end-to-end HMAC-verified (the broker delivers the
// sender's verbatim signed envelope, original id) and buffered like any
// direct message.
func (b *genericBus) handle(_ context.Context, env wire.Envelope) error {
	b.mu.Lock()
	b.pending = append(b.pending, mcp.InboundMessage{
		ID:     env.ID,
		From:   env.From,
		Source: env.Source,
		Body:   env.Body,
	})
	b.mu.Unlock()
	return nil
}

// take returns and clears the buffered messages.
func (b *genericBus) take() []mcp.InboundMessage {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.pending
	b.pending = nil
	return out
}

func (b *genericBus) Send(ctx context.Context, to string, body json.RawMessage) error {
	c := b.rc.Client()
	if c == nil {
		return ErrNotConnected
	}
	return c.Send(ctx, newMsgID(), to, nowTS(), defaultSource, body)
}

func (b *genericBus) Broadcast(ctx context.Context, body json.RawMessage) error {
	c := b.rc.Client()
	if c == nil {
		return ErrNotConnected
	}
	return c.Broadcast(ctx, newMsgID(), nowTS(), defaultSource, body)
}

// Peers requests the broker registry. The resume loop is the SOLE reader of
// the WS connection, so this must NOT read frames itself (two readers split
// frames and deadlock). It installs a one-shot sink the Recv loop forwards
// the peers reply to, writes the request, and waits for the sink.
func (b *genericBus) Peers(ctx context.Context) ([]string, error) {
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
		return nil, fmt.Errorf("adapter: peers reply timed out")
	}
}

func (b *genericBus) Drain(_ context.Context) ([]mcp.InboundMessage, error) {
	return b.take(), nil
}

// NewGenericBus wires a broker-backed mcp.Bus over a fresh ResumingClient
// and starts its reconnect/resume + drain loop in the background. It is the
// embeddable entry point the generic Mode uses internally and the seam the
// MCP server integration tests drive (so the full real path — broker WS
// client + reconnect + shared dedupe + HMAC verify/sign — is exercised, not
// a fake). Call stop() to cancel the loop and release the broker
// connection (the adapter's stdio session owns this lifecycle).
func NewGenericBus(ctx context.Context, cfg ClientConfig, dedupeSize int, log *slog.Logger) (mcp.Bus, func()) {
	if log == nil {
		// Discard sink: a nil logger means "no logging", not "default to
		// stderr" — the embedding mode/test owns log routing.
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	ctx, cancel := context.WithCancel(ctx)
	rc := NewResumingClient(cfg, dedupeSize)
	bus := &genericBus{rc: rc}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := rc.Run(ctx, bus.handle); err != nil && ctx.Err() == nil {
			log.Error("generic: resume loop exited", "err", err)
		}
	}()
	stop := func() {
		cancel()
		<-done
	}
	return bus, stop
}

// genericMode is the Mode for --adapter=generic. It owns the resuming
// client's lifecycle (its drain loop) and the stdio MCP server; Run blocks
// until ctx is cancelled or stdin closes.
type genericMode struct {
	cfg        ClientConfig
	dedupeSize int
}

func (m *genericMode) Name() string { return "generic" }

// Run starts the resume/drain loop in the background (it pumps inbound
// deliveries through HMAC-verify + shared dedupe into the bus buffer) and
// serves the stdio MCP protocol in the foreground. When stdin closes (host
// gone) the MCP server returns and the resume loop is cancelled — the
// adapter lifecycle is bound to its stdio session exactly like the design
// requires (no orphaned broker connection).
func (m *genericMode) Run(ctx context.Context) error {
	bus, stop := NewGenericBus(ctx, m.cfg, m.dedupeSize, nil)
	defer stop()
	srv := mcp.NewServer(bus, os.Stdin, os.Stdout)
	return srv.Serve(ctx)
}

const defaultSource = "peer-bus"

// newMsgID returns a fresh unique message id. The adapter mode owns id
// generation (Client.Send takes the id). 128 bits of crypto randomness as
// hex is collision-free for this use and keeps the binary dependency-light
// (no uuid/ulid module promoted just to mint an id).
func newMsgID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// nowTS is the envelope ts (RFC3339 UTC). body is hashed verbatim; ts is a
// signed scalar field so it must be stable per message — generated once
// here at send time.
func nowTS() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func init() {
	// Overwrite the Task 9 placeholder with the real generic constructor.
	// Additive: no central switch edited (Register is last-wins) — exactly
	// the mode.go contract.
	// A zero ClientConfig is used by the Task 9 dispatch unit test (it only
	// asserts Name()); construction always succeeds and the real binary
	// supplies a full config.
	Register("generic", func(cfg ClientConfig, dedupeSize int) (Mode, error) {
		return &genericMode{cfg: cfg, dedupeSize: dedupeSize}, nil
	})
}
