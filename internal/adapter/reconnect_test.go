package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nnemirovsky/peerbus/internal/wire"
)

// TestResumingClient_BackoffEscalatesOnRepeatedDialFailure: when the broker
// is unreachable, connect() must keep retrying with bounded escalating
// backoff (not spin), and Run must NOT return until ctx is cancelled. The
// timing assertion is loose (just "it backed off, did not hot-spin and did
// not give up early") so it is not flaky under -race.
func TestResumingClient_BackoffEscalatesOnRepeatedDialFailure(t *testing.T) {
	// A URL that always fails to dial (closed port).
	cfg := ClientConfig{
		URL:        "ws://127.0.0.1:1", // port 1 — connection refused
		Token:      "tok",
		Name:       "n",
		HMACSecret: testSecret,
	}
	rc := NewResumingClient(cfg, 8)

	ctx, cancel := context.WithCancel(context.Background())
	var ret atomic.Value // error
	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		ret.Store(rc.Run(ctx, func(context.Context, wire.Envelope) error { return nil }))
	}()

	// Let it fail-and-back-off a few cycles. baseBackoff is 100ms, so in
	// ~450ms it cannot have completed more than a handful of attempts —
	// proving it is backing off rather than hot-looping (a hot loop would
	// burn thousands of dials and likely never yield).
	time.Sleep(450 * time.Millisecond)
	if _, ok := ret.Load().(error); ok {
		t.Fatalf("Run returned before ctx cancel (it must keep retrying on dial failure)")
	}
	if rc.Client() != nil {
		t.Fatalf("Client() must be nil while no connection has ever succeeded")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(maxBackoff + 2*time.Second):
		t.Fatalf("Run did not return promptly after ctx cancel")
	}
	if err, _ := ret.Load().(error); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatalf("Run returned implausibly fast — backoff not exercised")
	}
}

// TestResumingClient_CtxCancelDuringConnectBackoff: cancelling ctx while
// connect() is asleep in its backoff select must unwind promptly and Run
// must return context.Canceled (not hang for a full backoff period).
func TestResumingClient_CtxCancelDuringConnectBackoff(t *testing.T) {
	cfg := ClientConfig{
		URL:        "ws://127.0.0.1:1",
		Token:      "tok",
		Name:       "n",
		HMACSecret: testSecret,
	}
	rc := NewResumingClient(cfg, 8)

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- rc.Run(ctx, func(context.Context, wire.Envelope) error { return nil }) }()

	// Cancel mid-backoff (first dial fails ~immediately, then it sleeps
	// baseBackoff=100ms before retrying — cancel inside that window).
	time.Sleep(120 * time.Millisecond)
	cancel()

	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	case <-time.After(maxBackoff + time.Second):
		t.Fatalf("Run did not unwind promptly on ctx cancel during backoff")
	}
}

// TestResumingClient_HandlerErrorForgetRedelivers: the surface/forget
// contract. When the handler returns an error, surface() must NOT ack and
// MUST forget the id from the dedupe cache, so a later redelivery of the
// same id is treated as NEW (handler invoked again). When the handler
// succeeds, surface() acks. This drives surface() directly (it is
// package-internal) so it is deterministic and not subject to broker
// reconnect timing.
func TestResumingClient_HandlerErrorForgetRedelivers(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A live client just so surface()'s Ack has somewhere to write (the
	// ack itself is not what we assert; the dedupe forget/keep is).
	c := f.rawClient(ctx, "consumer")
	defer c.Close()

	rc := NewResumingClient(f.cfg("consumer"), 16)

	del := wire.Deliver{
		ProtocolVersion: wire.ProtocolVersion,
		Type:            wire.ControlDeliver,
		DeliveryKey:     "redeliver-me",
		Envelope: wire.Envelope{
			ProtocolVersion: wire.ProtocolVersion,
			ID:              "redeliver-me",
			From:            "sender",
			To:              "consumer",
			TS:              "t",
			Source:          "peer-bus",
			Kind:            wire.KindMsg,
			Body:            json.RawMessage(`{"n":1}`),
		},
	}

	var calls int
	handler := func(_ context.Context, _ wire.Envelope) error {
		calls++
		if calls == 1 {
			// First delivery: refuse. surface must forget the id so a
			// redelivery is treated as new.
			return errors.New("not consumed yet")
		}
		return nil // second delivery: consume
	}

	// First delivery — handler errors.
	rc.surface(ctx, c, handler, del)
	if calls != 1 {
		t.Fatalf("after first surface, handler calls = %d, want 1", calls)
	}

	// Redelivery of the SAME id — because the handler errored, surface
	// forgot it, so it must be treated as NEW and the handler invoked
	// again (NOT silently re-acked as a duplicate).
	rc.surface(ctx, c, handler, del)
	if calls != 2 {
		t.Fatalf("redelivery after handler error: handler calls = %d, want 2 "+
			"(forgotten id must be treated as new, not duplicate-suppressed)", calls)
	}

	// Now it was consumed (calls==2 returned nil) → it is recorded as seen.
	// A further redelivery must be duplicate-suppressed (handler NOT called
	// again), proving consume-then-keep.
	rc.surface(ctx, c, handler, del)
	if calls != 2 {
		t.Fatalf("post-consume redelivery: handler calls = %d, want still 2 "+
			"(consumed id must be dedupe-suppressed)", calls)
	}
}
