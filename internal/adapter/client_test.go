package adapter

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nnemirovsky/peerbus/internal/broker"
	hmacpkg "github.com/nnemirovsky/peerbus/internal/hmac"
	"github.com/nnemirovsky/peerbus/internal/store"
	"github.com/nnemirovsky/peerbus/internal/wire"
)

// testSecret is a deliberately non-secret 32-byte fixture, same shape as
// the broker tests use.
var testSecret = []byte(strings.Repeat("peerbus-test-", 4)[:hmacpkg.MinSecretLen])

// brokerFixture is an in-process broker over httptest with an in-memory
// store. closeFn stops the http server (simulating a transport drop) and
// can be re-opened on the same store to model reconnect/resume.
type brokerFixture struct {
	t     *testing.T
	st    *store.Store
	srv   *broker.Server
	hs    *httptest.Server
	wsURL string
}

func newBrokerFixture(t *testing.T) *brokerFixture {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := broker.NewServer(broker.NewAuthenticator([]string{"tok"}), broker.NewRegistry(), st, nil)
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	return &brokerFixture{
		t:     t,
		st:    st,
		srv:   srv,
		hs:    hs,
		wsURL: "ws" + strings.TrimPrefix(hs.URL, "http"),
	}
}

func (f *brokerFixture) cfg(name string) ClientConfig {
	return ClientConfig{URL: f.wsURL, Token: "tok", Name: name, HMACSecret: testSecret}
}

// rawClient connects a Client and registers under name.
func (f *brokerFixture) rawClient(ctx context.Context, name string) *Client {
	f.t.Helper()
	c := NewClient(f.cfg(name))
	if err := c.Connect(ctx); err != nil {
		f.t.Fatalf("connect %s: %v", name, err)
	}
	return c
}

// TestClient_RegisterSendReceiveAck: two clients against an in-process
// broker — register, send a signed direct message, receive + HMAC-verify
// + ack.
func TestClient_RegisterSendReceiveAck(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bob := f.rawClient(ctx, "bob")
	defer bob.Close()
	alice := f.rawClient(ctx, "alice")
	defer alice.Close()

	body := json.RawMessage(`{"hello":"world"}`)
	if err := bob.Send(ctx, "m1", "alice", "ts1", "peer-bus", body); err != nil {
		t.Fatalf("send: %v", err)
	}

	del, err := alice.Recv(ctx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	env := del.Envelope
	if env.ID != "m1" || env.From != "bob" {
		t.Fatalf("received envelope = %+v, want id=m1 from=bob", env)
	}
	if del.DeliveryKey != "m1" {
		t.Fatalf("direct delivery_key = %q, want m1", del.DeliveryKey)
	}
	if err := hmacpkg.VerifyEnvelope(testSecret, env); err != nil {
		t.Fatalf("inbound envelope failed HMAC verify: %v", err)
	}
	if err := alice.Ack(ctx, del.DeliveryKey); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

// TestClient_BroadcastAndPeers: broadcast fans out (sender excluded) and
// Peers returns the registry.
func TestClient_BroadcastAndPeers(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sender := f.rawClient(ctx, "sender")
	defer sender.Close()
	a := f.rawClient(ctx, "a")
	defer a.Close()

	names, _, err := sender.Peers(ctx)
	if err != nil {
		t.Fatalf("peers: %v", err)
	}
	got := strings.Join(sortedCopy(names), ",")
	if got != "a,sender" {
		t.Fatalf("peers = %v, want [a sender]", names)
	}

	if err := sender.Broadcast(ctx, "bc1", "ts", "peer-bus", json.RawMessage(`{"x":1}`)); err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	// The broker delivers the sender's VERBATIM signed envelope on a
	// broadcast (to:"*", original id, original hmac). The per-recipient
	// row key rides on wire.Deliver.DeliveryKey, OUTSIDE the HMAC subset,
	// so the copy verifies end-to-end and DeliveryKey is the per-recipient
	// row id "<origID>|<recipient>".
	del, err := a.Recv(ctx)
	if err != nil {
		t.Fatalf("broadcast copy not delivered / failed HMAC: %v", err)
	}
	env := del.Envelope
	if env.ID != "bc1" || env.To != "*" {
		t.Fatalf("broadcast envelope = id %q to %q, want verbatim id=bc1 to=*", env.ID, env.To)
	}
	if del.DeliveryKey != "bc1|a" {
		t.Fatalf("broadcast delivery_key = %q, want per-recipient bc1|a", del.DeliveryKey)
	}
	if err := hmacpkg.VerifyEnvelope(testSecret, env); err != nil {
		t.Fatalf("broadcast copy not end-to-end HMAC-verifiable: %v", err)
	}
}

// TestClient_InboundHMACRejected: an inbound envelope that fails HMAC
// verification is rejected (ErrInboundHMAC), never surfaced as valid.
func TestClient_InboundHMACRejected(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bob := f.rawClient(ctx, "bob")
	defer bob.Close()
	alice := f.rawClient(ctx, "alice")
	defer alice.Close()

	// Send signed under a DIFFERENT secret so alice's verify fails. We
	// craft the wire frame directly: a tampered/foreign-signed envelope.
	wrong := []byte(strings.Repeat("wrong-secret-", 4)[:hmacpkg.MinSecretLen])
	env := wire.Envelope{
		ProtocolVersion: wire.ProtocolVersion,
		ID:              "bad1",
		From:            "bob",
		To:              "alice",
		TS:              "t",
		Source:          "peer-bus",
		Kind:            wire.KindMsg,
		Body:            json.RawMessage(`{"a":1}`),
	}
	signed, err := hmacpkg.SignEnvelope(wrong, env)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	raw, _ := json.Marshal(signed)
	if err := f.st.Enqueue(store.Message{ID: signed.ID, From: "bob", To: "alice", Envelope: raw}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Trigger a flush to alice by reconnecting alice (same-name re-reg).
	alice.Close()
	alice2 := f.rawClient(ctx, "alice")
	defer alice2.Close()

	rctx, rcancel := context.WithTimeout(ctx, 2*time.Second)
	defer rcancel()
	_, rerr := alice2.Recv(rctx)
	if rerr == nil {
		t.Fatalf("expected ErrInboundHMAC, got nil")
	}
	if !strings.Contains(rerr.Error(), "failed HMAC verify") {
		t.Fatalf("error = %v, want inbound HMAC rejection", rerr)
	}
}

// TestResumingClient_ReconnectResumeDedup is the core reconnect/resume
// assertion: a message delivered+consumed but whose ack is lost to a drop
// is redelivered by the broker after the same-name re-register; the shared
// dedupe cache suppresses the duplicate so the host sees each id exactly
// once.
func TestResumingClient_ReconnectResumeDedup(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Sender (a plain client) puts a message in flight to "drainer".
	sender := f.rawClient(ctx, "sender")
	defer sender.Close()

	// Register "drainer" once so it is a known peer, then drop it before
	// the message is sent (so the message queues offline / pending).
	pre := f.rawClient(ctx, "drainer")
	pre.Close()
	time.Sleep(100 * time.Millisecond)

	if err := sender.Send(ctx, "dup-1", "drainer", "ts", "peer-bus", json.RawMessage(`{"n":1}`)); err != nil {
		t.Fatalf("send: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// The host records every id it is asked to consume. The handler
	// blocks ack on the FIRST delivery (simulating ack lost to a drop):
	// it records consumption, signals, then deliberately drops the
	// underlying connection BEFORE the ack can be flushed.
	var (
		mu       sync.Mutex
		consumed []string
	)
	firstSeen := make(chan struct{}, 1)

	rc := NewResumingClient(f.cfg("drainer"), 64)

	handler := func(_ context.Context, env wire.Envelope) error {
		mu.Lock()
		consumed = append(consumed, env.ID)
		n := len(consumed)
		mu.Unlock()
		if n == 1 {
			// Consumed, but kill the connection before the ack flushes:
			// the broker keeps the message unacked and will redeliver it
			// after the same-name re-register.
			if c := rc.Client(); c != nil {
				c.Close()
			}
			select {
			case firstSeen <- struct{}{}:
			default:
			}
		}
		return nil
	}

	runErr := make(chan error, 1)
	go func() { runErr <- rc.Run(ctx, handler) }()

	select {
	case <-firstSeen:
	case <-time.After(5 * time.Second):
		t.Fatalf("handler never consumed the first delivery")
	}

	// After reconnect the broker redelivers dup-1 (it was unacked).
	// Dedupe must suppress it: give the loop time to reconnect, receive
	// the duplicate, and (re)ack it without re-invoking the handler.
	time.Sleep(1500 * time.Millisecond)

	cancel()
	<-runErr

	mu.Lock()
	defer mu.Unlock()
	count := 0
	for _, id := range consumed {
		if id == "dup-1" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("host consumed dup-1 %d times, want exactly 1 (dedupe failed) — consumed=%v", count, consumed)
	}
}

// TestResumingClient_DeliversAndAcks: the resuming loop surfaces a fresh
// message to the host exactly once and acks it (so it is NOT redelivered
// on a later reconnect).
func TestResumingClient_DeliversAndAcks(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sender := f.rawClient(ctx, "sender")
	defer sender.Close()

	got := make(chan string, 4)
	rc := NewResumingClient(f.cfg("rx"), 64)
	go func() {
		_ = rc.Run(ctx, func(_ context.Context, env wire.Envelope) error {
			got <- env.ID
			return nil
		})
	}()

	// Wait for rx to be connected before sending.
	deadline := time.Now().Add(3 * time.Second)
	for rc.Client() == nil && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)

	if err := sender.Send(ctx, "x1", "rx", "ts", "peer-bus", json.RawMessage(`{"v":1}`)); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case id := <-got:
		if id != "x1" {
			t.Fatalf("delivered id = %q, want x1", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("message never surfaced to host")
	}
	// Allow the ack to flush.
	time.Sleep(200 * time.Millisecond)
}

// --- mode dispatch tests ---

// stubMode is a minimal Mode for the additive-registration dispatch test
// (the production placeholderMode was removed — real modes self-register
// from their own init()).
type stubMode struct{ name string }

func (s stubMode) Name() string              { return s.name }
func (s stubMode) Run(context.Context) error { return nil }

func TestModeDispatch_KnownResolveUnknownErrors(t *testing.T) {
	// The real generic/cc modes self-register from their init().
	for _, m := range []string{"generic", "cc"} {
		ctor, err := Resolve(m)
		if err != nil {
			t.Fatalf("Resolve(%q) error: %v", m, err)
		}
		mode, err := ctor(ClientConfig{}, 0)
		if err != nil {
			t.Fatalf("ctor(%q) error: %v", m, err)
		}
		if mode.Name() != m {
			t.Fatalf("mode.Name() = %q, want %q", mode.Name(), m)
		}
	}
	// Unknown mode → clear error.
	if _, err := Resolve("nope"); err == nil {
		t.Fatalf("Resolve(nope) must error")
	}
}

func TestModeDispatch_AdditiveRegistration(t *testing.T) {
	const name = "test-extra-mode"
	if _, err := Resolve(name); err == nil {
		t.Fatalf("mode %q should be unknown before registration", name)
	}
	Register(name, func(_ ClientConfig, _ int) (Mode, error) {
		return stubMode{name: name}, nil
	})
	t.Cleanup(func() { unregisterMode(name) })
	if _, err := Resolve(name); err != nil {
		t.Fatalf("mode %q should resolve after additive Register: %v", name, err)
	}
	found := false
	for _, n := range Modes() {
		if n == name {
			found = true
		}
	}
	if !found {
		t.Fatalf("Modes() = %v, want it to include %q", Modes(), name)
	}
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}
