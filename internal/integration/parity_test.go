// Package integration holds the cc2cc-parity validation matrix integration
// tests: a REAL in-process broker (httptest WS server) plus real adapters
// (generic via adapter.NewGenericBus, cc via channel.Server over the shared
// ResumingClient), exercising every parity row in docs/cc2cc-parity.md.
//
// Pattern mirrors internal/broker, internal/mcp and internal/channel tests:
// store.Open(":memory:") -> broker.NewServer -> httptest.NewServer ->
// ws:// URL; adapters connect over the real coder/websocket client. Nothing
// is faked — the full path (broker routing + SQLite durability + reconnect/
// resume + shared dedupe + end-to-end HMAC) is what is asserted.
//
// Scope note: the full plan is in force (Task 1 ✅ RESOLVED — the generic-
// only reduced variant was rescinded). The cc parity rows (auto-register/
// unique-name, push-wake) ARE in scope and covered here. The push-wake row
// uses the automatable in-process proxy (a broker deliver to a cc adapter
// produces exactly one notifications/claude/channel notification); the real
// interactive claude-session confirmation stays Post-Completion and is
// referenced in docs/manual-e2e-claude-channel.md.
package integration_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nnemirovsky/peerbus/internal/adapter"
	"github.com/nnemirovsky/peerbus/internal/broker"
	"github.com/nnemirovsky/peerbus/internal/channel"
	hmacpkg "github.com/nnemirovsky/peerbus/internal/hmac"
	"github.com/nnemirovsky/peerbus/internal/store"
	"github.com/nnemirovsky/peerbus/internal/wire"
)

// testSecret is a non-secret 32-byte fixture (same shape every other package
// test uses).
var testSecret = []byte(strings.Repeat("peerbus-test-", 4)[:hmacpkg.MinSecretLen])

// ── in-process broker fixture (mirrors internal/mcp + internal/channel) ──

type brokerFixture struct {
	t     *testing.T
	st    *store.Store
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
		wsURL: "ws" + strings.TrimPrefix(hs.URL, "http"),
	}
}

func (f *brokerFixture) cfg(name string) adapter.ClientConfig {
	return adapter.ClientConfig{URL: f.wsURL, Token: "tok", Name: name, HMACSecret: testSecret}
}

func (f *brokerFixture) cfgToken(name, token string) adapter.ClientConfig {
	c := f.cfg(name)
	c.Token = token
	return c
}

func (f *brokerFixture) rawClient(ctx context.Context, name string) *adapter.Client {
	f.t.Helper()
	c := adapter.NewClient(f.cfg(name))
	if err := c.Connect(ctx); err != nil {
		f.t.Fatalf("connect %s: %v", name, err)
	}
	return c
}

// ── generic adapter harness: real mcp.Server-equivalent bus over a real
// broker (adapter.NewGenericBus is the embeddable seam the generic Mode
// uses) ──

type genericPeer struct {
	bus interface { // subset of mcp.Bus exercised here
		Send(ctx context.Context, to string, body json.RawMessage) error
		Broadcast(ctx context.Context, body json.RawMessage) error
		Peers(ctx context.Context) ([]string, error)
	}
	drain func(ctx context.Context) ([]drainMsg, error)
	stop  func()
}

type drainMsg struct {
	ID     string
	From   string
	Source string
	Body   json.RawMessage
}

// newGenericPeer brings up a real broker-backed generic bus for `name` and
// waits until its broker connection is live so injected messages are not
// lost before register completes.
func newGenericPeer(t *testing.T, f *brokerFixture, name string) *genericPeer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	bus, stopBus := adapter.NewGenericBus(ctx, f.cfg(name), 64, nil)

	p := &genericPeer{
		bus: bus,
		drain: func(dctx context.Context) ([]drainMsg, error) {
			ms, err := bus.Drain(dctx)
			if err != nil {
				return nil, err
			}
			out := make([]drainMsg, 0, len(ms))
			for _, m := range ms {
				out = append(out, drainMsg{ID: m.ID, From: m.From, Source: m.Source, Body: m.Body})
			}
			return out, nil
		},
		stop: func() {
			stopBus()
			cancel()
		},
	}
	t.Cleanup(p.stop)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := bus.Peers(ctx); err == nil {
			return p
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("generic peer %q never got a live broker connection", name)
	return nil
}

// drainUntil polls the generic bus drain until at least one message arrives
// (or the deadline elapses), accumulating all yielded messages.
func (p *genericPeer) drainUntil(ctx context.Context, d time.Duration) []drainMsg {
	deadline := time.Now().Add(d)
	var all []drainMsg
	for time.Now().Before(deadline) {
		ms, err := p.drain(ctx)
		if err == nil {
			all = append(all, ms...)
			if len(all) > 0 {
				return all
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return all
}

// ── cc adapter harness: real channel.Server over the shared ResumingClient
// (mirrors internal/adapter/cc.go's ccBus + internal/channel test harness) ──

type ccBus struct{ rc *adapter.ResumingClient }

func (b *ccBus) Send(ctx context.Context, to string, body json.RawMessage) error {
	c := b.rc.Client()
	if c == nil {
		return adapter.ErrNotConnected
	}
	return c.Send(ctx, "out-"+to+"-"+time.Now().Format("150405.000000000"), to,
		time.Now().UTC().Format(time.RFC3339Nano), "peer-bus", body)
}

func (b *ccBus) Broadcast(ctx context.Context, body json.RawMessage) error {
	c := b.rc.Client()
	if c == nil {
		return adapter.ErrNotConnected
	}
	return c.Broadcast(ctx, "bc-"+time.Now().Format("150405.000000000"),
		time.Now().UTC().Format(time.RFC3339Nano), "peer-bus", body)
}

func (b *ccBus) Peers(ctx context.Context) ([]string, error) {
	c := b.rc.Client()
	if c == nil {
		return nil, adapter.ErrNotConnected
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
	case <-time.After(5 * time.Second):
		return nil, context.DeadlineExceeded
	}
}

type ccPeer struct {
	in     *io.PipeWriter
	frames chan json.RawMessage
	stop   func()
	nextID int
	t      *testing.T
}

// pushFrame is the decoded shape of a notifications/claude/channel frame.
type pushFrame struct {
	ID     any    `json:"id"`
	Method string `json:"method"`
	Params struct {
		Content string            `json:"content"`
		Meta    map[string]string `json:"meta"`
	} `json:"params"`
}

// newCCPeer wires a REAL channel.Server to a REAL broker over pipes, runs
// the shared ResumingClient resume loop (each verified+deduped delivery ->
// claude/channel push, exactly internal/adapter/cc.go's ccBus.handle), and
// completes the MCP initialize handshake. `name` may be "" to exercise the
// cc auto-register / unique-name path (channel.UniqueName).
func newCCPeer(t *testing.T, f *brokerFixture, name string) (peer *ccPeer, registeredName string) {
	t.Helper()
	if name == "" {
		name = channel.UniqueName()
	}
	ctx, cancel := context.WithCancel(context.Background())

	rc := adapter.NewResumingClient(f.cfg(name), 64)
	bus := &ccBus{rc: rc}

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	srv := channel.NewServer(bus, inR, outW)

	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		_ = rc.Run(ctx, func(_ context.Context, env wire.Envelope) error {
			srv.Deliver(channel.Inbound{ID: env.ID, From: env.From, Source: env.Source, Body: env.Body})
			return nil
		})
	}()

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = srv.Serve(ctx)
	}()

	p := &ccPeer{in: inW, frames: make(chan json.RawMessage, 16), t: t}

	// SINGLE stdout reader goroutine (two readers on one bufio.Reader race
	// and steal frames — same constraint as the internal/channel harness).
	br := bufio.NewReader(outR)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			line, err := br.ReadBytes('\n')
			if len(line) > 0 {
				p.frames <- json.RawMessage(strings.TrimRight(string(line), "\r\n"))
			}
			if err != nil {
				return
			}
		}
	}()

	p.stop = func() {
		_ = inW.Close()
		cancel()
		<-serveDone
		<-loopDone
		_ = outW.Close()
		<-readerDone
	}
	t.Cleanup(p.stop)

	// Wait for a live broker connection so injected messages are not lost.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := bus.Peers(ctx); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	p.initialize()
	return p, name
}

func (p *ccPeer) initialize() {
	p.t.Helper()
	p.nextID++
	req, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": p.nextID, "method": "initialize",
		"params": map[string]any{"protocolVersion": "2025-06-18"},
	})
	if _, err := p.in.Write(append(req, '\n')); err != nil {
		p.t.Fatalf("initialize write: %v", err)
	}
	resp := p.readFrame()
	var r struct {
		Result struct {
			Capabilities struct {
				Experimental map[string]json.RawMessage `json:"experimental"`
			} `json:"capabilities"`
			ServerInfo struct {
				Name string `json:"name"`
			} `json:"serverInfo"`
		} `json:"result"`
		Error *json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(resp, &r); err != nil {
		p.t.Fatalf("initialize decode: %v (%s)", err, resp)
	}
	if r.Error != nil {
		p.t.Fatalf("initialize error: %s", resp)
	}
	if _, ok := r.Result.Capabilities.Experimental["claude/channel"]; !ok {
		p.t.Fatalf("initialize missing experimental[claude/channel]: %s", resp)
	}
	if r.Result.ServerInfo.Name != "peerbus-cc-adapter" {
		p.t.Fatalf("serverInfo.name = %q, want peerbus-cc-adapter", r.Result.ServerInfo.Name)
	}
	notif, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"})
	if _, err := p.in.Write(append(notif, '\n')); err != nil {
		p.t.Fatalf("initialized notify: %v", err)
	}
}

func (p *ccPeer) readFrame() json.RawMessage {
	p.t.Helper()
	select {
	case fr := <-p.frames:
		return fr
	case <-time.After(5 * time.Second):
		p.t.Fatalf("timed out waiting for a JSON-RPC frame")
		return nil
	}
}

func (p *ccPeer) readFrameNoFail(d time.Duration) (json.RawMessage, bool) {
	select {
	case fr := <-p.frames:
		return fr, true
	case <-time.After(d):
		return nil, false
	}
}

// ── Row 1: auto-register / unique name (generic + cc) ──

// TestParity_AutoRegisterUniqueName: two generic adapters auto-register
// distinct unique names and both appear in the broker registry — the
// cc2cc "each session auto-claims a unique peer" ergonomic.
func TestParity_AutoRegisterUniqueName(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	a := newGenericPeer(t, f, "alpha")
	b := newGenericPeer(t, f, "bravo")

	names, err := a.bus.Peers(ctx)
	if err != nil {
		t.Fatalf("peers: %v", err)
	}
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	if !got["alpha"] || !got["bravo"] {
		t.Fatalf("registry = %v, want alpha+bravo bound under their unique names", names)
	}
	_ = b
}

// TestParity_CCAutoRegisterUniqueName: a cc adapter launched with NO
// configured name auto-registers a minted unique name (channel.UniqueName,
// cc-<host>-<pid>-<rand>) and that name is visible in the broker registry —
// the cc-side of the cc2cc auto-register/unique-name parity row.
func TestParity_CCAutoRegisterUniqueName(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, name1 := newCCPeer(t, f, "") // "" => channel.UniqueName()
	if !strings.HasPrefix(name1, "cc-") {
		t.Fatalf("auto-registered name %q lacks cc- prefix", name1)
	}

	// A second auto-registered cc peer must mint a DISTINCT name.
	_, name2 := newCCPeer(t, f, "")
	if name1 == name2 {
		t.Fatalf("two auto-registered cc peers collided on name %q", name1)
	}

	// Both unique names are bound in the broker registry.
	observer := f.rawClient(ctx, "observer")
	defer observer.Close()
	names, _, err := observer.Peers(ctx)
	if err != nil {
		t.Fatalf("peers: %v", err)
	}
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	if !got[name1] || !got[name2] {
		t.Fatalf("registry = %v, want both auto-registered names %q,%q", names, name1, name2)
	}
}

// ── Row 2: peer discovery ──

// TestParity_PeerDiscovery: a generic adapter sees every other live peer via
// bus.peers (broker registry) — the cc2cc peer-discovery ergonomic.
func TestParity_PeerDiscovery(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	other := f.rawClient(ctx, "discoverable")
	defer other.Close()

	asker := newGenericPeer(t, f, "asker")
	names, err := asker.bus.Peers(ctx)
	if err != nil {
		t.Fatalf("peers: %v", err)
	}
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	if !got["asker"] || !got["discoverable"] {
		t.Fatalf("peers = %v, want asker+discoverable", names)
	}
}

// ── Row 3: direct message (cross-peer, HMAC end-to-end) ──

// TestParity_DirectMessage: generic peer A -> generic peer B by name; B
// drains it with the right from/source/body and it is HMAC-verifiable
// end-to-end (the broker does NOT rewrite signed fields for direct sends).
func TestParity_DirectMessage(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rx := newGenericPeer(t, f, "rx")
	tx := newGenericPeer(t, f, "tx")

	if err := tx.bus.Send(ctx, "rx", json.RawMessage(`{"hello":"world"}`)); err != nil {
		t.Fatalf("send: %v", err)
	}

	msgs := rx.drainUntil(ctx, 5*time.Second)
	if len(msgs) != 1 {
		t.Fatalf("rx drained %d messages, want 1: %+v", len(msgs), msgs)
	}
	m := msgs[0]
	if m.From != "tx" || m.Source != "peer-bus" {
		t.Fatalf("drained message from=%q source=%q, want from=tx source=peer-bus", m.From, m.Source)
	}
	if string(m.Body) != `{"hello":"world"}` {
		t.Fatalf("body = %s, want {\"hello\":\"world\"}", m.Body)
	}
}

// ── Row 4: broadcast (sender-excluded fan-out, cross-peer) ──

// TestParity_BroadcastFanOutSenderExcluded: a broadcast from one generic
// adapter fans out to every OTHER registered peer (one per-recipient durable
// copy each) and NOT to the sender — the cc2cc broadcast ergonomic with the
// locked no-self / no-backfill model.
//
// Recipients are raw broker clients (not generic buses): the broker rewrites
// the signed id/to per recipient on fan-out (router.go: copyEnv.ID =
// env.ID+"|"+r), so each per-recipient copy fails end-to-end HMAC by design
// — the generic bus loop logs+skips HMAC-failed broadcast copies (documented
// limitation, same as internal/mcp's TestBusBroadcastFansOut). Asserting on
// the raw Client.Recv outcome is the authoritative way to observe the
// fan-out + sender-exclusion guarantee independent of that documented
// broadcast-HMAC limitation.
func TestParity_BroadcastFanOutSenderExcluded(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	r1 := f.rawClient(ctx, "r1")
	defer r1.Close()
	r2 := f.rawClient(ctx, "r2")
	defer r2.Close()

	caster := newGenericPeer(t, f, "caster")
	if err := caster.bus.Broadcast(ctx, json.RawMessage(`{"announce":1}`)); err != nil {
		t.Fatalf("broadcast: %v", err)
	}

	// Each non-sender receives its own per-recipient copy. The broker
	// rewrote the signed id/to, so Recv reports ErrInboundHMAC — but it
	// still returns the carried envelope (from=caster), proving the fan-out
	// copy reached this recipient.
	for _, rp := range []struct {
		name string
		c    *adapter.Client
	}{{"r1", r1}, {"r2", r2}} {
		env, rerr := rp.c.Recv(ctx)
		if rerr != nil && !strings.Contains(rerr.Error(), "failed HMAC verify") {
			t.Fatalf("%s broadcast recv error = %v, want delivery or HMAC rejection", rp.name, rerr)
		}
		if env.From != "caster" {
			t.Fatalf("%s broadcast copy from = %q, want caster", rp.name, env.From)
		}
		if env.To != rp.name {
			t.Fatalf("%s broadcast copy to = %q, want per-recipient %q", rp.name, env.To, rp.name)
		}
	}

	// The sender is EXCLUDED — the broker queued no broadcast copy for
	// "caster". Re-binding "caster" with a raw client (same-token takeover)
	// and draining must surface nothing: no pending copy means Recv blocks
	// until the short timeout, not a delivery.
	selfRaw := f.rawClient(ctx, "caster")
	defer selfRaw.Close()
	recvCtx, recvCancel := context.WithTimeout(ctx, 800*time.Millisecond)
	defer recvCancel()
	if env, err := selfRaw.Recv(recvCtx); err == nil {
		t.Fatalf("sender received its own broadcast (id=%s from=%s) — fan-out exclusion broken", env.ID, env.From)
	}
}

// ── Row 5: HMAC signing / trust ──

// TestParity_HMACSignedDeliversAndForgedRejected: a correctly-signed direct
// message delivers and verifies; a message signed under the WRONG secret is
// rejected by the recipient (a compromised broker cannot forge a peer) — the
// cc2cc HMAC signing/trust ergonomic.
func TestParity_HMACSignedDeliversAndForgedRejected(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Recipient is a raw client so we can assert the HMAC outcome directly
	// (Client.Recv verifies end-to-end and returns ErrInboundHMAC on a bad
	// signature instead of surfacing it).
	rx := f.rawClient(ctx, "rx")
	defer rx.Close()

	// Valid: a properly-signed message verifies and delivers.
	good := f.rawClient(ctx, "good")
	defer good.Close()
	if err := good.Send(ctx, "ok-1", "rx", "ts", "peer-bus", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatalf("good send: %v", err)
	}
	env, err := rx.Recv(ctx)
	if err != nil {
		t.Fatalf("valid signed message must verify+deliver, got %v", err)
	}
	if env.From != "good" || string(env.Body) != `{"ok":true}` {
		t.Fatalf("delivered env = %+v, want from=good body={\"ok\":true}", env)
	}
	if verr := hmacpkg.VerifyEnvelope(testSecret, env); verr != nil {
		t.Fatalf("delivered direct message not HMAC-verifiable: %v", verr)
	}

	// Forged: a peer connected under a DIFFERENT HMAC secret. Its signature
	// fails the recipient's verify — rejected, never surfaced.
	forgedCfg := f.cfg("forger")
	forgedCfg.HMACSecret = []byte(strings.Repeat("wrong-secret-x", 3)[:hmacpkg.MinSecretLen])
	forged := adapter.NewClient(forgedCfg)
	if err := forged.Connect(ctx); err != nil {
		t.Fatalf("forged connect: %v", err)
	}
	defer forged.Close()
	if err := forged.Send(ctx, "forged-1", "rx", "ts", "peer-bus", json.RawMessage(`{"evil":true}`)); err != nil {
		t.Fatalf("forged send: %v", err)
	}
	_, rerr := rx.Recv(ctx)
	if rerr == nil {
		t.Fatalf("forged-HMAC inbound was accepted — trust model broken")
	}
	if !strings.Contains(rerr.Error(), "failed HMAC verify") {
		t.Fatalf("forged inbound error = %v, want HMAC rejection", rerr)
	}
}

// ── Row 6: offline persistence + delivery on next session/drain ──

// TestParity_OfflinePersistenceThenDrain: a message sent to a peer that is
// known-but-offline is durably queued, then delivered when that peer comes
// back online and drains — the cc2cc "delivered on the next session"
// ergonomic.
func TestParity_OfflinePersistenceThenDrain(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Register "late" once so the broker knows the peer, then drop it so the
	// message queues offline (delivered=0).
	pre := f.rawClient(ctx, "late")
	pre.Close()
	time.Sleep(150 * time.Millisecond)

	sender := f.rawClient(ctx, "sender")
	defer sender.Close()
	if err := sender.Send(ctx, "queued-1", "late", "ts", "peer-bus", json.RawMessage(`{"queued":true}`)); err != nil {
		t.Fatalf("send to offline peer: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	// "late" comes online (new generic session, same name) and drains the
	// message that was persisted while it was offline.
	late := newGenericPeer(t, f, "late")
	msgs := late.drainUntil(ctx, 8*time.Second)
	found := false
	for _, m := range msgs {
		if m.ID == "queued-1" {
			found = true
			if m.From != "sender" || string(m.Body) != `{"queued":true}` {
				t.Fatalf("offline-delivered msg = %+v, want from=sender body={\"queued\":true}", m)
			}
		}
	}
	if !found {
		t.Fatalf("offline-queued message not delivered on next session; drained=%+v", msgs)
	}
}

// ── Row 7: push-wake (cc) — in-process automatable proxy ──

// TestParity_CCPushWakeNotification asserts the automatable proxy for the
// cc push-wake parity row: a broker `deliver` to a cc adapter produces
// EXACTLY ONE notifications/claude/channel JSON-RPC notification with the
// right content + meta (from/source/msg_id). That notification is precisely
// the wire signal Claude Code consumes to wake an idle interactive session
// and create a turn.
//
// The REAL interactive `claude --dangerously-load-development-channels`
// session confirmation (notification actually creates a turn; bus.* replies
// work) cannot be automated by a non-interactive agent and stays
// Post-Completion — see docs/manual-e2e-claude-channel.md. This test is the
// in-process stand-in referenced from docs/cc2cc-parity.md row 7.
func TestParity_CCPushWakeNotification(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cc, ccName := newCCPeer(t, f, "cc-rx")
	if ccName != "cc-rx" {
		t.Fatalf("cc name = %q, want cc-rx", ccName)
	}

	tx := f.rawClient(ctx, "tx")
	defer tx.Close()

	body, _ := json.Marshal("wake up, there is a decision to make")
	if err := tx.Send(ctx, "wake-1", "cc-rx",
		time.Now().UTC().Format(time.RFC3339Nano), "peer-bus", body); err != nil {
		t.Fatalf("tx send: %v", err)
	}

	frame := cc.readFrame()
	var pf pushFrame
	if err := json.Unmarshal(frame, &pf); err != nil {
		t.Fatalf("push decode: %v (%s)", err, frame)
	}
	if pf.ID != nil {
		t.Fatalf("push-wake must be a JSON-RPC notification (no id); got %s", frame)
	}
	if pf.Method != "notifications/claude/channel" {
		t.Fatalf("method = %q, want notifications/claude/channel", pf.Method)
	}
	if pf.Params.Content != "wake up, there is a decision to make" {
		t.Fatalf("content = %q", pf.Params.Content)
	}
	if pf.Params.Meta["from"] != "tx" ||
		pf.Params.Meta["source"] != "peer-bus" ||
		pf.Params.Meta["msg_id"] != "wake-1" {
		t.Fatalf("meta = %v, want from=tx source=peer-bus msg_id=wake-1", pf.Params.Meta)
	}

	// EXACTLY ONE notification: a duplicate id never re-pushes (broker
	// dedups by UNIQUE(id); shared consumer dedupe is the second line).
	if dup, ok := cc.readFrameNoFail(400 * time.Millisecond); ok {
		t.Fatalf("a second push was emitted for one deliver: %s", dup)
	}
}

// ── bad-token register rejected ──

// TestParity_BadTokenRegisterRejected: a peer presenting a token the broker
// does not know is rejected at register — a name is bindable only under a
// valid token (the auth gate). Surfaces as Client.Connect returning an
// error (the broker closes the WS with a policy-violation close).
func TestParity_BadTokenRegisterRejected(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Valid token still works (control: the broker is up and accepting).
	okClient := adapter.NewClient(f.cfg("legit"))
	if err := okClient.Connect(ctx); err != nil {
		t.Fatalf("valid-token register must succeed, got %v", err)
	}
	okClient.Close()

	bad := adapter.NewClient(f.cfgToken("intruder", "WRONG-TOKEN"))
	err := bad.Connect(ctx)
	if err == nil {
		bad.Close()
		t.Fatalf("bad-token register was accepted — auth gate broken")
	}
}

// ── dedupe on redelivery ──

// TestParity_DedupeOnRedelivery: at-least-once + reconnect means a single
// logical message can be redelivered; the SHARED consumer dedupe must
// surface each id to the host EXACTLY ONCE. Models the locked delivery
// model: a message consumed but un-acked before a drop is redelivered after
// the same-name re-register and the dedupe suppresses the duplicate.
func TestParity_DedupeOnRedelivery(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sender := f.rawClient(ctx, "sender")
	defer sender.Close()

	// Register "consumer" once so it is a known peer, then drop it before
	// the message is sent (so it queues offline / pending).
	pre := f.rawClient(ctx, "consumer")
	pre.Close()
	time.Sleep(100 * time.Millisecond)

	if err := sender.Send(ctx, "dup-1", "consumer", "ts", "peer-bus", json.RawMessage(`{"n":1}`)); err != nil {
		t.Fatalf("send: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	var (
		consumed []string
		seen     = make(chan struct{}, 1)
	)
	rc := adapter.NewResumingClient(f.cfg("consumer"), 64)
	handler := func(_ context.Context, env wire.Envelope) error {
		consumed = append(consumed, env.ID)
		if len(consumed) == 1 {
			// Consumed but kill the connection before the ack flushes: the
			// broker keeps the message unacked and redelivers it after the
			// same-name re-register.
			if c := rc.Client(); c != nil {
				c.Close()
			}
			select {
			case seen <- struct{}{}:
			default:
			}
		}
		return nil
	}

	runErr := make(chan error, 1)
	go func() { runErr <- rc.Run(ctx, handler) }()

	select {
	case <-seen:
	case <-time.After(5 * time.Second):
		t.Fatalf("handler never consumed the first delivery")
	}

	// Give the resume loop time to reconnect, receive the redelivered
	// dup-1, and have the shared dedupe suppress it (re-ack, no re-surface).
	time.Sleep(1500 * time.Millisecond)
	cancel()
	<-runErr

	count := 0
	for _, id := range consumed {
		if id == "dup-1" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("host consumed dup-1 %d times, want exactly 1 (dedupe on redelivery failed) — consumed=%v", count, consumed)
	}
}
