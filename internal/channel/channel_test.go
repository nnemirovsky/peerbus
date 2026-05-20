package channel_test

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

// testSecret is a non-secret 32-byte fixture (same shape the broker/adapter
// tests use).
var testSecret = []byte(strings.Repeat("peerbus-test-", 4)[:hmacpkg.MinSecretLen])

// ── in-process broker fixture (mirrors internal/mcp/server_test.go) ──

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
	return &brokerFixture{t: t, st: st, wsURL: "ws" + strings.TrimPrefix(hs.URL, "http")}
}

func (f *brokerFixture) cfg(name string) adapter.ClientConfig {
	return adapter.ClientConfig{URL: f.wsURL, Token: "tok", Name: name, HMACSecret: testSecret}
}

func (f *brokerFixture) rawClient(ctx context.Context, name string) *adapter.Client {
	f.t.Helper()
	c := adapter.NewClient(f.cfg(name))
	if err := c.Connect(ctx); err != nil {
		f.t.Fatalf("connect %s: %v", name, err)
	}
	return c
}

// ── cc bus over the real resuming client (mirrors internal/adapter/cc.go's
// ccBus — built from EXPORTED adapter APIs so the test drives the real
// broker WS client + reconnect/resume + shared dedupe + HMAC, not a fake) ──

type ccBus struct{ rc *adapter.ResumingClient }

func (b *ccBus) Send(ctx context.Context, to string, body json.RawMessage) error {
	c := b.rc.Client()
	if c == nil {
		return adapter.ErrNotConnected
	}
	return c.Send(ctx, "out-"+to+"-"+time.Now().Format("150405.000000000"), to, time.Now().UTC().Format(time.RFC3339Nano), "peer-bus", body)
}

func (b *ccBus) Broadcast(ctx context.Context, body json.RawMessage) error {
	c := b.rc.Client()
	if c == nil {
		return adapter.ErrNotConnected
	}
	return c.Broadcast(ctx, "bc-"+time.Now().Format("150405.000000000"), time.Now().UTC().Format(time.RFC3339Nano), "peer-bus", body)
}

func (b *ccBus) Peers(ctx context.Context) (string, []string, error) {
	self := b.rc.Name()
	c := b.rc.Client()
	if c == nil {
		return self, nil, adapter.ErrNotConnected
	}
	sink := make(chan []string, 1)
	c.SetPeersSink(sink)
	defer c.SetPeersSink(nil)
	if err := c.RequestPeers(ctx); err != nil {
		return self, nil, err
	}
	select {
	case names := <-sink:
		out := make([]string, 0, len(names))
		for _, n := range names {
			if n != self {
				out = append(out, n)
			}
		}
		return self, out, nil
	case <-ctx.Done():
		return self, nil, ctx.Err()
	case <-time.After(5 * time.Second):
		return self, nil, context.DeadlineExceeded
	}
}

// ── harness: real channel.Server wired to a real broker over pipes ──

type harness struct {
	t      *testing.T
	in     *io.PipeWriter
	out    *bufio.Reader
	frames chan json.RawMessage // single reader goroutine -> here
	srv    *channel.Server
	stop   func()
	nextID int
}

func newHarness(t *testing.T, f *brokerFixture, name string) *harness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	rc := adapter.NewResumingClient(f.cfg(name), 64)
	bus := &ccBus{rc: rc}

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	srv := channel.NewServer(bus, inR, outW)

	// resume loop: each verified+deduped delivery -> claude/channel push
	// (exactly what internal/adapter/cc.go's ccBus.handle does).
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		_ = rc.Run(ctx, func(_ context.Context, env wire.Envelope) error {
			srv.Deliver(channel.Inbound{
				ID: env.ID, From: env.From, Source: env.Source,
				Kind: string(env.Kind), Body: env.Body,
			})
			return nil
		})
	}()

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = srv.Serve(ctx)
	}()

	h := &harness{t: t, in: inW, out: bufio.NewReader(outR), frames: make(chan json.RawMessage, 16), srv: srv}

	// SINGLE stdout reader goroutine. Two ad-hoc reader goroutines on one
	// bufio.Reader race and steal each other's frames; funnel every frame
	// through one channel instead so readFrame/readFrameNoFail just select.
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			line, err := h.out.ReadBytes('\n')
			if len(line) > 0 {
				h.frames <- json.RawMessage(strings.TrimRight(string(line), "\r\n"))
			}
			if err != nil {
				return
			}
		}
	}()

	h.stop = func() {
		_ = inW.Close()
		cancel()
		<-serveDone
		<-loopDone
		_ = outW.Close()
		<-readerDone
	}
	t.Cleanup(h.stop)

	// wait for a live broker connection so injected messages are not lost.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, err := bus.Peers(ctx); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	h.initialize()
	return h
}

func (h *harness) sendReq(method string, params any) json.RawMessage {
	h.t.Helper()
	h.nextID++
	req := map[string]any{"jsonrpc": "2.0", "id": h.nextID, "method": method}
	if params != nil {
		req["params"] = params
	}
	b, _ := json.Marshal(req)
	if _, err := h.in.Write(append(b, '\n')); err != nil {
		h.t.Fatalf("write %s: %v", method, err)
	}
	return h.readFrame()
}

func (h *harness) notify(method string) {
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": method})
	if _, err := h.in.Write(append(b, '\n')); err != nil {
		h.t.Fatalf("notify %s: %v", method, err)
	}
}

// readFrame returns the next newline-delimited JSON-RPC frame off stdout (a
// response OR a server->client notification), failing on timeout.
func (h *harness) readFrame() json.RawMessage {
	h.t.Helper()
	select {
	case f := <-h.frames:
		return f
	case <-time.After(5 * time.Second):
		h.t.Fatalf("timed out waiting for a JSON-RPC frame")
		return nil
	}
}

// readFrameNoFail returns the next frame within d, or (nil,false) on
// timeout. Used to assert that NO notification is emitted (the unread frame
// stays queued in the single reader's channel, so a later readFrame still
// sees subsequent frames in order).
func (h *harness) readFrameNoFail(d time.Duration) (json.RawMessage, bool) {
	select {
	case f := <-h.frames:
		return f, true
	case <-time.After(d):
		return nil, false
	}
}

func (h *harness) initialize() {
	h.t.Helper()
	resp := h.sendReq("initialize", map[string]any{"protocolVersion": "2025-06-18"})
	var r struct {
		Result struct {
			Capabilities struct {
				Experimental map[string]json.RawMessage `json:"experimental"`
				Tools        json.RawMessage            `json:"tools"`
			} `json:"capabilities"`
			ServerInfo struct {
				Name string `json:"name"`
			} `json:"serverInfo"`
		} `json:"result"`
		Error *json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(resp, &r); err != nil {
		h.t.Fatalf("initialize decode: %v (%s)", err, resp)
	}
	if r.Error != nil {
		h.t.Fatalf("initialize error: %s", resp)
	}
	if _, ok := r.Result.Capabilities.Experimental["claude/channel"]; !ok {
		h.t.Fatalf("initialize result missing experimental[claude/channel]: %s", resp)
	}
	if len(r.Result.Capabilities.Tools) == 0 {
		h.t.Fatalf("initialize result missing tools capability: %s", resp)
	}
	if r.Result.ServerInfo.Name != "peerbus-cc-adapter" {
		h.t.Fatalf("serverInfo.name = %q, want peerbus-cc-adapter", r.Result.ServerInfo.Name)
	}
	h.notify("notifications/initialized")
}

func (h *harness) callTool(name string, args map[string]any) (structured map[string]any, isErr bool, rpcErr map[string]any) {
	h.t.Helper()
	params := map[string]any{"name": name}
	if args != nil {
		params["arguments"] = args
	}
	resp := h.sendReq("tools/call", params)
	var r struct {
		Result struct {
			StructuredContent map[string]any `json:"structuredContent"`
			IsError           bool           `json:"isError"`
		} `json:"result"`
		Error map[string]any `json:"error"`
	}
	if err := json.Unmarshal(resp, &r); err != nil {
		h.t.Fatalf("tools/call decode: %v (%s)", err, resp)
	}
	return r.Result.StructuredContent, r.Result.IsError, r.Error
}

// pushFrame is the decoded shape of a notifications/claude/channel frame.
type pushFrame struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id"`
	Method  string `json:"method"`
	Params  struct {
		Content string            `json:"content"`
		Meta    map[string]string `json:"meta"`
	} `json:"params"`
}

// ── tests ──

// TestInitializeAdvertisesChannelCapability: the initialize result advertises
// experimental["claude/channel"]={} AND tools, with the cc serverInfo name.
// (asserted inside newHarness.initialize)
func TestInitializeAdvertisesChannelCapability(t *testing.T) {
	f := newBrokerFixture(t)
	_ = newHarness(t, f, "cc-init")
}

// TestToolsListNoDrain: cc advertises bus.send/broadcast/peers and NOT
// bus.drain (it is push-driven, not host-drained).
func TestToolsListNoDrain(t *testing.T) {
	f := newBrokerFixture(t)
	h := newHarness(t, f, "cc-tools")
	resp := h.sendReq("tools/list", nil)
	var r struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(resp, &r); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := map[string]bool{}
	for _, tl := range r.Result.Tools {
		got[tl.Name] = true
	}
	for _, want := range []string{"bus.send", "bus.broadcast", "bus.peers"} {
		if !got[want] {
			t.Fatalf("tools/list missing %q; got %v", want, got)
		}
	}
	if got["bus.drain"] {
		t.Fatalf("cc must NOT advertise bus.drain; got %v", got)
	}
}

// TestNotificationMapping: a broker deliver becomes exactly one
// notifications/claude/channel push with correct content + meta
// (from/source/msg_id); a repeat id is suppressed by the shared dedupe; a
// forged-HMAC inbound is skipped (no notification, no crash).
func TestNotificationMapping(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h := newHarness(t, f, "cc-rx")

	// peer "tx" sends a direct message to the cc adapter.
	tx := f.rawClient(ctx, "tx")
	defer tx.Close()

	body, _ := json.Marshal("hello from tx")
	if err := tx.Send(ctx, "msg-1", "cc-rx", time.Now().UTC().Format(time.RFC3339Nano), "peer-bus", body); err != nil {
		t.Fatalf("tx send: %v", err)
	}

	frame := h.readFrame()
	var pf pushFrame
	if err := json.Unmarshal(frame, &pf); err != nil {
		t.Fatalf("push decode: %v (%s)", err, frame)
	}
	if pf.ID != nil {
		t.Fatalf("push must be a notification (no id); got %s", frame)
	}
	if pf.Method != "notifications/claude/channel" {
		t.Fatalf("method = %q, want notifications/claude/channel", pf.Method)
	}
	// Pretty content: 📨 banner + From/Type/Content lines. Body is a JSON
	// string ("hello from tx"), so decodeBody unwraps it to that text.
	want := "\U0001F4E8 peerbus message\nFrom: tx\nType: msg\nContent: hello from tx"
	if pf.Params.Content != want {
		t.Fatalf("content = %q, want %q", pf.Params.Content, want)
	}
	if pf.Params.Meta["from"] != "tx" {
		t.Fatalf("meta.from = %q, want tx", pf.Params.Meta["from"])
	}
	if pf.Params.Meta["source"] != "peer-bus" {
		t.Fatalf("meta.source = %q, want peer-bus", pf.Params.Meta["source"])
	}
	if pf.Params.Meta["msg_id"] != "msg-1" {
		t.Fatalf("meta.msg_id = %q, want msg-1", pf.Params.Meta["msg_id"])
	}
	if pf.Params.Meta["kind"] != "msg" {
		t.Fatalf("meta.kind = %q, want msg", pf.Params.Meta["kind"])
	}

	// Re-send the SAME id: broker dedups by UNIQUE(id) so it never even
	// reaches the adapter again -> no second push.
	if err := tx.Send(ctx, "msg-1", "cc-rx", time.Now().UTC().Format(time.RFC3339Nano), "peer-bus", body); err != nil {
		t.Fatalf("tx resend: %v", err)
	}
	if frame, ok := h.readFrameNoFail(400 * time.Millisecond); ok {
		t.Fatalf("duplicate id produced a second push: %s", frame)
	}

	// A NEW id still pushes (loop survived the duplicate).
	body2, _ := json.Marshal("second")
	if err := tx.Send(ctx, "msg-2", "cc-rx", time.Now().UTC().Format(time.RFC3339Nano), "peer-bus", body2); err != nil {
		t.Fatalf("tx send 2: %v", err)
	}
	frame2 := h.readFrame()
	var pf2 pushFrame
	if err := json.Unmarshal(frame2, &pf2); err != nil {
		t.Fatalf("push2 decode: %v (%s)", err, frame2)
	}
	if !strings.Contains(pf2.Params.Content, "Content: second") ||
		pf2.Params.Meta["msg_id"] != "msg-2" {
		t.Fatalf("unexpected second push: %s", frame2)
	}
}

// TestForgedInboundSkipped: a message signed with the WRONG secret is
// HMAC-rejected by the shared client and never surfaced as a push; a
// subsequent well-signed message still pushes (loop did not crash).
func TestForgedInboundSkipped(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h := newHarness(t, f, "cc-rx2")

	// forged peer: connects with a DIFFERENT HMAC secret -> its signature
	// fails the recipient's verify.
	forgedCfg := f.cfg("forger")
	forgedCfg.HMACSecret = []byte(strings.Repeat("wrong-secret-x", 3)[:hmacpkg.MinSecretLen])
	forged := adapter.NewClient(forgedCfg)
	if err := forged.Connect(ctx); err != nil {
		t.Fatalf("forged connect: %v", err)
	}
	defer forged.Close()

	fb, _ := json.Marshal("forged payload")
	if err := forged.Send(ctx, "forged-1", "cc-rx2", time.Now().UTC().Format(time.RFC3339Nano), "peer-bus", fb); err != nil {
		t.Fatalf("forged send: %v", err)
	}
	if frame, ok := h.readFrameNoFail(500 * time.Millisecond); ok {
		t.Fatalf("forged message produced a push (must be skipped): %s", frame)
	}

	// a legitimately-signed message still pushes.
	tx := f.rawClient(ctx, "tx2")
	defer tx.Close()
	gb, _ := json.Marshal("legit")
	if err := tx.Send(ctx, "legit-1", "cc-rx2", time.Now().UTC().Format(time.RFC3339Nano), "peer-bus", gb); err != nil {
		t.Fatalf("legit send: %v", err)
	}
	frame := h.readFrame()
	var pf pushFrame
	if err := json.Unmarshal(frame, &pf); err != nil {
		t.Fatalf("legit push decode: %v (%s)", err, frame)
	}
	if !strings.Contains(pf.Params.Content, "Content: legit") {
		t.Fatalf("content = %q, want pretty body 'Content: legit'", pf.Params.Content)
	}
}

// TestOutboundTools: bus.send / bus.broadcast / bus.peers hit the in-process
// broker and the outbound message is HMAC-verifiable end-to-end (direct).
func TestOutboundTools(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rx := f.rawClient(ctx, "peer2")
	defer rx.Close()

	h := newHarness(t, f, "cc-sender")

	// bus.peers lists the registry as {self, peers}.
	st, isErr, rpcErr := h.callTool("bus.peers", nil)
	if rpcErr != nil || isErr {
		t.Fatalf("bus.peers failed: rpcErr=%v isErr=%v", rpcErr, isErr)
	}
	if self, _ := st["self"].(string); self != "cc-sender" {
		t.Fatalf("bus.peers self = %v, want cc-sender", st["self"])
	}
	peers, _ := st["peers"].([]any)
	found := false
	for _, p := range peers {
		if p == "peer2" {
			found = true
		}
		if p == "cc-sender" {
			t.Fatalf("bus.peers must not include self; got %v", peers)
		}
	}
	if !found {
		t.Fatalf("bus.peers missing peer2: %v", st["peers"])
	}

	// bus.send -> peer2 receives an HMAC-verifiable direct message.
	_, isErr, rpcErr = h.callTool("bus.send", map[string]any{
		"to":   "peer2",
		"body": map[string]any{"hi": "there"},
	})
	if rpcErr != nil || isErr {
		t.Fatalf("bus.send failed: rpcErr=%v isErr=%v", rpcErr, isErr)
	}
	del, err := rx.Recv(ctx) // Recv HMAC-verifies; an error here = bad signature
	if err != nil {
		t.Fatalf("peer2 recv (HMAC verify): %v", err)
	}
	env := del.Envelope
	if env.From != "cc-sender" {
		t.Fatalf("from = %q, want cc-sender", env.From)
	}
	var got map[string]string
	if err := json.Unmarshal(env.Body, &got); err != nil || got["hi"] != "there" {
		t.Fatalf("unexpected body %s (%v)", env.Body, err)
	}

	// bus.broadcast: a raw recipient must actually receive the broadcast,
	// end-to-end HMAC-verifiable (the broker delivers the sender's verbatim
	// signed envelope; per-recipient routing rides on DeliveryKey).
	bx := f.rawClient(ctx, "bx")
	defer bx.Close()
	_, isErr, rpcErr = h.callTool("bus.broadcast", map[string]any{
		"body": map[string]any{"all": "hands"},
	})
	if rpcErr != nil || isErr {
		t.Fatalf("bus.broadcast failed: rpcErr=%v isErr=%v", rpcErr, isErr)
	}
	bdel, berr := bx.Recv(ctx)
	if berr != nil {
		t.Fatalf("broadcast not delivered to bx / failed HMAC: %v", berr)
	}
	if bdel.Envelope.From != "cc-sender" || bdel.Envelope.To != "*" {
		t.Fatalf("broadcast env = from %q to %q, want cc-sender / *", bdel.Envelope.From, bdel.Envelope.To)
	}
	if bdel.DeliveryKey == "" || bdel.DeliveryKey == bdel.Envelope.ID {
		t.Fatalf("broadcast delivery_key = %q, want per-recipient key != id", bdel.DeliveryKey)
	}
	var allGot map[string]string
	if err := json.Unmarshal(bdel.Envelope.Body, &allGot); err != nil || allGot["all"] != "hands" {
		t.Fatalf("broadcast body = %s (%v), want {\"all\":\"hands\"}", bdel.Envelope.Body, err)
	}
}

// TestUniqueName: auto-registration mints distinct, lowercase
// <adjective>-<noun>-<3 base36> names. The exact corpus is intentionally
// an implementation detail; only the shape is contractual.
func TestUniqueName(t *testing.T) {
	a := channel.UniqueName()
	b := channel.UniqueName()
	if a == "" || b == "" {
		t.Fatalf("UniqueName returned empty (a=%q b=%q)", a, b)
	}
	if a == b {
		t.Fatalf("UniqueName not unique across two calls: %q == %q", a, b)
	}
	for _, n := range []string{a, b} {
		parts := strings.Split(n, "-")
		if len(parts) != 3 {
			t.Fatalf("UniqueName %q: want three hyphen-separated parts, got %d", n, len(parts))
		}
		if got := len(parts[2]); got != 3 {
			t.Fatalf("UniqueName %q: suffix len = %d, want 3", n, got)
		}
		for _, p := range parts {
			if p == "" {
				t.Fatalf("UniqueName %q: empty segment", n)
			}
			for _, r := range p {
				if (r < 'a' || r > 'z') && (r < '0' || r > '9') {
					t.Fatalf("UniqueName %q: non-[a-z0-9] char %q", n, r)
				}
			}
		}
	}
}

// TestUniqueName_EnvOverride: PEERBUS_NAME, when set, is honoured verbatim
// (operator escape hatch — bypasses the friendly-name generator).
func TestUniqueName_EnvOverride(t *testing.T) {
	t.Setenv("PEERBUS_NAME", "fixed-operator-name")
	if got := channel.UniqueName(); got != "fixed-operator-name" {
		t.Fatalf("PEERBUS_NAME override: got %q, want fixed-operator-name", got)
	}
}

// TestAnnounceSelf: the startup self-announcement is ONE
// notifications/claude/channel push with the correct content + meta
// (kind=system, self=<name>). The cc adapter fires this once per session
// after a successful register so the consuming Claude session immediately
// knows its identity.
func TestAnnounceSelf(t *testing.T) {
	f := newBrokerFixture(t)
	h := newHarness(t, f, "cc-announce")
	h.srv.AnnounceSelf("cc-announce")
	frame := h.readFrame()
	var pf pushFrame
	if err := json.Unmarshal(frame, &pf); err != nil {
		t.Fatalf("decode: %v (%s)", err, frame)
	}
	if pf.Method != "notifications/claude/channel" {
		t.Fatalf("method = %q, want notifications/claude/channel", pf.Method)
	}
	want := "\U0001F4E1 peerbus: connected as cc-announce"
	if pf.Params.Content != want {
		t.Fatalf("content = %q, want %q", pf.Params.Content, want)
	}
	if pf.Params.Meta["kind"] != "system" {
		t.Fatalf("meta.kind = %q, want system", pf.Params.Meta["kind"])
	}
	if pf.Params.Meta["self"] != "cc-announce" {
		t.Fatalf("meta.self = %q, want cc-announce", pf.Params.Meta["self"])
	}
}

// ── self-announce gating: mirrors internal/adapter/cc.go wiring ──
//
// gatedHarness spins up the same triple as ccMode.Run — a real channel.Server
// over pipes, a real ResumingClient against a real in-process broker, the
// resume loop wired to forward inbound deliveries as channel pushes, AND the
// SetOnConnect(<-srv.Initialized()) -> AnnounceSelf gate that is the subject
// of these tests. The host side does NOT auto-send notifications/initialized
// (unlike newHarness); each test drives the handshake explicitly so it can
// observe what does and does not arrive before initialized.
type gatedHarness struct {
	t        *testing.T
	in       *io.PipeWriter
	out      *bufio.Reader
	frames   chan json.RawMessage
	srv      *channel.Server
	rc       *adapter.ResumingClient
	stop     func()
	cancel   context.CancelFunc
	loopDone <-chan struct{}
	nextID   int
}

func newGatedHarness(t *testing.T, f *brokerFixture, name string) *gatedHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	rc := adapter.NewResumingClient(f.cfg(name), 64)
	bus := &ccBus{rc: rc}

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	srv := channel.NewServer(bus, inR, outW)

	// EXACTLY the gate cc.go installs: announce on every successful
	// (re)register, but only after MCP notifications/initialized.
	initialized := srv.Initialized()
	rc.SetOnConnect(func() {
		select {
		case <-initialized:
		case <-ctx.Done():
			return
		}
		srv.AnnounceSelf(rc.Name())
	})

	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		_ = rc.Run(ctx, func(_ context.Context, env wire.Envelope) error {
			srv.Deliver(channel.Inbound{
				ID: env.ID, From: env.From, Source: env.Source,
				Kind: string(env.Kind), Body: env.Body,
			})
			return nil
		})
	}()

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = srv.Serve(ctx)
	}()

	h := &gatedHarness{
		t: t, in: inW, out: bufio.NewReader(outR),
		frames: make(chan json.RawMessage, 16),
		srv:    srv, rc: rc, cancel: cancel, loopDone: loopDone,
	}

	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			line, err := h.out.ReadBytes('\n')
			if len(line) > 0 {
				h.frames <- json.RawMessage(strings.TrimRight(string(line), "\r\n"))
			}
			if err != nil {
				return
			}
		}
	}()

	h.stop = func() {
		_ = inW.Close()
		cancel()
		<-serveDone
		<-loopDone
		_ = outW.Close()
		<-readerDone
	}
	t.Cleanup(h.stop)
	return h
}

func (h *gatedHarness) sendReq(method string, params any) json.RawMessage {
	h.t.Helper()
	h.nextID++
	req := map[string]any{"jsonrpc": "2.0", "id": h.nextID, "method": method}
	if params != nil {
		req["params"] = params
	}
	b, _ := json.Marshal(req)
	if _, err := h.in.Write(append(b, '\n')); err != nil {
		h.t.Fatalf("write %s: %v", method, err)
	}
	return h.readFrame()
}

func (h *gatedHarness) notify(method string) {
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": method})
	if _, err := h.in.Write(append(b, '\n')); err != nil {
		h.t.Fatalf("notify %s: %v", method, err)
	}
}

func (h *gatedHarness) readFrame() json.RawMessage {
	h.t.Helper()
	select {
	case f := <-h.frames:
		return f
	case <-time.After(5 * time.Second):
		h.t.Fatalf("timed out waiting for a JSON-RPC frame")
		return nil
	}
}

func (h *gatedHarness) readFrameNoFail(d time.Duration) (json.RawMessage, bool) {
	select {
	case f := <-h.frames:
		return f, true
	case <-time.After(d):
		return nil, false
	}
}

// waitForRegister waits until the resuming client has a live broker
// connection (so the OnConnect callback is guaranteed to have fired at least
// once). Mirrors newHarness's bus.Peers ping but works without the MCP
// handshake having completed (peers is an outbound broker RPC, not an MCP
// tool call here).
func (h *gatedHarness) waitForRegister() {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h.rc.Client() != nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	h.t.Fatalf("resuming client never registered")
}

// TestAnnounceSelfGatedOnInitialized: AnnounceSelf MUST NOT fire before the
// MCP client has sent notifications/initialized. Even after a successful
// broker register (OnConnect has fired), no claude/channel push reaches
// stdout until the handshake completes — Claude Code silently drops
// server-initiated notifications received before initialized
// (CHANNELS_SCHEMA.md §3) and we'd be writing them into the void.
func TestAnnounceSelfGatedOnInitialized(t *testing.T) {
	f := newBrokerFixture(t)
	h := newGatedHarness(t, f, "cc-gated")
	h.waitForRegister()

	// Broker register is complete; OnConnect has been invoked. NO MCP
	// handshake has happened yet. Assert nothing arrives.
	if frame, ok := h.readFrameNoFail(400 * time.Millisecond); ok {
		t.Fatalf("announce leaked before notifications/initialized: %s", frame)
	}

	// Now drive the handshake; the announce should land promptly.
	resp := h.sendReq("initialize", map[string]any{"protocolVersion": "2025-06-18"})
	if !strings.Contains(string(resp), `"protocolVersion"`) {
		t.Fatalf("initialize response unexpected: %s", resp)
	}
	h.notify("notifications/initialized")

	frame := h.readFrame()
	var pf pushFrame
	if err := json.Unmarshal(frame, &pf); err != nil {
		t.Fatalf("announce decode: %v (%s)", err, frame)
	}
	if pf.Method != "notifications/claude/channel" {
		t.Fatalf("method = %q, want notifications/claude/channel", pf.Method)
	}
	if pf.Params.Meta["kind"] != "system" || pf.Params.Meta["self"] != "cc-gated" {
		t.Fatalf("meta = %v, want kind=system self=cc-gated", pf.Params.Meta)
	}
	if want := "\U0001F4E1 peerbus: connected as cc-gated"; pf.Params.Content != want {
		t.Fatalf("content = %q, want %q", pf.Params.Content, want)
	}

	// Exactly ONE announce per register. No spurious second push.
	if frame, ok := h.readFrameNoFail(300 * time.Millisecond); ok {
		t.Fatalf("second announce after single register: %s", frame)
	}
}

// TestAnnounceSelfReannouncesOnReconnect: a broker drop + redial under the
// same name triggers a SECOND announce. The first connect's announce already
// fired (the session is past initialized), the resume loop reconnects on
// drop and OnConnect runs again — the wait on initialized is a no-op the
// second time so the banner lands immediately. Keeps the connected-as line
// reliable across mid-session broker flaps.
func TestAnnounceSelfReannouncesOnReconnect(t *testing.T) {
	f := newBrokerFixture(t)
	h := newGatedHarness(t, f, "cc-reconnect")
	h.waitForRegister()

	// Complete the handshake; consume the first announce.
	_ = h.sendReq("initialize", map[string]any{"protocolVersion": "2025-06-18"})
	h.notify("notifications/initialized")
	first := h.readFrame()
	var pf1 pushFrame
	if err := json.Unmarshal(first, &pf1); err != nil {
		t.Fatalf("first announce decode: %v (%s)", err, first)
	}
	if pf1.Params.Meta["self"] != "cc-reconnect" {
		t.Fatalf("first announce self = %q, want cc-reconnect", pf1.Params.Meta["self"])
	}

	// Force a transport drop by closing the current Client. The resume
	// loop redials under the same name and OnConnect fires again.
	if c := h.rc.Client(); c != nil {
		c.Close()
	}

	// A second announce MUST arrive after the redial succeeds.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		frame, ok := h.readFrameNoFail(200 * time.Millisecond)
		if !ok {
			continue
		}
		var pf2 pushFrame
		if err := json.Unmarshal(frame, &pf2); err != nil {
			t.Fatalf("decode reconnect frame: %v (%s)", err, frame)
		}
		// Skip any pre-existing buffered frames (none expected here).
		if pf2.Method == "notifications/claude/channel" &&
			pf2.Params.Meta["kind"] == "system" &&
			pf2.Params.Meta["self"] == "cc-reconnect" {
			return
		}
		t.Fatalf("unexpected frame after reconnect: %s", frame)
	}
	t.Fatalf("no re-announce within 3s of reconnect")
}

// TestPrettyContentDecoding exercises the three decode branches of the
// pretty-content body decoder via direct Server.Deliver calls (the broker
// path is covered by the live-server tests above). Each branch maps the
// opaque body JSON to the human-readable Content: line.
func TestPrettyContentDecoding(t *testing.T) {
	cases := []struct {
		name string
		kind string
		body string
		want string // expected trailing Content: <decoded>
	}{
		{"string-body", "msg", `"plain hello"`, "Content: plain hello"},
		{"object-text-field", "msg", `{"text":"hi there"}`, "Content: hi there"},
		{"object-message-field", "broadcast", `{"message":"all hands"}`, "Content: all hands"},
		{"object-content-field", "msg", `{"content":"yet another"}`, "Content: yet another"},
		{"object-fallback", "msg", `{"foo":"bar"}`, `Content: {"foo":"bar"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newBrokerFixture(t)
			h := newHarness(t, f, "cc-fmt-"+tc.name)
			h.srv.Deliver(channel.Inbound{
				ID: "id-" + tc.name, From: "tx", Source: "peer-bus",
				Kind: tc.kind, Body: json.RawMessage(tc.body),
			})
			frame := h.readFrame()
			var pf pushFrame
			if err := json.Unmarshal(frame, &pf); err != nil {
				t.Fatalf("decode: %v (%s)", err, frame)
			}
			if !strings.HasPrefix(pf.Params.Content, "\U0001F4E8 peerbus message\n") {
				t.Fatalf("missing pretty banner: %q", pf.Params.Content)
			}
			wantType := "Type: " + tc.kind + "\n"
			if !strings.Contains(pf.Params.Content, wantType) {
				t.Fatalf("missing %q in %q", wantType, pf.Params.Content)
			}
			if !strings.HasSuffix(pf.Params.Content, tc.want) {
				t.Fatalf("content = %q, want suffix %q", pf.Params.Content, tc.want)
			}
			if pf.Params.Meta["kind"] != tc.kind {
				t.Fatalf("meta.kind = %q, want %q", pf.Params.Meta["kind"], tc.kind)
			}
		})
	}
}
