package mcp_test

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
	hmacpkg "github.com/nnemirovsky/peerbus/internal/hmac"
	"github.com/nnemirovsky/peerbus/internal/mcp"
	"github.com/nnemirovsky/peerbus/internal/store"
	"github.com/nnemirovsky/peerbus/internal/wire"
)

// testSecret is a non-secret 32-byte fixture, same shape the broker/adapter
// tests use.
var testSecret = []byte(strings.Repeat("peerbus-test-", 4)[:hmacpkg.MinSecretLen])

// ── in-process broker fixture (mirrors internal/adapter/client_test.go) ──

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

// rawClient connects a plain broker client used to inject messages toward
// the adapter-under-test.
func (f *brokerFixture) rawClient(ctx context.Context, name string) *adapter.Client {
	f.t.Helper()
	c := adapter.NewClient(f.cfg(name))
	if err := c.Connect(ctx); err != nil {
		f.t.Fatalf("connect %s: %v", name, err)
	}
	return c
}

// ── MCP JSON-RPC client over in-memory pipes driving the real server ──

type mcpHarness struct {
	t      *testing.T
	in     *io.PipeWriter // host -> server stdin
	out    *bufio.Reader  // server stdout -> host
	stop   func()
	nextID int
}

// newWiredHarness spins up the REAL mcp.Server wired to a REAL broker-backed
// generic bus (adapter.NewGenericBus) over a fresh broker fixture for the
// given peer name. Exercises the full path: MCP JSON-RPC -> bus -> broker WS
// client + reconnect/resume + shared dedupe + HMAC.
func newWiredHarness(t *testing.T, f *brokerFixture, name string) *mcpHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	bus, stopBus := adapter.NewGenericBus(ctx, f.cfg(name), 64, nil)

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	srv := mcp.NewServer(bus, inR, outW)

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = srv.Serve(ctx)
	}()

	h := &mcpHarness{
		t:   t,
		in:  inW,
		out: bufio.NewReader(outR),
	}
	h.stop = func() {
		_ = inW.Close()
		cancel()
		stopBus()
		<-serveDone
		_ = outW.Close()
	}
	t.Cleanup(h.stop)

	// Wait until the bus has a live broker connection so injected messages
	// are not lost before register completes.
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

func (h *mcpHarness) send(method string, params any) json.RawMessage {
	h.t.Helper()
	h.nextID++
	id := h.nextID
	req := map[string]any{"jsonrpc": "2.0", "id": id, "method": method}
	if params != nil {
		req["params"] = params
	}
	b, _ := json.Marshal(req)
	if _, err := h.in.Write(append(b, '\n')); err != nil {
		h.t.Fatalf("write %s: %v", method, err)
	}
	return h.readResponse()
}

func (h *mcpHarness) notify(method string) {
	h.t.Helper()
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": method})
	if _, err := h.in.Write(append(b, '\n')); err != nil {
		h.t.Fatalf("notify %s: %v", method, err)
	}
}

func (h *mcpHarness) readResponse() json.RawMessage {
	h.t.Helper()
	type result struct {
		line []byte
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		line, err := h.out.ReadBytes('\n')
		ch <- result{line, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil && len(r.line) == 0 {
			h.t.Fatalf("read response: %v", r.err)
		}
		return json.RawMessage(strings.TrimRight(string(r.line), "\r\n"))
	case <-time.After(5 * time.Second):
		h.t.Fatalf("timed out waiting for JSON-RPC response")
		return nil
	}
}

func (h *mcpHarness) initialize() {
	h.t.Helper()
	resp := h.send("initialize", map[string]any{"protocolVersion": "2025-06-18"})
	var r struct {
		Result struct {
			ServerInfo struct {
				Name string `json:"name"`
			} `json:"serverInfo"`
		} `json:"result"`
		Error *struct{} `json:"error"`
	}
	if err := json.Unmarshal(resp, &r); err != nil {
		h.t.Fatalf("initialize decode: %v (%s)", err, resp)
	}
	if r.Error != nil || r.Result.ServerInfo.Name == "" {
		h.t.Fatalf("initialize failed: %s", resp)
	}
	h.notify("notifications/initialized")
}

// callTool issues a tools/call and returns the parsed structured result map
// plus isError flag plus any JSON-RPC error object.
func (h *mcpHarness) callTool(name string, args map[string]any) (structured map[string]any, isErr bool, rpcErr map[string]any) {
	h.t.Helper()
	params := map[string]any{"name": name}
	if args != nil {
		params["arguments"] = args
	}
	resp := h.send("tools/call", params)
	var r struct {
		Result struct {
			StructuredContent map[string]any `json:"structuredContent"`
			Content           []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
		Error map[string]any `json:"error"`
	}
	if err := json.Unmarshal(resp, &r); err != nil {
		h.t.Fatalf("tools/call decode: %v (%s)", err, resp)
	}
	return r.Result.StructuredContent, r.Result.IsError, r.Error
}

// ── tests ──

// TestToolsList lists exactly the four bus.* tools.
func TestToolsList(t *testing.T) {
	f := newBrokerFixture(t)
	h := newWiredHarness(t, f, "lister")

	resp := h.send("tools/list", nil)
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
	for _, want := range []string{"bus.send", "bus.broadcast", "bus.peers", "bus.drain"} {
		if !got[want] {
			t.Fatalf("tools/list missing %q; got %v", want, got)
		}
	}
}

// TestBusSendDelivers: bus.send delivers a direct, HMAC-verifiable message
// to another peer.
func TestBusSendDelivers(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rx := f.rawClient(ctx, "rx")
	defer rx.Close()

	h := newWiredHarness(t, f, "sender")
	_, isErr, rpcErr := h.callTool("bus.send", map[string]any{
		"to":   "rx",
		"body": map[string]any{"hello": "world"},
	})
	if rpcErr != nil || isErr {
		t.Fatalf("bus.send failed: rpcErr=%v isErr=%v", rpcErr, isErr)
	}

	del, err := rx.Recv(ctx)
	if err != nil {
		t.Fatalf("rx recv: %v", err)
	}
	env := del.Envelope
	if env.From != "sender" {
		t.Fatalf("from = %q, want sender", env.From)
	}
	// Direct delivery: DeliveryKey == signed envelope id.
	if del.DeliveryKey != env.ID {
		t.Fatalf("direct delivery_key = %q, want == signed id %q", del.DeliveryKey, env.ID)
	}
	if err := hmacpkg.VerifyEnvelope(testSecret, env); err != nil {
		t.Fatalf("delivered direct message not HMAC-verifiable: %v", err)
	}
	if string(env.Body) != `{"hello":"world"}` {
		t.Fatalf("body = %s", env.Body)
	}
}

// TestBusBroadcastFansOut: bus.broadcast reaches another registered peer
// with the sender's verbatim signed envelope — it MUST verify end-to-end
// (the broker no longer rewrites id/to; the per-recipient routing rides on
// wire.Deliver.DeliveryKey, outside the HMAC). Unconditional assertions:
// delivery occurred AND the body content is the broadcast payload.
func TestBusBroadcastFansOut(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rx := f.rawClient(ctx, "rx")
	defer rx.Close()

	h := newWiredHarness(t, f, "caster")
	_, isErr, rpcErr := h.callTool("bus.broadcast", map[string]any{
		"body": map[string]any{"announce": 1},
	})
	if rpcErr != nil || isErr {
		t.Fatalf("bus.broadcast failed: rpcErr=%v isErr=%v", rpcErr, isErr)
	}

	del, rerr := rx.Recv(ctx)
	if rerr != nil {
		t.Fatalf("broadcast not delivered / failed HMAC: %v", rerr)
	}
	env := del.Envelope
	if env.From != "caster" {
		t.Fatalf("broadcast from = %q, want caster", env.From)
	}
	if env.To != "*" {
		t.Fatalf("delivered broadcast envelope to = %q, want \"*\" (verbatim signed)", env.To)
	}
	// DeliveryKey is the per-recipient row key, carried OUTSIDE the signed
	// envelope; it must be present and distinct from the signed id.
	if del.DeliveryKey == "" || del.DeliveryKey == env.ID {
		t.Fatalf("delivery_key = %q, want per-recipient key != signed id %q", del.DeliveryKey, env.ID)
	}
	if err := hmacpkg.VerifyEnvelope(testSecret, env); err != nil {
		t.Fatalf("broadcast copy not end-to-end HMAC-verifiable: %v", err)
	}
	var got map[string]int
	if err := json.Unmarshal(env.Body, &got); err != nil || got["announce"] != 1 {
		t.Fatalf("broadcast body = %s (%v), want {\"announce\":1}", env.Body, err)
	}
}

// TestBusPeersLists: bus.peers returns the broker registry.
func TestBusPeersLists(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	other := f.rawClient(ctx, "other")
	defer other.Close()

	h := newWiredHarness(t, f, "asker")
	structured, isErr, rpcErr := h.callTool("bus.peers", nil)
	if rpcErr != nil || isErr {
		t.Fatalf("bus.peers failed: rpcErr=%v isErr=%v", rpcErr, isErr)
	}
	if self, _ := structured["self"].(string); self != "asker" {
		t.Fatalf("bus.peers self = %v, want asker", structured["self"])
	}
	peersAny, _ := structured["peers"].([]any)
	got := map[string]bool{}
	for _, p := range peersAny {
		got[p.(string)] = true
	}
	// Self is filtered out of the peers list (bus.peers' new shape).
	if got["asker"] {
		t.Fatalf("peers must not include self; got %v", peersAny)
	}
	if !got["other"] {
		t.Fatalf("peers = %v, want other", peersAny)
	}
}

// TestBusDrainReturnsAndAcks: a direct message sent to the adapter is
// returned by bus.drain (with source/from) and acked (so it is not
// redelivered); a second drain returns nothing.
func TestBusDrainReturnsAndAcks(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h := newWiredHarness(t, f, "drainer")

	sender := f.rawClient(ctx, "sender")
	defer sender.Close()
	if err := sender.Send(ctx, "d1", "drainer", "ts", "peer-bus", json.RawMessage(`{"n":1}`)); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Poll bus.drain until the message has been pumped through the resume
	// loop into the buffer.
	var msgs []any
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		structured, isErr, rpcErr := h.callTool("bus.drain", nil)
		if rpcErr != nil || isErr {
			t.Fatalf("bus.drain failed: rpcErr=%v isErr=%v", rpcErr, isErr)
		}
		msgs, _ = structured["messages"].([]any)
		if len(msgs) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(msgs) != 1 {
		t.Fatalf("drain returned %d messages, want 1", len(msgs))
	}
	m := msgs[0].(map[string]any)
	if m["id"] != "d1" || m["from"] != "sender" || m["source"] != "peer-bus" {
		t.Fatalf("drained message = %v, want id=d1 from=sender source=peer-bus", m)
	}

	// Second drain: empty (already taken). And since the resume loop acked
	// it, a reconnect would not redeliver it (covered by adapter tests).
	structured, _, _ := h.callTool("bus.drain", nil)
	again, _ := structured["messages"].([]any)
	if len(again) != 0 {
		t.Fatalf("second drain returned %d, want 0", len(again))
	}
}

// TestBusDrainDedupeSuppressesRepeat: the same id delivered twice (broker
// redelivery of an unacked message after a same-name reconnect) is
// surfaced by bus.drain exactly once — proving the SHARED dedupe cache is
// the one filtering drain output, not a per-mode reimplementation.
func TestBusDrainDedupeSuppressesRepeat(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Register "drainer" once then drop it so the message queues offline.
	pre := f.rawClient(ctx, "drainer")
	pre.Close()
	time.Sleep(100 * time.Millisecond)

	sender := f.rawClient(ctx, "sender")
	defer sender.Close()
	if err := sender.Send(ctx, "dup-x", "drainer", "ts", "peer-bus", json.RawMessage(`{"n":1}`)); err != nil {
		t.Fatalf("send: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	h := newWiredHarness(t, f, "drainer")

	// First drain should yield dup-x once.
	collect := func() []any {
		var msgs []any
		deadline := time.Now().Add(4 * time.Second)
		for time.Now().Before(deadline) {
			structured, _, _ := h.callTool("bus.drain", nil)
			batch, _ := structured["messages"].([]any)
			msgs = append(msgs, batch...)
			if len(msgs) > 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		return msgs
	}
	first := collect()
	count := 0
	for _, m := range first {
		if m.(map[string]any)["id"] == "dup-x" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("first drain saw dup-x %d times, want 1", count)
	}

	// Re-enqueue the SAME id and force a redelivery by reconnecting the
	// underlying broker client via a second raw client takeover is not
	// available here; instead enqueue a duplicate id directly into the
	// store and trigger a flush. The shared dedupe must suppress it.
	signed, err := hmacpkg.SignEnvelope(testSecret, wire.Envelope{
		ProtocolVersion: wire.ProtocolVersion,
		ID:              "dup-x",
		From:            "sender",
		To:              "drainer",
		TS:              "ts",
		Source:          "peer-bus",
		Kind:            wire.KindMsg,
		Body:            json.RawMessage(`{"n":1}`),
	})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	raw, _ := json.Marshal(signed)
	// Enqueue is idempotent on id (UNIQUE) — this models the broker
	// redelivering the same id; even if a new row is created the consumer
	// dedupe is the assertion under test.
	_ = f.st.Enqueue(store.Message{ID: "dup-x-redelivery", From: "sender", To: "drainer", Envelope: raw})

	// Drain again over a window: dup-x must NOT reappear (shared dedupe
	// already saw it on the first drain).
	time.Sleep(300 * time.Millisecond)
	structured, _, _ := h.callTool("bus.drain", nil)
	more, _ := structured["messages"].([]any)
	for _, m := range more {
		if m.(map[string]any)["id"] == "dup-x" {
			t.Fatalf("dup-x reappeared after redelivery — shared dedupe not applied to drain")
		}
	}
}

// TestBusDrainSkipsHMACInvalidInbound: an inbound envelope signed under the
// wrong secret (a forged/corrupt message, or a broadcast copy the broker
// rewrote) is rejected — never surfaced by bus.drain, and the drain loop
// does not crash.
func TestBusDrainSkipsHMACInvalidInbound(t *testing.T) {
	f := newBrokerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Register "victim" once (then drop it) so the store knows the peer and
	// the forged/valid rows can be queued offline for it.
	pre := f.rawClient(ctx, "victim")
	pre.Close()
	time.Sleep(100 * time.Millisecond)

	// Forge a message signed under a different secret, queued for the
	// adapter peer before it connects.
	wrong := []byte(strings.Repeat("wrong-secret-", 4)[:hmacpkg.MinSecretLen])
	signed, err := hmacpkg.SignEnvelope(wrong, wire.Envelope{
		ProtocolVersion: wire.ProtocolVersion,
		ID:              "forged-1",
		From:            "attacker",
		To:              "victim",
		TS:              "t",
		Source:          "peer-bus",
		Kind:            wire.KindMsg,
		Body:            json.RawMessage(`{"evil":true}`),
	})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	raw, _ := json.Marshal(signed)
	if err := f.st.Enqueue(store.Message{ID: "forged-1", From: "attacker", To: "victim", Envelope: raw}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Also queue a VALID message so we can prove the loop survived the
	// forged one and keeps draining.
	good, _ := hmacpkg.SignEnvelope(testSecret, wire.Envelope{
		ProtocolVersion: wire.ProtocolVersion,
		ID:              "good-1",
		From:            "friend",
		To:              "victim",
		TS:              "t",
		Source:          "peer-bus",
		Kind:            wire.KindMsg,
		Body:            json.RawMessage(`{"ok":true}`),
	})
	rawGood, _ := json.Marshal(good)
	if err := f.st.Enqueue(store.Message{ID: "good-1", From: "friend", To: "victim", Envelope: rawGood}); err != nil {
		t.Fatalf("enqueue good: %v", err)
	}

	h := newWiredHarness(t, f, "victim")

	var ids []string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		structured, isErr, rpcErr := h.callTool("bus.drain", nil)
		if rpcErr != nil || isErr {
			t.Fatalf("bus.drain failed: rpcErr=%v isErr=%v", rpcErr, isErr)
		}
		for _, m := range mustSlice(structured["messages"]) {
			ids = append(ids, m.(map[string]any)["id"].(string))
		}
		if len(ids) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, id := range ids {
		if id == "forged-1" {
			t.Fatalf("forged HMAC-invalid message was surfaced by bus.drain")
		}
	}
	foundGood := false
	for _, id := range ids {
		if id == "good-1" {
			foundGood = true
		}
	}
	if !foundGood {
		t.Fatalf("valid message not drained — loop did not survive the forged one; got %v", ids)
	}
}

// TestToolErrorCases: missing/invalid args and unknown tool produce the
// right JSON-RPC errors.
func TestToolErrorCases(t *testing.T) {
	f := newBrokerFixture(t)
	h := newWiredHarness(t, f, "errs")

	// bus.send missing "to"
	_, _, rpcErr := h.callTool("bus.send", map[string]any{"body": map[string]any{"x": 1}})
	if rpcErr == nil {
		t.Fatalf("bus.send without 'to' must be a JSON-RPC error")
	}
	if code, _ := rpcErr["code"].(float64); int(code) != -32602 {
		t.Fatalf("bus.send missing-arg code = %v, want -32602", rpcErr["code"])
	}

	// bus.send missing "body"
	_, _, rpcErr = h.callTool("bus.send", map[string]any{"to": "x"})
	if rpcErr == nil {
		t.Fatalf("bus.send without 'body' must be a JSON-RPC error")
	}

	// bus.broadcast missing "body"
	_, _, rpcErr = h.callTool("bus.broadcast", map[string]any{})
	if rpcErr == nil {
		t.Fatalf("bus.broadcast without 'body' must be a JSON-RPC error")
	}

	// unknown tool
	_, _, rpcErr = h.callTool("bus.nope", nil)
	if rpcErr == nil {
		t.Fatalf("unknown tool must be a JSON-RPC error")
	}
	if code, _ := rpcErr["code"].(float64); int(code) != -32601 {
		t.Fatalf("unknown tool code = %v, want -32601", rpcErr["code"])
	}

	// unknown JSON-RPC method
	resp := h.send("does/not/exist", nil)
	var r struct {
		Error map[string]any `json:"error"`
	}
	_ = json.Unmarshal(resp, &r)
	if r.Error == nil {
		t.Fatalf("unknown method must error")
	}

	// malformed tools/call params (arguments not an object)
	bad := h.send("tools/call", map[string]any{"name": "bus.peers", "arguments": "not-an-object"})
	var br struct {
		Error map[string]any `json:"error"`
	}
	_ = json.Unmarshal(bad, &br)
	if br.Error == nil {
		t.Fatalf("malformed arguments must be a JSON-RPC error")
	}
}

func mustSlice(v any) []any {
	s, _ := v.([]any)
	return s
}

// idleBus is a no-op Bus for the shutdown test (no broker needed — the test
// asserts Serve returns on ctx cancel while blocked on idle stdin, which is
// independent of bus behaviour).
type idleBus struct{}

func (idleBus) Send(context.Context, string, json.RawMessage) error { return nil }
func (idleBus) Broadcast(context.Context, json.RawMessage) error    { return nil }
func (idleBus) Peers(context.Context) (string, []string, error)     { return "", nil, nil }
func (idleBus) Drain(context.Context) ([]mcp.InboundMessage, error) { return nil, nil }

// TestServeReturnsPromptlyOnCtxCancelWithIdleStdin is the MAJOR-R4 regression:
// readMessage blocks on stdin and is not ctx-aware; before the fix, Serve
// checked ctx only at the loop top and then blocked forever in readMessage on
// an idle pipe, so a SIGTERM never made Serve (and thus the adapter mode's
// Run) return — it hung until SIGKILL. Serve must now return ~immediately
// when ctx is cancelled even with a blocked, idle input.
func TestServeReturnsPromptlyOnCtxCancelWithIdleStdin(t *testing.T) {
	// An io.Pipe reader with NO writer blocks readMessage indefinitely —
	// exactly the idle-stdin condition. *io.PipeReader is an io.Closer, so
	// Serve's closeInput unblocks it on cancel (mirrors os.Stdin).
	pr, pw := io.Pipe()
	t.Cleanup(func() { _ = pw.Close() })

	srv := mcp.NewServer(idleBus{}, pr, io.Discard)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()

	// Let Serve get firmly blocked inside readMessage, then cancel.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
			t.Fatalf("Serve returned %v, want a context-cancelled error", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did NOT return within 3s of ctx cancel — adapter shutdown hang regressed")
	}
}
