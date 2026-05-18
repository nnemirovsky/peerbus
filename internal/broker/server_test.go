package broker

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	bhmac "github.com/nnemirovsky/peerbus/internal/hmac"
	"github.com/nnemirovsky/peerbus/internal/store"
	"github.com/nnemirovsky/peerbus/internal/wire"
)

// testSecret is a deliberately non-secret 32-byte fixture (the repeated word
// "peerbus-test-" padded/truncated to hmac.MinSecretLen). Built at runtime so
// no high-entropy literal lands in the source tree / secret scanners.
var testSecret = strings.Repeat("peerbus-test-", 4)[:bhmac.MinSecretLen]

// newTestServer spins an in-process broker over httptest with an in-memory
// store and the given accepted tokens. It returns the ws:// URL and store.
func newTestServer(t *testing.T, tokens ...string) (string, *store.Store) {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	srv := NewServer(NewAuthenticator(tokens), NewRegistry(), st, nil)
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	return "ws" + strings.TrimPrefix(hs.URL, "http"), st
}

// dial opens a raw WS connection to the broker.
func dial(t *testing.T, url string) (*websocket.Conn, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(websocket.StatusNormalClosure, "") })
	return c, ctx
}

func sendJSON(t *testing.T, ctx context.Context, c *websocket.Conn, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := c.Write(ctx, websocket.MessageText, b); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func readJSON(t *testing.T, ctx context.Context, c *websocket.Conn, v any) {
	t.Helper()
	_, b, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("unmarshal %q: %v", b, err)
	}
}

func regFrame(name, token string) wire.Register {
	return wire.Register{
		ProtocolVersion: wire.ProtocolVersion,
		Type:            wire.ControlRegister,
		Token:           token,
		Name:            name,
	}
}

func TestServer_ValidRegister(t *testing.T) {
	url, _ := newTestServer(t, "tok")
	c, ctx := dial(t, url)

	sendJSON(t, ctx, c, regFrame("alice", "tok"))

	var ack wire.Peers
	readJSON(t, ctx, c, &ack)
	if ack.Type != wire.ControlPeers {
		t.Fatalf("ack type = %q, want peers", ack.Type)
	}
	found := false
	for _, n := range ack.Names {
		if n == "alice" {
			found = true
		}
	}
	if !found {
		t.Fatalf("registered peer alice not in ack peer list %v", ack.Names)
	}
}

func TestServer_BadTokenRejected(t *testing.T) {
	url, _ := newTestServer(t, "good-token")
	c, ctx := dial(t, url)

	sendJSON(t, ctx, c, regFrame("alice", "WRONG"))

	_, _, err := c.Read(ctx)
	if err == nil {
		t.Fatalf("expected connection closed on bad token, got no error")
	}
	if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatalf("close status = %v, want PolicyViolation", websocket.CloseStatus(err))
	}
}

func TestServer_MalformedHandshake(t *testing.T) {
	url, _ := newTestServer(t, "tok")
	c, ctx := dial(t, url)

	if err := c.Write(ctx, websocket.MessageText, []byte("{not json")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, err := c.Read(ctx); err == nil {
		t.Fatalf("expected close on malformed handshake")
	}
}

func TestServer_NonRegisterFirstFrameRejected(t *testing.T) {
	url, _ := newTestServer(t, "tok")
	c, ctx := dial(t, url)

	sendJSON(t, ctx, c, wire.Ack{
		ProtocolVersion: wire.ProtocolVersion,
		Type:            wire.ControlAck,
		ID:              "x",
	})
	if _, _, err := c.Read(ctx); err == nil {
		t.Fatalf("expected close when first frame is not register")
	}
}

func TestServer_VersionMismatchRejected(t *testing.T) {
	url, _ := newTestServer(t, "tok")
	c, ctx := dial(t, url)

	bad := regFrame("alice", "tok")
	bad.ProtocolVersion = "v999"
	sendJSON(t, ctx, c, bad)

	_, _, err := c.Read(ctx)
	if err == nil {
		t.Fatalf("expected close on version mismatch")
	}
	if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatalf("close status = %v, want PolicyViolation", websocket.CloseStatus(err))
	}
}

func TestServer_DuplicateNameSameTokenTakeover(t *testing.T) {
	url, _ := newTestServer(t, "tok")

	c1, ctx1 := dial(t, url)
	sendJSON(t, ctx1, c1, regFrame("alice", "tok"))
	var ack1 wire.Peers
	readJSON(t, ctx1, c1, &ack1)

	// Second connection claims the same name under the SAME token.
	c2, ctx2 := dial(t, url)
	sendJSON(t, ctx2, c2, regFrame("alice", "tok"))
	var ack2 wire.Peers
	readJSON(t, ctx2, c2, &ack2)

	// The OLD connection must be torn down by the takeover (the displaced
	// client observes an abnormal closure — see Server.CloseTakenOver).
	if _, _, err := c1.Read(ctx1); err == nil {
		t.Fatalf("old connection not closed on same-token takeover")
	}
}

func TestServer_DuplicateNameDifferentTokenReject(t *testing.T) {
	url, _ := newTestServer(t, "tok-A", "tok-B")

	c1, ctx1 := dial(t, url)
	sendJSON(t, ctx1, c1, regFrame("alice", "tok-A"))
	var ack1 wire.Peers
	readJSON(t, ctx1, c1, &ack1)

	// Different token claims the same name => reject; first conn untouched.
	c2, ctx2 := dial(t, url)
	sendJSON(t, ctx2, c2, regFrame("alice", "tok-B"))
	if _, _, err := c2.Read(ctx2); err == nil {
		t.Fatalf("expected reject for different-token name claim")
	}

	// First connection must be untouched by the rejected claim: a short
	// read sees no frame and times out (the connection is still open and
	// the broker did NOT close it), rather than returning a close error.
	rctx, rcancel := context.WithTimeout(ctx1, 300*time.Millisecond)
	defer rcancel()
	_, _, err := c1.Read(rctx)
	if websocket.CloseStatus(err) != -1 {
		t.Fatalf("first connection was closed by a different-token reject: %v", err)
	}
}

// TestServer_TakeoverRaceMessageNotLost asserts the locked guarantee: a
// message queued to a name while a same-token takeover is happening is NOT
// lost — it falls to the offline/pending store path and is delivered to the
// NEW connection on (re)register.
func TestServer_TakeoverRaceMessageNotLost(t *testing.T) {
	url, st := newTestServer(t, "tok")

	// alice connects.
	c1, ctx1 := dial(t, url)
	sendJSON(t, ctx1, c1, regFrame("alice", "tok"))
	var ack1 wire.Peers
	readJSON(t, ctx1, c1, &ack1)

	// A message is enqueued for alice (simulating a send that lands while
	// the connection is being displaced — it persists durably regardless
	// of which conn is live).
	env := wire.Envelope{
		ProtocolVersion: wire.ProtocolVersion,
		ID:              "msg-1",
		From:            "bob",
		To:              "alice",
		TS:              "t",
		Source:          "peer-bus",
		Kind:            wire.KindMsg,
		Body:            json.RawMessage(`{"hi":1}`),
	}
	signed, err := bhmac.SignEnvelope([]byte(testSecret), env)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	raw, _ := json.Marshal(signed)
	if err := st.Enqueue(store.Message{
		ID: signed.ID, From: signed.From, To: signed.To, Envelope: raw,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// alice reconnects under the same token (same-token takeover). The new
	// connection must receive the pending message via the flush path.
	c2, ctx2 := dial(t, url)
	sendJSON(t, ctx2, c2, regFrame("alice", "tok"))

	var ack2 wire.Peers
	readJSON(t, ctx2, c2, &ack2) // handshake ack

	var del wire.Deliver
	readJSON(t, ctx2, c2, &del)
	if del.Type != wire.ControlDeliver || del.Envelope.ID != "msg-1" {
		t.Fatalf("new conn did not receive the pending takeover-race message, got %+v", del)
	}
	// Recipient can verify it end-to-end (HMAC survived the store round-trip).
	if err := bhmac.VerifyEnvelope([]byte(testSecret), del.Envelope); err != nil {
		t.Fatalf("delivered envelope failed HMAC verify: %v", err)
	}
}

func TestLoadConfig_EnvOverridesAndValidation(t *testing.T) {
	// Missing token => error.
	if _, err := LoadConfig(Config{HMACSecret: []byte(testSecret)}); err == nil {
		t.Fatalf("expected error when no tokens configured")
	}
	// Short HMAC secret => error.
	if _, err := LoadConfig(Config{Tokens: []string{"t"}, HMACSecret: []byte("short")}); err == nil {
		t.Fatalf("expected error for short HMAC secret")
	}

	// Env overrides struct fields.
	t.Setenv(EnvListenAddr, "1.2.3.4:9999")
	t.Setenv(EnvTokens, "a, b ,c")
	t.Setenv(EnvHMACSecret, testSecret)
	t.Setenv(EnvDBPath, "/tmp/x.db")
	cfg, err := LoadConfig(Config{ListenAddr: "ignored:1", Tokens: []string{"old"}})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.ListenAddr != "1.2.3.4:9999" {
		t.Fatalf("ListenAddr = %q, env did not override", cfg.ListenAddr)
	}
	if strings.Join(cfg.Tokens, ",") != "a,b,c" {
		t.Fatalf("Tokens = %v, want [a b c] (trimmed)", cfg.Tokens)
	}
	if cfg.DBPath != "/tmp/x.db" {
		t.Fatalf("DBPath = %q, env did not override", cfg.DBPath)
	}
}

func TestAuthenticator_Valid(t *testing.T) {
	a := NewAuthenticator([]string{"alpha", "beta"})
	if !a.Valid("alpha") || !a.Valid("beta") {
		t.Fatalf("configured tokens must be valid")
	}
	if a.Valid("") || a.Valid("gamma") {
		t.Fatalf("unknown/empty token must be invalid")
	}
}
