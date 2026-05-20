package adapter

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nnemirovsky/peerbus/internal/broker"
	"github.com/nnemirovsky/peerbus/internal/store"
)

// TestClientConnect_NameClaimedSurfacesTyped: a fresh Client connecting
// under a name that is already bound under a DIFFERENT bearer token
// receives the broker's "name claimed under a different token" policy
// violation and Client.Connect surfaces it as ErrNameClaimed (not as the
// generic "register rejected or no ack" wrapper). This is the typed
// signal the cc adapter's collision-retry loop keys off when it rotates
// to a fresh friendly name.
func TestClientConnect_NameClaimedSurfacesTyped(t *testing.T) {
	f2 := newBrokerFixtureWithTokens(t, []string{"tok", "tok2"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Holder binds "shared-name" under token "tok".
	holder := NewClient(ClientConfig{
		URL: f2.wsURL, Token: "tok", Name: "shared-name", HMACSecret: testSecret,
	})
	if err := holder.Connect(ctx); err != nil {
		t.Fatalf("holder connect: %v", err)
	}
	defer holder.Close()

	// Challenger tries the same name under DIFFERENT token "tok2".
	challenger := NewClient(ClientConfig{
		URL: f2.wsURL, Token: "tok2", Name: "shared-name", HMACSecret: testSecret,
	})
	err := challenger.Connect(ctx)
	if err == nil {
		challenger.Close()
		t.Fatalf("challenger connected; want ErrNameClaimed")
	}
	if !errors.Is(err, ErrNameClaimed) {
		t.Fatalf("challenger connect err = %v, want errors.Is(_, ErrNameClaimed)", err)
	}
}

// TestResumingClient_NameClaimedShortCircuits: ResumingClient.connect (the
// reconnect/resume entry point) propagates ErrNameClaimed up instead of
// retrying with backoff. A permanent rejection MUST NOT spin the redial
// loop forever — the embedding mode rotates the name and starts a fresh
// resuming client.
func TestResumingClient_NameClaimedShortCircuits(t *testing.T) {
	f := newBrokerFixtureWithTokens(t, []string{"tok", "tok2"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	holder := NewClient(ClientConfig{
		URL: f.wsURL, Token: "tok", Name: "claimed", HMACSecret: testSecret,
	})
	if err := holder.Connect(ctx); err != nil {
		t.Fatalf("holder connect: %v", err)
	}
	defer holder.Close()

	rc := NewResumingClient(ClientConfig{
		URL: f.wsURL, Token: "tok2", Name: "claimed", HMACSecret: testSecret,
	}, 16)
	start := time.Now()
	_, err := rc.connect(ctx)
	if err == nil {
		t.Fatalf("connect succeeded; want ErrNameClaimed")
	}
	if !errors.Is(err, ErrNameClaimed) {
		t.Fatalf("connect err = %v, want errors.Is(_, ErrNameClaimed)", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("connect took %v; expected near-immediate short-circuit", elapsed)
	}
}

// TestResumingClient_NameAccessor: ResumingClient.Name() returns the
// configured name (the value used in register and the value bus.peers
// reports as `self`).
func TestResumingClient_NameAccessor(t *testing.T) {
	rc := NewResumingClient(ClientConfig{Name: "my-name"}, 0)
	if got := rc.Name(); got != "my-name" {
		t.Fatalf("Name() = %q, want my-name", got)
	}
}

// TestFilterSelf: filterSelf removes self from a name slice without
// mutating the input (used by bus.Peers implementations to suppress the
// caller from its own peer list).
func TestFilterSelf(t *testing.T) {
	in := []string{"alpha", "self", "beta", "self"}
	out := filterSelf(in, "self")
	if len(out) != 2 || out[0] != "alpha" || out[1] != "beta" {
		t.Fatalf("filterSelf = %v, want [alpha beta]", out)
	}
	if len(in) != 4 || in[1] != "self" {
		t.Fatalf("filterSelf mutated input: %v", in)
	}
	// Empty self => identity.
	if got := filterSelf([]string{"x", "y"}, ""); len(got) != 2 || got[0] != "x" {
		t.Fatalf("filterSelf with empty self = %v, want [x y]", got)
	}
}

// newBrokerFixtureWithTokens is a variant of newBrokerFixture (defined in
// client_test.go) that accepts a custom token list so a test can bind two
// peers under DIFFERENT tokens against the same broker — required for
// driving the name-claimed-under-different-token rejection path.
func newBrokerFixtureWithTokens(t *testing.T, tokens []string) *brokerFixture {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := broker.NewServer(broker.NewAuthenticator(tokens), broker.NewRegistry(), st, nil)
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
