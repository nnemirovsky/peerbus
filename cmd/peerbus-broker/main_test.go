package main

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/nnemirovsky/peerbus/internal/broker"
	bhmac "github.com/nnemirovsky/peerbus/internal/hmac"
	"github.com/nnemirovsky/peerbus/internal/store"
	"github.com/nnemirovsky/peerbus/internal/wire"
)

// TestRunVersionClean: --version prints the version and exits 0 (mirrors
// cmd/peerbus-adapter/main_test.go).
func TestRunVersionClean(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"--version"}, &out, &errb); code != 0 {
		t.Fatalf("--version exit %d, want 0 (stderr=%q)", code, errb.String())
	}
	if !strings.Contains(out.String(), "peerbus ") {
		t.Fatalf("stdout = %q, want version string", out.String())
	}
}

// TestRunMissingSubcommandErrors: no subcommand exits non-zero with guidance.
func TestRunMissingSubcommandErrors(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run(nil, &out, &errb); code == 0 {
		t.Fatalf("missing subcommand should exit non-zero, got 0")
	}
	if !strings.Contains(errb.String(), "a subcommand is required") {
		t.Fatalf("stderr = %q, want it to mention a required subcommand", errb.String())
	}
}

// TestRunUnknownSubcommandErrors: an unknown subcommand exits non-zero.
func TestRunUnknownSubcommandErrors(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"bogus"}, &out, &errb); code == 0 {
		t.Fatalf("unknown subcommand should exit non-zero, got 0")
	}
	if !strings.Contains(errb.String(), "unknown subcommand") {
		t.Fatalf("stderr = %q, want unknown-subcommand message", errb.String())
	}
}

// TestRunServeRejectsMissingToken: the env→broker.Config path fails fast when
// no bearer token is configured (broker refuses to start). Exercises the
// runServe config-error branch.
func TestRunServeRejectsMissingToken(t *testing.T) {
	t.Setenv(broker.EnvTokens, "")
	t.Setenv(broker.EnvHMACSecret, strings.Repeat("x", bhmac.MinSecretLen))
	t.Setenv(broker.EnvDBPath, ":memory:")

	var out, errb bytes.Buffer
	if code := run([]string{"serve"}, &out, &errb); code != 2 {
		t.Fatalf("serve with no token exit %d, want 2 (stderr=%q)", code, errb.String())
	}
	if !strings.Contains(errb.String(), "bearer token is required") {
		t.Fatalf("stderr = %q, want missing-token message", errb.String())
	}
}

// TestRunServeRejectsShortHMACSecret: a short/missing HMAC secret is rejected
// (the broker will not start with a weak end-to-end key).
func TestRunServeRejectsShortHMACSecret(t *testing.T) {
	t.Setenv(broker.EnvTokens, "tok")
	t.Setenv(broker.EnvHMACSecret, "too-short")
	t.Setenv(broker.EnvDBPath, ":memory:")

	var out, errb bytes.Buffer
	if code := run([]string{"serve"}, &out, &errb); code != 2 {
		t.Fatalf("serve with short HMAC secret exit %d, want 2 (stderr=%q)", code, errb.String())
	}
	if !strings.Contains(errb.String(), "secret must be at least") {
		t.Fatalf("stderr = %q, want short-secret message", errb.String())
	}
}

// TestRunAuditVerifyUsage: `audit` without `verify` is a usage error;
// `audit verify` on a fresh store reports an intact chain and exits 0.
func TestRunAuditVerify(t *testing.T) {
	dir := t.TempDir()
	db := dir + "/peerbus.db"

	var out, errb bytes.Buffer
	if code := run([]string{"--db", db, "audit"}, &out, &errb); code != 2 {
		t.Fatalf("`audit` without verb exit %d, want 2", code)
	}

	out.Reset()
	errb.Reset()
	if code := run([]string{"--db", db, "audit", "verify"}, &out, &errb); code != 0 {
		t.Fatalf("`audit verify` on a fresh store exit %d, want 0 (stderr=%q)", code, errb.String())
	}
	if !strings.Contains(out.String(), "audit chain OK") {
		t.Fatalf("stdout = %q, want 'audit chain OK'", out.String())
	}
}

// TestAssembledHandlerAcceptsWSRegister is the smoke test: the broker handler
// assembled exactly as runServe wires it (broker.NewServer over an
// authenticator + registry + store) accepts a WebSocket register handshake
// and replies with a peers ack. This guards the cmd→broker wiring contract
// (the binary's only job for `serve` is to construct this handler and run it).
func TestAssembledHandlerAcceptsWSRegister(t *testing.T) {
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	// Same construction as runServe (sans the signal/ListenAndServe loop).
	srv := broker.NewServer(
		broker.NewAuthenticator([]string{"tok"}),
		broker.NewRegistry(),
		st,
		nil,
	)
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")
	c, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer func() { _ = c.Close(websocket.StatusNormalClosure, "") }()

	if err := wsjson.Write(ctx, c, wire.Register{
		ProtocolVersion: wire.ProtocolVersion,
		Type:            wire.ControlRegister,
		Token:           "tok",
		Name:            "smoke",
	}); err != nil {
		t.Fatalf("write register: %v", err)
	}

	var ack wire.Peers
	if err := wsjson.Read(ctx, c, &ack); err != nil {
		t.Fatalf("read ack: %v", err)
	}
	if ack.Type != wire.ControlPeers {
		t.Fatalf("handshake ack type = %q, want peers", ack.Type)
	}
}
