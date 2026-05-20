package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestAdapterUnknownErrors(t *testing.T) {
	var out, errb bytes.Buffer
	code := dispatch([]string{"adapter", "--adapter=bogus"}, &out, &errb)
	if code == 0 {
		t.Fatalf("unknown --adapter should exit non-zero, got %d", code)
	}
	if !strings.Contains(errb.String(), "unknown adapter mode") {
		t.Fatalf("stderr = %q, want it to mention unknown adapter mode", errb.String())
	}
}

func TestAdapterMissingFlagErrors(t *testing.T) {
	var out, errb bytes.Buffer
	code := dispatch([]string{"adapter"}, &out, &errb)
	if code == 0 {
		t.Fatalf("missing --adapter should exit non-zero, got %d", code)
	}
	if !strings.Contains(errb.String(), "missing required --adapter") {
		t.Fatalf("stderr = %q, want it to mention missing --adapter", errb.String())
	}
}

// TestAdapterKnownModeRequiresEnv: a known mode resolves+constructs, but
// with no PEERBUS_URL/PEERBUS_TOKEN the binary refuses to start with a clear
// non-zero exit.
func TestAdapterKnownModeRequiresEnv(t *testing.T) {
	t.Setenv("PEERBUS_URL", "")
	t.Setenv("PEERBUS_TOKEN", "")
	for _, mode := range []string{"generic", "cc"} {
		var out, errb bytes.Buffer
		code := dispatch([]string{"adapter", "--adapter=" + mode}, &out, &errb)
		if code == 0 {
			t.Fatalf("mode %q: exit 0, want non-zero (missing PEERBUS_URL)", mode)
		}
		if !strings.Contains(errb.String(), "PEERBUS_URL is required") {
			t.Fatalf("mode %q: stderr = %q, want PEERBUS_URL requirement", mode, errb.String())
		}
	}
}

// TestAdapterRejectsShortHMACSecret is the MAJOR-R5 regression: a present
// URL+TOKEN but a missing/short PEERBUS_HMAC_SECRET must fail fast with a
// clear non-zero exit, NOT start and reconnect-spin forever.
func TestAdapterRejectsShortHMACSecret(t *testing.T) {
	t.Setenv("PEERBUS_URL", "ws://127.0.0.1:0")
	t.Setenv("PEERBUS_TOKEN", "tok")
	t.Setenv("PEERBUS_NAME", "n")
	t.Setenv("PEERBUS_HMAC_SECRET", "too-short")
	for _, mode := range []string{"generic", "cc"} {
		var out, errb bytes.Buffer
		code := dispatch([]string{"adapter", "--adapter=" + mode}, &out, &errb)
		if code == 0 {
			t.Fatalf("mode %q: exit 0, want non-zero (short HMAC secret)", mode)
		}
		if !strings.Contains(errb.String(), "PEERBUS_HMAC_SECRET must be at least") {
			t.Fatalf("mode %q: stderr = %q, want HMAC-secret requirement", mode, errb.String())
		}
	}
}

// TestAdapterGenericRejectsEmptyName is the MAJOR-R5 regression for generic
// mode: an empty PEERBUS_NAME there has no auto-name fallback and would
// reconnect-spin; it must fail fast. cc tolerates an empty name (auto-mint).
func TestAdapterGenericRejectsEmptyName(t *testing.T) {
	t.Setenv("PEERBUS_URL", "ws://127.0.0.1:0")
	t.Setenv("PEERBUS_TOKEN", "tok")
	t.Setenv("PEERBUS_NAME", "")
	t.Setenv("PEERBUS_HMAC_SECRET", strings.Repeat("x", 32))

	var out, errb bytes.Buffer
	code := dispatch([]string{"adapter", "--adapter=generic"}, &out, &errb)
	if code == 0 {
		t.Fatalf("generic with empty PEERBUS_NAME: exit 0, want non-zero")
	}
	if !strings.Contains(errb.String(), "PEERBUS_NAME is required for --adapter=generic") {
		t.Fatalf("stderr = %q, want empty-name requirement", errb.String())
	}
}
