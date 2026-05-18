package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunUnknownAdapterErrors(t *testing.T) {
	var out, errb bytes.Buffer
	code := run([]string{"--adapter=bogus"}, &out, &errb)
	if code == 0 {
		t.Fatalf("unknown --adapter should exit non-zero, got %d", code)
	}
	if !strings.Contains(errb.String(), "unknown adapter mode") {
		t.Fatalf("stderr = %q, want it to mention unknown adapter mode", errb.String())
	}
}

func TestRunMissingAdapterErrors(t *testing.T) {
	var out, errb bytes.Buffer
	code := run(nil, &out, &errb)
	if code == 0 {
		t.Fatalf("missing --adapter should exit non-zero, got %d", code)
	}
	if !strings.Contains(errb.String(), "missing required --adapter") {
		t.Fatalf("stderr = %q, want it to mention missing --adapter", errb.String())
	}
}

// TestRunKnownAdapterRequiresEnv: a known mode resolves+constructs, but with
// no PEERBUS_URL/PEERBUS_TOKEN the binary refuses to start with a clear
// non-zero exit (it no longer prints a skeleton line and exits 0 — it wires
// the real run path).
func TestRunKnownAdapterRequiresEnv(t *testing.T) {
	t.Setenv("PEERBUS_URL", "")
	t.Setenv("PEERBUS_TOKEN", "")
	for _, mode := range []string{"generic", "cc"} {
		var out, errb bytes.Buffer
		code := run([]string{"--adapter=" + mode}, &out, &errb)
		if code == 0 {
			t.Fatalf("mode %q: exit 0, want non-zero (missing PEERBUS_URL)", mode)
		}
		if !strings.Contains(errb.String(), "PEERBUS_URL is required") {
			t.Fatalf("mode %q: stderr = %q, want PEERBUS_URL requirement", mode, errb.String())
		}
	}
}

func TestRunVersionClean(t *testing.T) {
	var out, errb bytes.Buffer
	code := run([]string{"--version"}, &out, &errb)
	if code != 0 {
		t.Fatalf("--version exit %d, want 0", code)
	}
	if !strings.Contains(out.String(), "peerbus ") {
		t.Fatalf("stdout = %q, want version string", out.String())
	}
}
