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

func TestRunKnownAdapterClean(t *testing.T) {
	for _, mode := range []string{"generic", "cc"} {
		var out, errb bytes.Buffer
		code := run([]string{"--adapter=" + mode}, &out, &errb)
		if code != 0 {
			t.Fatalf("mode %q: exit %d, want 0 (stderr=%q)", mode, code, errb.String())
		}
		if !strings.Contains(out.String(), mode) {
			t.Fatalf("mode %q: stdout = %q, want it to mention the mode", mode, out.String())
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
