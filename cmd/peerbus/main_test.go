package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestTopLevelVersion: `peerbus --version` prints the version and exits 0,
// WITHOUT requiring a subcommand. Matches the v0.1.0 behaviour of both old
// mains (both supported `--version` as a top-level flag).
func TestTopLevelVersion(t *testing.T) {
	var out, errb bytes.Buffer
	if code := dispatch([]string{"--version"}, &out, &errb); code != 0 {
		t.Fatalf("--version exit %d, want 0 (stderr=%q)", code, errb.String())
	}
	if !strings.Contains(out.String(), "peerbus ") {
		t.Fatalf("stdout = %q, want version string", out.String())
	}
}

// TestNoArgsExits2: `peerbus` with no args prints usage to stderr and exits
// 2 (unknown-subcommand class of failure).
func TestNoArgsExits2(t *testing.T) {
	var out, errb bytes.Buffer
	if code := dispatch(nil, &out, &errb); code != 2 {
		t.Fatalf("no args exit %d, want 2 (stderr=%q)", code, errb.String())
	}
	if !strings.Contains(errb.String(), "a subcommand is required") {
		t.Fatalf("stderr = %q, want it to mention a required subcommand", errb.String())
	}
	if !strings.Contains(errb.String(), "usage: peerbus") {
		t.Fatalf("stderr = %q, want a usage block", errb.String())
	}
}

// TestHelpExits0: `peerbus help` prints usage to stdout and exits 0. Same
// for `-h` and `--help`.
func TestHelpExits0(t *testing.T) {
	for _, arg := range []string{"help", "-h", "--help"} {
		var out, errb bytes.Buffer
		if code := dispatch([]string{arg}, &out, &errb); code != 0 {
			t.Fatalf("%s exit %d, want 0 (stderr=%q)", arg, code, errb.String())
		}
		if !strings.Contains(out.String(), "usage: peerbus") {
			t.Fatalf("%s stdout = %q, want a usage block", arg, out.String())
		}
	}
}

// TestUnknownCommandExits2: an unknown top-level subcommand prints the
// "unknown command" line and exits 2.
func TestUnknownCommandExits2(t *testing.T) {
	var out, errb bytes.Buffer
	if code := dispatch([]string{"bogus"}, &out, &errb); code != 2 {
		t.Fatalf("unknown command exit %d, want 2 (stderr=%q)", code, errb.String())
	}
	if !strings.Contains(errb.String(), "unknown command") {
		t.Fatalf("stderr = %q, want unknown-command message", errb.String())
	}
}
