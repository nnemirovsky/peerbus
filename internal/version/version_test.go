package version

import (
	"strings"
	"testing"
)

func TestVersionNotEmpty(t *testing.T) {
	if Version == "" {
		t.Fatal("Version must not be empty")
	}
}

func TestStringContainsVersion(t *testing.T) {
	s := String()
	if !strings.HasPrefix(s, "peerbus ") {
		t.Fatalf("String() = %q, want prefix %q", s, "peerbus ")
	}
	if !strings.Contains(s, Version) {
		t.Fatalf("String() = %q, want it to contain Version %q", s, Version)
	}
}
