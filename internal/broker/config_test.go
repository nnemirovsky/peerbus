package broker

import (
	"strings"
	"testing"

	bhmac "github.com/nnemirovsky/peerbus/internal/hmac"
)

// validSecret is a non-secret 32-byte fixture matching the hmac minimum.
func validSecret() []byte {
	return []byte(strings.Repeat("config-test-", 4)[:bhmac.MinSecretLen])
}

// TestDefaultListenAddr documents the broker's default listen address. The
// constant is part of the operator contract (README + deploy manifest) so a
// silent change here would mis-document the deployment.
func TestDefaultListenAddr(t *testing.T) {
	if DefaultListenAddr != "127.0.0.1:47821" {
		t.Fatalf("DefaultListenAddr = %q, want 127.0.0.1:47821", DefaultListenAddr)
	}
}

// TestLoadConfig_DefaultListenAddrApplied: a Config that omits ListenAddr
// (and no PEERBUS_LISTEN env override) gets DefaultListenAddr applied. The
// rest of the required fields must be valid or LoadConfig errors before
// reaching the default-fill branch.
func TestLoadConfig_DefaultListenAddrApplied(t *testing.T) {
	// Defensively unset env that LoadConfig would honour.
	t.Setenv("PEERBUS_LISTEN", "")
	t.Setenv("PEERBUS_TOKENS", "")
	t.Setenv("PEERBUS_HMAC_SECRET", "")
	t.Setenv("PEERBUS_DB", "")

	cfg, err := LoadConfig(Config{
		Tokens:     []string{"tok"},
		HMACSecret: validSecret(),
	})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.ListenAddr != DefaultListenAddr {
		t.Fatalf("ListenAddr = %q, want %q", cfg.ListenAddr, DefaultListenAddr)
	}
}

// TestLoadConfig_EnvListenOverride: PEERBUS_LISTEN, when set, overrides
// both the struct default and the constant DefaultListenAddr (env-overrides
// -struct precedence — see LoadConfig docs).
func TestLoadConfig_EnvListenOverride(t *testing.T) {
	t.Setenv("PEERBUS_LISTEN", "0.0.0.0:9001")
	t.Setenv("PEERBUS_TOKENS", "")
	t.Setenv("PEERBUS_HMAC_SECRET", "")
	t.Setenv("PEERBUS_DB", "")

	cfg, err := LoadConfig(Config{
		ListenAddr: "127.0.0.1:1234",
		Tokens:     []string{"tok"},
		HMACSecret: validSecret(),
	})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.ListenAddr != "0.0.0.0:9001" {
		t.Fatalf("ListenAddr = %q, want env override 0.0.0.0:9001", cfg.ListenAddr)
	}
}
