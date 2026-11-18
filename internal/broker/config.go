package broker

import (
	"fmt"
	"os"
	"strings"
)

// Config holds the broker's runtime configuration.
//
// Configuration sources and precedence (documented, load-bearing):
//
//  1. A Config struct supplied programmatically (defaults / embedding host).
//  2. Environment variables, which OVERRIDE any non-empty struct field.
//
// Precedence is "env overrides struct": LoadConfig takes a base Config (which
// may be the zero value) and, for every recognised PEERBUS_* variable that is
// set and non-empty, replaces the corresponding field. This lets a deployment
// ship sane defaults in code while letting the operator override any of them
// from the environment without a rebuild.
type Config struct {
	// ListenAddr is the TCP address the WS server binds (host:port).
	ListenAddr string
	// Tokens is the set of accepted static bearer tokens. A peer name is
	// bindable only under one of these.
	Tokens []string
	// DBPath is the durable SQLite store path (":memory:" for ephemeral).
	DBPath string
}

// Environment variable names. PEERBUS_TOKENS is a comma-separated list.
const (
	EnvListenAddr = "PEERBUS_LISTEN"
	EnvTokens     = "PEERBUS_TOKENS"
	// EnvHMACSecret is the adapters' end-to-end signing secret. The broker
	// never reads it: messages are signed and verified only by the peers, so
	// a broker host that does not hold the secret cannot forge one.
	EnvHMACSecret = "PEERBUS_HMAC_SECRET"
	EnvDBPath     = "PEERBUS_DB"
)

// DefaultListenAddr is used when neither the struct nor the environment sets
// a listen address. 47821 was chosen because it is outside the IANA
// well-known/registered range hot-spots (8080 is the prototypical "I'm
// already running a tutorial here" port), is absent from macOS
// /etc/services, and sits well below the OS ephemeral range so a bound
// listener is unlikely to race a randomly-assigned outbound socket. Bound
// to loopback by default — operators wanting cross-host access set
// PEERBUS_LISTEN=0.0.0.0:47821 explicitly.
const DefaultListenAddr = "127.0.0.1:47821"

// parseTokens splits a comma-separated token list, trimming whitespace and
// dropping empties.
func parseTokens(s string) []string {
	var out []string
	for _, t := range strings.Split(s, ",") {
		if t = strings.TrimSpace(t); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// LoadConfig returns the effective configuration: it starts from base and lets
// any set, non-empty PEERBUS_* environment variable override the matching
// field (env-overrides-struct precedence — see Config). At least one bearer
// token is required. The end-to-end HMAC secret is not part of the broker's
// configuration: only the adapters sign and verify envelopes.
func LoadConfig(base Config) (Config, error) {
	cfg := base

	if v := os.Getenv(EnvListenAddr); v != "" {
		cfg.ListenAddr = v
	}
	if v := os.Getenv(EnvTokens); v != "" {
		cfg.Tokens = parseTokens(v)
	}
	if v := os.Getenv(EnvDBPath); v != "" {
		cfg.DBPath = v
	}

	if cfg.ListenAddr == "" {
		cfg.ListenAddr = DefaultListenAddr
	}
	if cfg.DBPath == "" {
		cfg.DBPath = "peerbus.db"
	}
	if len(cfg.Tokens) == 0 {
		return Config{}, fmt.Errorf("broker config: at least one bearer token is required")
	}
	return cfg, nil
}
