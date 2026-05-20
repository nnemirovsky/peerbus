package main

// `adapter` subcommand. Ported verbatim from the v0.1.0
// cmd/peerbus-adapter/main.go — same --adapter dispatch, same env vars, same
// fail-fast guards (missing URL/Token, short HMAC, empty generic name).

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/nnemirovsky/peerbus/internal/adapter"
	"github.com/nnemirovsky/peerbus/internal/hmac"
	"github.com/nnemirovsky/peerbus/internal/version"
)

// envClientConfig is split out so the env→ClientConfig mapping is testable
// and the variable names live in exactly one place. Matches v0.1.0
// peerbus-adapter behaviour verbatim.
func envClientConfig() adapter.ClientConfig {
	return adapter.ClientConfig{
		URL:        os.Getenv("PEERBUS_URL"),
		Name:       os.Getenv("PEERBUS_NAME"),
		Token:      os.Getenv("PEERBUS_TOKEN"),
		HMACSecret: []byte(os.Getenv("PEERBUS_HMAC_SECRET")),
	}
}

// adapterRun parses adapter flags, builds the broker client config from the
// environment, resolves + constructs the mode, and runs it until a
// termination signal or the host closes stdio. Returns a process exit code.
// Behaviour mirrors v0.1.0 `peerbus-adapter --adapter=<mode>` exactly:
//
//   - missing or unknown --adapter           → exit 2
//   - missing PEERBUS_URL or PEERBUS_TOKEN   → exit 2
//   - short/missing PEERBUS_HMAC_SECRET      → exit 2
//   - --adapter=generic with empty NAME      → exit 2
//   - mode construct error                   → exit 1
//   - mode Run error (ctx not canceled)      → exit 1
//   - clean shutdown                         → exit 0
func adapterRun(args []string, stdout, stderr io.Writer) int {
	known := strings.Join(adapter.Modes(), ", ")

	fs := flag.NewFlagSet("peerbus adapter", flag.ContinueOnError)
	fs.SetOutput(stderr)
	mode := fs.String("adapter", "", "adapter mode ("+strings.ReplaceAll(known, ", ", "|")+")")
	showVersion := fs.Bool("version", false, "print version and exit")
	fs.Usage = func() {
		_, _ = fmt.Fprintf(stderr, "usage: peerbus adapter --adapter=<mode>\n\n")
		_, _ = fmt.Fprintf(stderr, "modes are resolved via the additive --adapter dispatch registry\n\n")
		_, _ = fmt.Fprintf(stderr, "broker connection is read from the environment:\n")
		_, _ = fmt.Fprintf(stderr, "  PEERBUS_URL PEERBUS_NAME PEERBUS_TOKEN PEERBUS_HMAC_SECRET\n\n")
		_, _ = fmt.Fprintf(stderr, "flags:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		_, _ = fmt.Fprintln(stdout, version.String())
		return 0
	}

	if *mode == "" {
		_, _ = fmt.Fprintf(stderr, "peerbus: missing required --adapter=<mode> (one of: %s)\n", known)
		return 2
	}
	ctor, err := adapter.Resolve(*mode)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "peerbus: unknown adapter mode %q (one of: %s)\n", *mode, known)
		return 2
	}

	cfg := envClientConfig()
	if cfg.URL == "" {
		_, _ = fmt.Fprintln(stderr, "peerbus: PEERBUS_URL is required")
		return 2
	}
	if cfg.Token == "" {
		_, _ = fmt.Fprintln(stderr, "peerbus: PEERBUS_TOKEN is required")
		return 2
	}
	// Fail fast on a missing/short HMAC secret. Without this the client
	// treats hmac.ErrShortSecret as a transient error and reconnect-spins
	// forever (no progress, no signal to the operator). Bound matches the
	// broker's enforcement (hmac.MinSecretLen).
	if len(cfg.HMACSecret) < hmac.MinSecretLen {
		_, _ = fmt.Fprintf(stderr, "peerbus: PEERBUS_HMAC_SECRET must be at least %d bytes\n",
			hmac.MinSecretLen)
		return 2
	}
	// Generic mode binds a fixed peer name and has no auto-name fallback
	// (only cc auto-generates one). An empty PEERBUS_NAME there is rejected
	// at broker register on every attempt → reconnect spin; fail fast
	// instead. cc tolerates an empty name (mints cc-<host>-<pid>-<rand>).
	if *mode == "generic" && cfg.Name == "" {
		_, _ = fmt.Fprintln(stderr, "peerbus: PEERBUS_NAME is required for --adapter=generic")
		return 2
	}

	m, err := ctor(cfg, 0) // 0 => DefaultDedupeSize
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "peerbus: construct mode %q: %v\n", *mode, err)
		return 1
	}

	// Bind the run lifecycle to SIGINT/SIGTERM. The mode's own Run also
	// returns when the host closes stdio (the cc/generic anti-orphan
	// property); either path cancels the context and Run returns.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := m.Run(ctx); err != nil && ctx.Err() == nil {
		_, _ = fmt.Fprintf(stderr, "peerbus: mode %q exited: %v\n", *mode, err)
		return 1
	}
	return 0
}
