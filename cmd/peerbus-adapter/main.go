// Command peerbus-adapter is the thin, mostly-ephemeral adapter process.
//
// Mode selection goes through the additive --adapter dispatch registry
// (internal/adapter.Resolve): a mode is looked up by name, not by a
// hard-coded switch, so future modes register without editing this file.
// Each mode (generic stdio MCP, cc claude/channel) owns its own run loop;
// this binary reads the broker connection config from the environment,
// constructs the resolved mode, and runs it until SIGINT/SIGTERM or the
// host closes stdio.
//
// Environment (matches internal/adapter.ClientConfig and the deployment
// docs):
//
//	PEERBUS_URL          broker ws:// or wss:// endpoint (required)
//	PEERBUS_NAME         unique peer name to bind (cc mode auto-generates
//	                     one when empty; generic mode requires it)
//	PEERBUS_TOKEN        static bearer token presented at register (required)
//	PEERBUS_HMAC_SECRET  shared end-to-end HMAC-SHA256 secret (required,
//	                     >= hmac.MinSecretLen bytes)
//
// --version prints the version and exits 0; a missing/unknown --adapter
// mode exits non-zero (the Task 2 contract, preserved).
package main

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
	"github.com/nnemirovsky/peerbus/internal/version"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// envClientConfig is split out so the env→ClientConfig mapping is testable
// and the variable names live in exactly one place.
func envClientConfig() adapter.ClientConfig {
	return adapter.ClientConfig{
		URL:        os.Getenv("PEERBUS_URL"),
		Name:       os.Getenv("PEERBUS_NAME"),
		Token:      os.Getenv("PEERBUS_TOKEN"),
		HMACSecret: []byte(os.Getenv("PEERBUS_HMAC_SECRET")),
	}
}

// run parses adapter flags, builds the broker client config from the
// environment, resolves + constructs the mode, and runs it until a
// termination signal or the host closing stdio. It returns a process exit
// code. Split out of main so the behaviour is testable.
func run(args []string, stdout, stderr io.Writer) int {
	known := strings.Join(adapter.Modes(), ", ")

	fs := flag.NewFlagSet("peerbus-adapter", flag.ContinueOnError)
	fs.SetOutput(stderr)
	mode := fs.String("adapter", "", "adapter mode ("+strings.ReplaceAll(known, ", ", "|")+")")
	showVersion := fs.Bool("version", false, "print version and exit")
	fs.Usage = func() {
		fmt.Fprintf(stderr, "usage: peerbus-adapter --adapter=<mode>\n\n")
		fmt.Fprintf(stderr, "modes are resolved via the additive --adapter dispatch registry\n\n")
		fmt.Fprintf(stderr, "broker connection is read from the environment:\n")
		fmt.Fprintf(stderr, "  PEERBUS_URL PEERBUS_NAME PEERBUS_TOKEN PEERBUS_HMAC_SECRET\n\n")
		fmt.Fprintf(stderr, "flags:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Fprintln(stdout, version.String())
		return 0
	}

	if *mode == "" {
		fmt.Fprintf(stderr, "peerbus-adapter: missing required --adapter=<mode> (one of: %s)\n", known)
		return 2
	}
	ctor, err := adapter.Resolve(*mode)
	if err != nil {
		fmt.Fprintf(stderr, "peerbus-adapter: unknown adapter mode %q (one of: %s)\n", *mode, known)
		return 2
	}

	cfg := envClientConfig()
	if cfg.URL == "" {
		fmt.Fprintln(stderr, "peerbus-adapter: PEERBUS_URL is required")
		return 2
	}
	if cfg.Token == "" {
		fmt.Fprintln(stderr, "peerbus-adapter: PEERBUS_TOKEN is required")
		return 2
	}

	m, err := ctor(cfg, 0) // 0 => DefaultDedupeSize
	if err != nil {
		fmt.Fprintf(stderr, "peerbus-adapter: construct mode %q: %v\n", *mode, err)
		return 1
	}

	// Bind the run lifecycle to SIGINT/SIGTERM. The mode's own Run also
	// returns when the host closes stdio (the cc/generic anti-orphan
	// property); either path cancels the context and Run returns.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := m.Run(ctx); err != nil && ctx.Err() == nil {
		fmt.Fprintf(stderr, "peerbus-adapter: mode %q exited: %v\n", *mode, err)
		return 1
	}
	return 0
}
