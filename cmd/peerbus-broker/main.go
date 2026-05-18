// Command peerbus-broker is the long-lived agent-agnostic message broker.
//
// Subcommands:
//
//	serve         start the WebSocket broker (Task 7 core: token auth + peer
//	              registry; routing lands in Task 8)
//	audit verify  walk the blake3 hash-chain audit log and report any break
//
// Configuration for `serve` is loaded via internal/broker.LoadConfig
// (env-overrides-struct precedence; see that package).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nnemirovsky/peerbus/internal/audit"
	"github.com/nnemirovsky/peerbus/internal/broker"
	"github.com/nnemirovsky/peerbus/internal/store"
	"github.com/nnemirovsky/peerbus/internal/version"
)

// defaultDBPath is the store location used when --db is not given.
const defaultDBPath = "peerbus.db"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run parses broker flags/subcommands and returns a process exit code. It is
// split out of main so it is testable.
func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("peerbus-broker", flag.ContinueOnError)
	fs.SetOutput(stderr)
	showVersion := fs.Bool("version", false, "print version and exit")
	dbPath := fs.String("db", defaultDBPath, "path to the durable SQLite store")
	fs.Usage = func() {
		fmt.Fprintf(stderr, "usage: peerbus-broker [flags] [subcommand]\n\n")
		fmt.Fprintf(stderr, "subcommands:\n")
		fmt.Fprintf(stderr, "  serve           start the WebSocket broker (token auth + peer registry)\n")
		fmt.Fprintf(stderr, "  audit verify    walk the blake3 audit hash-chain and report any break\n\n")
		fmt.Fprintf(stderr, "serve config is loaded from PEERBUS_* env (LISTEN, TOKENS, HMAC_SECRET, DB)\n\n")
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

	rest := fs.Args()
	if len(rest) == 0 {
		fmt.Fprintln(stderr, "peerbus-broker: a subcommand is required (serve | audit verify)")
		fs.Usage()
		return 2
	}

	switch rest[0] {
	case "serve":
		return runServe(*dbPath, stdout, stderr)
	case "audit":
		return runAudit(rest[1:], *dbPath, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "peerbus-broker: unknown subcommand %q\n", rest[0])
		return 2
	}
}

// runAudit handles the `audit ...` subcommand tree. Only `audit verify` is
// implemented; an exit code of 0 means the chain is intact, 1 means a break
// was found, 2 means a usage/operational error.
func runAudit(args []string, dbPath string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "verify" {
		fmt.Fprintln(stderr, "usage: peerbus-broker [--db PATH] audit verify")
		return 2
	}

	st, err := store.Open(dbPath)
	if err != nil {
		fmt.Fprintf(stderr, "peerbus-broker: open store %q: %v\n", dbPath, err)
		return 2
	}
	defer func() { _ = st.Close() }()

	brk, err := audit.Verify(st)
	if err != nil {
		fmt.Fprintf(stderr, "peerbus-broker: audit verify: %v\n", err)
		return 2
	}
	if brk != nil {
		fmt.Fprintf(stdout, "audit chain BROKEN: %s\n", brk.Error())
		return 1
	}
	fmt.Fprintln(stdout, "audit chain OK")
	return 0
}

// runServe loads the broker configuration (env-overrides-struct precedence,
// see internal/broker.LoadConfig) and runs the WebSocket broker until the
// process receives SIGINT/SIGTERM. The --db flag seeds the store path but
// PEERBUS_DB still overrides it (env precedence is uniform). Exit 0 on a
// clean shutdown, 2 on a config/operational error.
func runServe(dbPath string, stdout, stderr io.Writer) int {
	cfg, err := broker.LoadConfig(broker.Config{DBPath: dbPath})
	if err != nil {
		fmt.Fprintf(stderr, "peerbus-broker: %v\n", err)
		return 2
	}

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		fmt.Fprintf(stderr, "peerbus-broker: open store %q: %v\n", cfg.DBPath, err)
		return 2
	}
	defer func() { _ = st.Close() }()

	log := slog.New(slog.NewTextHandler(stderr, nil))
	srv := broker.NewServer(
		broker.NewAuthenticator(cfg.Tokens),
		broker.NewRegistry(),
		st,
		log,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Fprintf(stdout, "peerbus-broker: serving on %s\n", cfg.ListenAddr)
	if err := srv.ListenAndServe(ctx, cfg.ListenAddr); err != nil &&
		!errorsIsContextCanceled(err) {
		fmt.Fprintf(stderr, "peerbus-broker: serve: %v\n", err)
		return 2
	}
	fmt.Fprintln(stdout, "peerbus-broker: shut down cleanly")
	return 0
}

// errorsIsContextCanceled reports whether err is (or wraps) context
// cancellation, which is the expected outcome of a SIGINT/SIGTERM shutdown
// and must not be treated as a failure exit.
func errorsIsContextCanceled(err error) bool {
	return errors.Is(err, context.Canceled)
}
