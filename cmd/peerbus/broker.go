package main

// Broker subcommands: `serve` and `audit verify`. Ported verbatim from the
// v0.1.0 cmd/peerbus-broker/main.go — same flags, same env precedence, same
// exit codes (audit verify still exits 0 intact / 1 break / 2 operational).

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/nnemirovsky/peerbus/internal/audit"
	"github.com/nnemirovsky/peerbus/internal/broker"
	"github.com/nnemirovsky/peerbus/internal/store"
	"github.com/nnemirovsky/peerbus/internal/version"
)

// defaultDBPath is the store location used when --db is not given. Matches
// the v0.1.0 broker default.
const defaultDBPath = "peerbus.db"

// brokerServe parses serve-subcommand flags, loads broker config from env
// (env-overrides-struct precedence; see internal/broker.LoadConfig), and
// runs the WebSocket broker until SIGINT/SIGTERM. Exit 0 on clean shutdown,
// 2 on config/operational error. Behaviour mirrors v0.1.0 `peerbus-broker
// serve` exactly.
func brokerServe(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("peerbus serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	showVersion := fs.Bool("version", false, "print version and exit")
	dbPath := fs.String("db", defaultDBPath, "path to the durable SQLite store")
	fs.Usage = func() {
		_, _ = fmt.Fprintf(stderr, "usage: peerbus serve [flags]\n\n")
		_, _ = fmt.Fprintf(stderr, "config loaded from PEERBUS_* env (LISTEN, TOKENS, HMAC_SECRET, DB)\n\n")
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

	cfg, err := broker.LoadConfig(broker.Config{DBPath: *dbPath})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "peerbus: %v\n", err)
		return 2
	}

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "peerbus: open store %q: %v\n", cfg.DBPath, err)
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

	_, _ = fmt.Fprintf(stdout, "peerbus: serving on %s\n", cfg.ListenAddr)
	if err := srv.ListenAndServe(ctx, cfg.ListenAddr); err != nil &&
		!errors.Is(err, context.Canceled) {
		_, _ = fmt.Fprintf(stderr, "peerbus: serve: %v\n", err)
		return 2
	}
	_, _ = fmt.Fprintln(stdout, "peerbus: shut down cleanly")
	return 0
}

// brokerAuditVerify implements the `audit verify` subcommand. The first
// positional arg must be the literal verb "verify"; the --db flag is
// accepted EITHER before or after the verb (matches the v0.1.0 broker's
// flag-before-subcommand calling convention and its tests which pass
// `--db PATH audit verify` and bare `audit`).
//
// Exit codes: 0 chain intact, 1 a break was found, 2 usage/operational
// error.
func brokerAuditVerify(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("peerbus audit", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", defaultDBPath, "path to the durable SQLite store")
	fs.Usage = func() {
		_, _ = fmt.Fprintf(stderr, "usage: peerbus audit verify [--db PATH]\n")
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	rest := fs.Args()
	if len(rest) == 0 || rest[0] != "verify" {
		_, _ = fmt.Fprintln(stderr, "usage: peerbus audit verify [--db PATH]")
		return 2
	}

	st, err := store.Open(*dbPath)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "peerbus: open store %q: %v\n", *dbPath, err)
		return 2
	}
	defer func() { _ = st.Close() }()

	brk, err := audit.Verify(st)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "peerbus: audit verify: %v\n", err)
		return 2
	}
	if brk != nil {
		_, _ = fmt.Fprintf(stdout, "audit chain BROKEN: %s\n", brk.Error())
		return 1
	}
	_, _ = fmt.Fprintln(stdout, "audit chain OK")
	return 0
}
