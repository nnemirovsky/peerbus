// Command peerbus-broker is the long-lived agent-agnostic message broker.
//
// This is still a Task 2 scaffold for the WebSocket server (Tasks 7-8), but
// the `audit verify` subcommand (Task 6) is fully wired: it opens the durable
// store and walks the blake3 hash-chain audit log, reporting the first break.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/nnemirovsky/peerbus/internal/audit"
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
		fmt.Fprintf(stderr, "  audit verify    walk the blake3 audit hash-chain and report any break\n\n")
		fmt.Fprintf(stderr, "broker server (Tasks 7-8) is still a scaffold stub\n\n")
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
		fmt.Fprintln(stdout, "peerbus-broker: scaffold stub; broker server not implemented yet")
		return 0
	}

	switch rest[0] {
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
