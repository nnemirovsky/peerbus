// Command peerbus is the single multi-command binary for the peerbus
// project: one git/kubectl-style dispatcher for the broker and adapter
// subcommands.
//
// Subcommands:
//
//	serve                       start the WebSocket broker (token auth + peer
//	                            registry + direct/broadcast routing, offline
//	                            queue, ack/redelivery).
//	audit verify [--db PATH]    walk the blake3 hash-chain audit log and
//	                            report any break.
//	adapter --adapter=<mode>    run the adapter (mode resolved through the
//	                            additive --adapter dispatch registry; today:
//	                            cc | generic).
//
// Top-level flags:
//
//	--version    print version and exit 0 (handled BEFORE subcommand parsing
//	             so `peerbus --version` works without a subcommand).
//	help, -h, --help  print the usage block listing the above subcommands.
//
// Unknown subcommand → "peerbus: unknown command %q" on stderr, usage block,
// exit 2. Per-subcommand exit codes are preserved (e.g. audit verify still
// exits 0 intact / 1 break / 2 operational).
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/nnemirovsky/peerbus/internal/version"
)

func main() {
	os.Exit(dispatch(os.Args[1:], os.Stdout, os.Stderr))
}

// dispatch routes the top-level args to the matching subcommand handler.
// Split out of main so the routing is testable; each subcommand handler
// (brokerServe, brokerAuditVerify, adapterRun) is also independently
// testable.
func dispatch(args []string, stdout, stderr io.Writer) int {
	// Top-level --version MUST work BEFORE subcommand parsing so
	// `peerbus --version` is answerable without a subcommand.
	if len(args) > 0 {
		switch args[0] {
		case "--version", "-version":
			_, _ = fmt.Fprintln(stdout, version.String())
			return 0
		case "help", "-h", "--help":
			printUsage(stdout)
			return 0
		}
	}

	if len(args) == 0 {
		_, _ = fmt.Fprintln(stderr, "peerbus: a subcommand is required")
		printUsage(stderr)
		return 2
	}

	switch args[0] {
	case "serve":
		return brokerServe(args[1:], stdout, stderr)
	case "audit":
		return brokerAuditVerify(args[1:], stdout, stderr)
	case "adapter":
		return adapterRun(args[1:], stdout, stderr)
	default:
		_, _ = fmt.Fprintf(stderr, "peerbus: unknown command %q\n", args[0])
		printUsage(stderr)
		return 2
	}
}

// printUsage emits the top-level usage block listing every subcommand. Kept
// in one place so `help`, no-args, and unknown-command all show the same
// surface.
func printUsage(w io.Writer) {
	_, _ = fmt.Fprintf(w, "usage: peerbus <command> [flags]\n\n")
	_, _ = fmt.Fprintf(w, "commands:\n")
	_, _ = fmt.Fprintf(w, "  serve                        start the WebSocket broker\n")
	_, _ = fmt.Fprintf(w, "  audit verify [--db PATH]     walk the blake3 audit hash-chain\n")
	_, _ = fmt.Fprintf(w, "  adapter --adapter=<mode>     run an adapter (cc | generic)\n\n")
	_, _ = fmt.Fprintf(w, "flags:\n")
	_, _ = fmt.Fprintf(w, "  --version                    print version and exit\n")
	_, _ = fmt.Fprintf(w, "  -h, --help                   print this help and exit\n\n")
	_, _ = fmt.Fprintf(w, "serve config is loaded from PEERBUS_* env (LISTEN, TOKENS, HMAC_SECRET, DB).\n")
	_, _ = fmt.Fprintf(w, "adapter config is loaded from PEERBUS_* env (URL, NAME, TOKEN, HMAC_SECRET).\n")
}
