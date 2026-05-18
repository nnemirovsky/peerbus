// Command peerbus-broker is the long-lived agent-agnostic message broker.
//
// This is a Task 2 scaffold stub: it parses flags and exits cleanly. The
// WebSocket server, token auth, durable routing, and the `audit verify`
// subcommand are wired in Tasks 6-8.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/nnemirovsky/peerbus/internal/version"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run parses broker flags and returns a process exit code. It is split out of
// main so it is testable.
func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("peerbus-broker", flag.ContinueOnError)
	fs.SetOutput(stderr)
	showVersion := fs.Bool("version", false, "print version and exit")
	fs.Usage = func() {
		fmt.Fprintf(stderr, "usage: peerbus-broker [flags] [subcommand]\n\n")
		fmt.Fprintf(stderr, "scaffold stub (Task 2); broker server lands in Tasks 7-8\n\n")
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

	// Subcommand placeholder (e.g. `audit verify`) — implemented in Task 6.
	if rest := fs.Args(); len(rest) > 0 {
		fmt.Fprintf(stdout, "peerbus-broker: subcommand %q not implemented yet (scaffold stub)\n", rest[0])
		return 0
	}

	fmt.Fprintln(stdout, "peerbus-broker: scaffold stub; broker server not implemented yet")
	return 0
}
