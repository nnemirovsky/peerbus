// Command peerbus-adapter is the thin, mostly-ephemeral adapter process.
//
// Mode selection goes through the additive --adapter dispatch registry
// (internal/adapter.Resolve): a mode is looked up by name, not by a
// hard-coded switch, so future modes register without editing this file.
// The real per-mode run loops (generic stdio MCP, cc claude/channel) land
// in Tasks 10/11; this Task 9 skeleton resolves + acknowledges the mode
// and exits cleanly. --version and the missing/unknown-mode non-zero
// contract from Task 2 are preserved.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/nnemirovsky/peerbus/internal/adapter"
	"github.com/nnemirovsky/peerbus/internal/version"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run parses adapter flags and returns a process exit code. Split out of
// main so the flag-parse behaviour is testable.
func run(args []string, stdout, stderr io.Writer) int {
	known := strings.Join(adapter.Modes(), ", ")

	fs := flag.NewFlagSet("peerbus-adapter", flag.ContinueOnError)
	fs.SetOutput(stderr)
	mode := fs.String("adapter", "", "adapter mode ("+strings.ReplaceAll(known, ", ", "|")+")")
	showVersion := fs.Bool("version", false, "print version and exit")
	fs.Usage = func() {
		fmt.Fprintf(stderr, "usage: peerbus-adapter --adapter=<mode>\n\n")
		fmt.Fprintf(stderr, "modes are resolved via the additive --adapter dispatch registry\n\n")
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
	if _, err := adapter.Resolve(*mode); err != nil {
		fmt.Fprintf(stderr, "peerbus-adapter: unknown adapter mode %q (one of: %s)\n", *mode, known)
		return 2
	}

	// Skeleton: the mode resolves through the registry. The concrete
	// run loop is wired in Tasks 10/11; here we acknowledge and exit
	// cleanly, preserving the Task 2 contract.
	fmt.Fprintf(stdout, "peerbus-adapter: mode %q accepted; adapter run loop wired in a later task (skeleton)\n", *mode)
	return 0
}
