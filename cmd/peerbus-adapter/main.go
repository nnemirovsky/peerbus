// Command peerbus-adapter is the thin, mostly-ephemeral adapter process.
//
// This is a Task 2 scaffold stub: it parses the --adapter=<mode> flag and
// exits cleanly for known placeholder modes, or non-zero with a clear error
// for an unknown/missing mode. The real broker WS client and MCP servers are
// wired in Tasks 9-11.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/nnemirovsky/peerbus/internal/version"
)

// knownModes is the scaffold placeholder mode set. The real additive dispatch
// table lands in Task 9 (internal/adapter/mode.go).
var knownModes = map[string]bool{
	"generic": true,
	"cc":      true,
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run parses adapter flags and returns a process exit code. Split out of main
// so the flag-parse behaviour is testable.
func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("peerbus-adapter", flag.ContinueOnError)
	fs.SetOutput(stderr)
	mode := fs.String("adapter", "", "adapter mode (generic|cc)")
	showVersion := fs.Bool("version", false, "print version and exit")
	fs.Usage = func() {
		fmt.Fprintf(stderr, "usage: peerbus-adapter --adapter=<mode>\n\n")
		fmt.Fprintf(stderr, "scaffold stub (Task 2); adapter logic lands in Tasks 9-11\n\n")
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
		fmt.Fprintln(stderr, "peerbus-adapter: missing required --adapter=<mode> (one of: generic, cc)")
		return 2
	}
	if !knownModes[*mode] {
		fmt.Fprintf(stderr, "peerbus-adapter: unknown adapter mode %q (one of: generic, cc)\n", *mode)
		return 2
	}

	fmt.Fprintf(stdout, "peerbus-adapter: mode %q accepted; adapter not implemented yet (scaffold stub)\n", *mode)
	return 0
}
