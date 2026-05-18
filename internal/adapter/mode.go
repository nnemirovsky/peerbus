package adapter

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// --adapter mode dispatch — an ADDITIVE registry, not a switch.
//
// The broker never knows the adapter mode (Solution Overview: "broker
// never knows the mode"). Each mode is one Go binary behaviour selected by
// --adapter=<mode>. A new mode registers itself with Register (typically
// from an init() in its own file: internal/adapter/generic.go in Task 10,
// internal/adapter/cc.go in Task 11) WITHOUT editing any central switch —
// that is the "additive future modes" requirement.
//
// Task 9 is the skeleton: it ships the registry and registers placeholder
// constructors for the planned modes so the binary keeps its Task 2
// clean-exit contract. Tasks 10/11 RE-register "generic"/"cc" with the
// real implementations (Register overwrites, last registration wins),
// without touching this file.

// Mode is a running adapter instance. The skeleton's contract is just
// Run(ctx) -> error so the binary can start it and exit on its return;
// concrete modes (Task 10/11) embed a ResumingClient and an MCP/channel
// server behind this same interface.
type Mode interface {
	// Run drives the adapter until ctx is cancelled or a fatal error.
	Run(ctx context.Context) error
	// Name is the mode's --adapter token (for diagnostics).
	Name() string
}

// Constructor builds a Mode from the static client config. dedupeSize
// bounds the shared seen-id cache (non-positive => DefaultDedupeSize).
type Constructor func(cfg ClientConfig, dedupeSize int) (Mode, error)

var (
	registryMu sync.RWMutex
	modeReg    = map[string]Constructor{}
)

// Register adds (or replaces) the constructor for name. Calling it from an
// init() makes a new mode available with no edit to any dispatch switch.
// Re-registration (Tasks 10/11 replacing a placeholder) is intentional and
// last-wins.
func Register(name string, ctor Constructor) {
	registryMu.Lock()
	defer registryMu.Unlock()
	modeReg[name] = ctor
}

// Resolve returns the constructor registered for name, or a clear error
// (listing the known modes) for an unknown mode.
func Resolve(name string) (Constructor, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	ctor, ok := modeReg[name]
	if !ok {
		return nil, fmt.Errorf("adapter: unknown mode %q (known: %v)", name, sortedModesLocked())
	}
	return ctor, nil
}

// Modes returns the sorted list of registered mode names.
func Modes() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return sortedModesLocked()
}

// sortedModesLocked returns the registered names sorted. Caller holds the
// registry lock (read or write).
func sortedModesLocked() []string {
	names := make([]string, 0, len(modeReg))
	for n := range modeReg {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// placeholderMode is the Task 9 skeleton stand-in for a not-yet-built
// mode. It satisfies Mode and returns immediately with a clear error so
// the binary fails loudly rather than silently doing nothing if a
// placeholder is ever actually run. Tasks 10/11 replace these via
// Register; the binary's known-mode acceptance contract holds in the
// meantime.
type placeholderMode struct{ name string }

func (p placeholderMode) Name() string { return p.name }

func (p placeholderMode) Run(context.Context) error {
	return fmt.Errorf("adapter: mode %q not implemented yet (skeleton placeholder; lands in a later task)", p.name)
}

func init() {
	// Planned modes registered as placeholders so --adapter=generic|cc
	// still resolve in the Task 9 skeleton. Tasks 10/11 overwrite these
	// with the real constructors via their own init()s — no edit here.
	for _, m := range []string{"generic", "cc"} {
		name := m
		Register(name, func(_ ClientConfig, _ int) (Mode, error) {
			return placeholderMode{name: name}, nil
		})
	}
}
