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
// The real modes register themselves from their own init() — generic.go
// registers "generic", cc.go registers "cc" — so this file owns ONLY the
// registry mechanism, never a per-mode entry. Adding a mode is a new file
// with an init(), no edit here.

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
// Re-registration is allowed and last-wins.
func Register(name string, ctor Constructor) {
	registryMu.Lock()
	defer registryMu.Unlock()
	modeReg[name] = ctor
}

// unregisterMode removes the constructor for name (no-op if absent). It
// exists only so tests that Register a throwaway mode can restore the
// package-global registry to its prior state (t.Cleanup), keeping the
// registry repeat-safe under `go test -count=N`.
func unregisterMode(name string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	delete(modeReg, name)
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
