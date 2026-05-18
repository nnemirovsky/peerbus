// Package version exposes the peerbus build version.
package version

// Version is the current peerbus version. Bumped on release; may be
// overridden at build time via -ldflags "-X .../internal/version.Version=...".
var Version = "0.0.0-dev"

// String returns the human-readable version string.
func String() string {
	return "peerbus " + Version
}
