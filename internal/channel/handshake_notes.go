// Package channel will implement the Claude Code "channels" MCP capability
// (`claude/channel`) for the peerbus cc adapter.
//
// PROVISIONAL — BLOCKED SPIKE.
//
// Every type in this file is a PROVISIONAL reconstruction of the
// `claude/channel` handshake and push-wake schema. The authoritative schema
// could NOT be captured: the Task 1 spike ran inside a non-interactive agent
// that cannot launch an interactive
// `claude --dangerously-load-development-channels` session. See
// docs/spikes/claude-channel-handshake.md for (a) the intended capture
// procedure, (b) why automated capture is impossible here, and (c) the
// provisional schema these structs mirror.
//
// These structs carry NO logic. They exist only so the provisional schema is
// expressed in Go and round-trip tested. They MUST be reconciled against a
// real captured session before any --adapter=cc work begins. Per the plan's
// Task 1 blocker clause, the project proceeds under the generic-only reduced
// plan variant; this file is a placeholder for the deferred cc path.
package channel

import "encoding/json"

// ProvisionalProtocolVersion is the MCP protocol version string guessed for
// the handshake. PROVISIONAL — unverified.
const ProvisionalProtocolVersion = "2025-06-18"

// ProvisionalChannelCapabilityKey is the experimental capability key the
// server is assumed to advertise. PROVISIONAL — the plan specifies
// `experimental: { "claude/channel": {} }`; the exact nesting is unverified.
const ProvisionalChannelCapabilityKey = "claude/channel"

// ProvisionalPushMethod is the guessed JSON-RPC notification method used to
// push-wake an idle session. PROVISIONAL — unverified.
const ProvisionalPushMethod = "notifications/claude/channel"

// ClientInfo is the MCP client identity block. PROVISIONAL.
type ClientInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// ServerInfo is the MCP server identity block. PROVISIONAL.
type ServerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// Capabilities is the MCP capabilities object. The channel capability is
// assumed to live under `experimental["claude/channel"]` as an (possibly
// empty) object. PROVISIONAL — nesting and key are unverified.
type Capabilities struct {
	Experimental map[string]json.RawMessage `json:"experimental,omitempty"`
	Tools        json.RawMessage            `json:"tools,omitempty"`
}

// InitializeParams is the `initialize` request params (client -> server).
// PROVISIONAL.
type InitializeParams struct {
	ProtocolVersion string       `json:"protocolVersion"`
	Capabilities    Capabilities `json:"capabilities"`
	ClientInfo      ClientInfo   `json:"clientInfo"`
}

// InitializeResult is the `initialize` result (server -> client). The server
// echoes the channel capability so Claude treats it as push-capable.
// PROVISIONAL.
type InitializeResult struct {
	ProtocolVersion string       `json:"protocolVersion"`
	Capabilities    Capabilities `json:"capabilities"`
	ServerInfo      ServerInfo   `json:"serverInfo"`
}

// ContentBlock is one MCP-style content block in a push notification.
// PROVISIONAL — content may instead be a flat string.
type ContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

// PushMetadata carries peerbus envelope hints surfaced to the session.
// PROVISIONAL.
type PushMetadata struct {
	From   string `json:"from,omitempty"`
	Source string `json:"source,omitempty"`
}

// ChannelPushParams is the `params` of a `notifications/claude/channel` push.
// PROVISIONAL — field names, nesting, and the correlation id are guesses.
type ChannelPushParams struct {
	ChannelID string         `json:"channelId"`
	TurnID    string         `json:"turnId,omitempty"`
	Content   []ContentBlock `json:"content"`
	Metadata  PushMetadata   `json:"metadata,omitempty"`
}

// ChannelPushNotification is a full JSON-RPC notification frame that
// push-wakes an idle session. A notification has no `id`. PROVISIONAL.
//
// Method MUST equal ProvisionalPushMethod for the frame to be a valid push;
// the round-trip test treats a missing/empty Method as the malformed case.
type ChannelPushNotification struct {
	JSONRPC string            `json:"jsonrpc"`
	Method  string            `json:"method"`
	Params  ChannelPushParams `json:"params"`
}
