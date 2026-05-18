// Package channel implements the Claude Code "channels" MCP capability
// (`claude/channel`) for the peerbus cc adapter.
//
// DOCUMENTED SCHEMA.
//
// Every type in this file mirrors the authoritative `claude/channel` wire
// schema sourced from official Claude Code documentation
// (`channels-reference.md`, Claude Code v2.1.80+), recorded verbatim at the
// repo root in CHANNELS_SCHEMA.md and summarized in
// docs/spikes/claude-channel-handshake.md. No live capture was required; the
// earlier PROVISIONAL/BLOCKED status is rescinded.
//
// These structs document the schema in Go and are round-trip tested. The
// live cc adapter (channel.go) builds the same frames directly; this file is
// the schema-of-record the tests pin to.
package channel

import "encoding/json"

// MCPProtocolVersion is the MCP protocol version string the cc adapter
// advertises (echoed from the client when present; this is the fallback).
const MCPProtocolVersion = "2025-06-18"

// ChannelCapabilityKey is the experimental capability key the server
// advertises: capabilities.experimental["claude/channel"] = {} (DOCUMENTED,
// CHANNELS_SCHEMA.md §1). Its presence registers Claude Code's notification
// listener for the push method below.
const ChannelCapabilityKey = "claude/channel"

// PushMethod is the JSON-RPC notification method the server emits to
// push-wake an idle session (DOCUMENTED, CHANNELS_SCHEMA.md §3).
const PushMethod = "notifications/claude/channel"

// ClientInfo is the MCP client identity block.
type ClientInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// ServerInfo is the MCP server identity block.
type ServerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// Capabilities is the MCP capabilities object. The channel capability lives
// under experimental["claude/channel"] as an empty object; tools is the
// standard MCP capability, present because the cc adapter exposes the bus.*
// reply tools (two-way channel). DOCUMENTED — CHANNELS_SCHEMA.md §1.
type Capabilities struct {
	Experimental map[string]json.RawMessage `json:"experimental,omitempty"`
	Tools        json.RawMessage            `json:"tools,omitempty"`
}

// InitializeParams is the `initialize` request params (client -> server).
type InitializeParams struct {
	ProtocolVersion string       `json:"protocolVersion"`
	Capabilities    Capabilities `json:"capabilities"`
	ClientInfo      ClientInfo   `json:"clientInfo"`
}

// InitializeResult is the `initialize` result (server -> client). The server
// advertises experimental["claude/channel"]={} (so Claude treats it as a
// push-capable channel) and tools={} (so Claude discovers the bus.* reply
// tools). DOCUMENTED — CHANNELS_SCHEMA.md §1.
type InitializeResult struct {
	ProtocolVersion string       `json:"protocolVersion"`
	Capabilities    Capabilities `json:"capabilities"`
	ServerInfo      ServerInfo   `json:"serverInfo"`
}

// PushParams is the `params` of a `notifications/claude/channel`
// push (DOCUMENTED — CHANNELS_SCHEMA.md §3):
//
//   - Content (required): the event body, delivered as the text content of
//     the injected <channel> XML tag.
//   - Meta (optional): each key/value becomes an XML attribute on the
//     <channel> tag. ALL VALUES MUST BE STRINGS. Keys must be valid
//     identifiers (letters, digits, underscores only); keys with hyphens or
//     special characters are silently dropped by Claude Code.
type PushParams struct {
	Content string            `json:"content"`
	Meta    map[string]string `json:"meta,omitempty"`
}

// PushNotification is a full JSON-RPC notification frame that
// push-wakes an idle session. A notification has no `id`.
//
// Method MUST equal PushMethod for the frame to be a valid push; the
// round-trip test treats a missing/empty Method as the malformed case.
type PushNotification struct {
	JSONRPC string     `json:"jsonrpc"`
	Method  string     `json:"method"`
	Params  PushParams `json:"params"`
}
