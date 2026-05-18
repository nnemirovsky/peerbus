// Package channel implements the Claude Code "channels" MCP capability
// (`claude/channel`) for the peerbus cc adapter (--adapter=cc).
//
// SCHEMA: DOCUMENTED. Every wire shape here mirrors the authoritative
// schema in CHANNELS_SCHEMA.md (Claude Code v2.1.80+, channels-reference);
// the typed mirror is in handshake_notes.go and is round-trip tested. No
// live capture was required.
//
// ── What this is ──
//
// A stdio MCP server that is, additionally, a claude/channel: it advertises
// capabilities.experimental["claude/channel"]={} (registers Claude Code's
// notification listener so push-wake works) AND the standard tools={}
// capability (the bus.* reply tools). It is built ON TOP of internal/mcp —
// the SAME JSON-RPC core the generic adapter uses; internal/mcp was extended
// additively (ServerOption + Server.Notify) rather than forked. The
// JSON-RPC framing, dispatch, and tool plumbing are not reimplemented here.
//
// Inbound (push-wake): a broker `deliver` (already HMAC-verified and deduped
// by the SHARED internal/adapter machinery — see internal/adapter/cc.go) is
// mapped to a JSON-RPC notification `notifications/claude/channel` with
//
//	params = { content: <message body as text>,
//	           meta: { from, source, msg_id } }   (all meta values strings)
//
// emitted via mcp.Server.Notify (the additive server->client path). meta
// keys are identifier-safe (letters/digits/underscore only) per
// CHANNELS_SCHEMA.md §3 — keys with hyphens are silently dropped by Claude
// Code, so we use from / source / msg_id.
//
// Outbound (reply path): standard MCP tools/list + tools/call exposing
// bus.send / bus.broadcast / bus.peers — the SAME tool surface and
// semantics as the generic adapter, served by the same internal/mcp tool
// plumbing over the SAME broker client + shared dedupe + HMAC (wired in
// internal/adapter/cc.go). The reply path is ordinary MCP tool calls; there
// is no special channel reply notification and no turn/correlation id
// (CHANNELS_SCHEMA.md §4).
//
// Permission relay (notifications/claude/channel/permission) is DELIBERATELY
// NOT implemented: peerbus keeps escalation policy in the consuming agent's
// prompt, never in the bus. We therefore do not declare
// experimental["claude/channel/permission"].
package channel

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/nnemirovsky/peerbus/internal/mcp"
)

// OutboundBus is the broker-facing reply surface the bus.* tools delegate
// to. internal/adapter/cc.go implements it over the shared resuming broker
// client (HMAC sign + reconnect/resume) — the channel layer never touches
// the broker, HMAC, or dedupe itself.
type OutboundBus interface {
	Send(ctx context.Context, to string, body json.RawMessage) error
	Broadcast(ctx context.Context, body json.RawMessage) error
	Peers(ctx context.Context) ([]string, error)
}

// Inbound is one already-HMAC-verified, already-deduped delivery the cc
// adapter pushes into the session. Source is the envelope `source` (e.g.
// "peer-bus" — the tag the consuming agent's prompt keys escalation off;
// peerbus itself has no such logic). Body is the opaque application JSON
// verbatim.
type Inbound struct {
	ID     string
	From   string
	Source string
	Body   json.RawMessage
}

// Server is the cc-adapter MCP server: the internal/mcp JSON-RPC core
// configured as a claude/channel (experimental capability + Notify push
// path), with the bus.* tools delegating to an OutboundBus.
type Server struct {
	mcp *mcp.Server
	out OutboundBus
}

// busAdapter bridges the channel's OutboundBus to the mcp.Bus interface the
// internal/mcp tool plumbing expects. Drain is intentionally inert: the cc
// adapter is push-driven (inbound arrives as claude/channel notifications,
// not via a host-driven bus.drain), and bus.drain is NOT advertised to the
// host for cc — see toolsListResult. Returning empty keeps the shared tool
// plumbing reusable without a cc-specific fork.
type busAdapter struct{ out OutboundBus }

func (b busAdapter) Send(ctx context.Context, to string, body json.RawMessage) error {
	return b.out.Send(ctx, to, body)
}
func (b busAdapter) Broadcast(ctx context.Context, body json.RawMessage) error {
	return b.out.Broadcast(ctx, body)
}
func (b busAdapter) Peers(ctx context.Context) ([]string, error) {
	return b.out.Peers(ctx)
}
func (b busAdapter) Drain(context.Context) ([]mcp.InboundMessage, error) {
	return nil, nil
}

// NewServer builds the cc-adapter MCP server reading framed JSON-RPC from in
// and writing newline-delimited JSON-RPC to out. It advertises the
// claude/channel experimental capability and the standard tools capability,
// and serves bus.send/bus.broadcast/bus.peers over the supplied OutboundBus.
func NewServer(ob OutboundBus, in io.Reader, w io.Writer) *Server {
	srv := mcp.NewServer(
		busAdapter{out: ob},
		in, w,
		mcp.WithServerName("peerbus-cc-adapter"),
		mcp.WithoutDrain(),
		// DOCUMENTED — CHANNELS_SCHEMA.md §1: experimental["claude/channel"]
		// value is always {}. (NOT claude/channel/permission — see package
		// doc.) The key+shape is fixed, so this is a parameterless option.
		mcp.WithChannelCapability(),
	)
	return &Server{mcp: srv, out: ob}
}

// Serve runs the JSON-RPC read/dispatch loop until ctx is cancelled or stdin
// closes (the cc adapter's lifecycle is bound to its stdio session).
func (s *Server) Serve(ctx context.Context) error { return s.mcp.Serve(ctx) }

// Deliver maps one inbound broker delivery to a claude/channel push-wake
// notification and emits it (DOCUMENTED — CHANNELS_SCHEMA.md §3). content is
// the message body as text; meta carries identifier-safe string attributes
// (from / source / msg_id) that Claude Code surfaces as <channel> XML
// attributes. The body is opaque JSON: if it is a JSON string we unwrap it
// to its text so the session sees plain text, otherwise the compact JSON is
// passed through verbatim.
func (s *Server) Deliver(in Inbound) {
	s.mcp.Notify(PushMethod, ChannelPushParams{
		Content: bodyAsText(in.Body),
		Meta: map[string]string{
			"from":   in.From,
			"source": in.Source,
			"msg_id": in.ID,
		},
	})
}

// bodyAsText renders an opaque body for the channel `content` string. A JSON
// string body is unwrapped to its raw text (so Claude sees the message, not
// a quoted JSON literal); anything else is passed through as compact JSON.
func bodyAsText(body json.RawMessage) string {
	if len(body) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(body, &s); err == nil {
		return s
	}
	return string(body)
}

// UniqueName generates a stable-ish unique peer name for auto-registration
// (cc2cc-parity ergonomics) when no name is configured. Scheme:
//
//	cc-<hostname>-<pid>-<6 hex random>
//
// hostname+pid make it readable and naturally distinct per session/host;
// the random suffix breaks ties if two sessions on the same host race the
// same pid reuse across a restart. Documented here so the scheme is part of
// the contract, not an implementation accident.
func UniqueName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	var r [3]byte
	_, _ = rand.Read(r[:])
	return fmt.Sprintf("cc-%s-%d-%s", host, os.Getpid(), hex.EncodeToString(r[:]))
}
