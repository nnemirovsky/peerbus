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
//	params = { content: <pretty multi-line summary>,
//	           meta:    { from, source, msg_id, kind } }
//
// emitted via mcp.Server.Notify (the additive server->client path). meta
// keys are identifier-safe (letters/digits/underscore only) per
// CHANNELS_SCHEMA.md §3 — keys with hyphens are silently dropped by Claude
// Code, so we use from / source / msg_id / kind. The pretty content shape
// (📨 banner + From/Type/Content lines) mirrors cc2cc's per-message render
// so a session reading peerbus and cc2cc traffic side-by-side sees a
// uniform layout; see formatInbound.
//
// On every successful broker (re)register the cc adapter emits ONE
// system-kind notification (kind="system", content "📡 peerbus: connected
// as <name>") so the consuming agent immediately knows its own bus name
// without an explicit bus.whoami round-trip — see AnnounceSelf. The push is
// gated on the MCP client having sent notifications/initialized: Claude
// Code silently drops claude/channel notifications received before the
// handshake completes (CHANNELS_SCHEMA.md §3), so a pre-handshake announce
// would never reach turn 1 of the session.
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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/nnemirovsky/peerbus/internal/mcp"
)

// OutboundBus is the broker-facing reply surface the bus.* tools delegate
// to. internal/adapter/cc.go implements it over the shared resuming broker
// client (HMAC sign + reconnect/resume) — the channel layer never touches
// the broker, HMAC, or dedupe itself.
//
// Peers returns (self, peers, err): self is THIS adapter's bound peer name
// (so bus.peers can echo it back in the shaped {self, peers} result without
// a separate bus.whoami round-trip); peers is the broker registry sans the
// caller's own entry (filtered at the bus implementation — the broker
// returns the full registry including this peer, and exposing yourself in
// "peers" is confusing for the consuming agent).
type OutboundBus interface {
	Send(ctx context.Context, to string, body json.RawMessage) error
	Broadcast(ctx context.Context, body json.RawMessage) error
	Peers(ctx context.Context) (self string, peers []string, err error)
}

// Inbound is one already-HMAC-verified, already-deduped delivery the cc
// adapter pushes into the session. Source is the envelope `source` (e.g.
// "peer-bus" — the tag the consuming agent's prompt keys escalation off;
// peerbus itself has no such logic). Kind is the envelope `kind` ("msg" or
// "broadcast") so the channel layer can surface it as the <channel> XML
// kind attribute without re-decoding the body. Body is the opaque
// application JSON verbatim.
type Inbound struct {
	ID     string
	From   string
	Source string
	Kind   string
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

func (b busAdapter) Peers(ctx context.Context) (string, []string, error) {
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
// a multi-line human-readable summary of the inbound message (see
// formatInbound); meta carries identifier-safe string attributes
// (from / source / msg_id / kind) that Claude Code surfaces as <channel> XML
// attributes.
//
// "kind" is "msg" for direct messages and "broadcast" for fan-outs; the
// channel layer takes the kind from the inbound envelope so the consuming
// agent's prompt can branch on it without re-parsing the body.
func (s *Server) Deliver(in Inbound) {
	s.mcp.Notify(PushMethod, PushParams{
		Content: formatInbound(in),
		Meta: map[string]string{
			"from":   in.From,
			"source": in.Source,
			"msg_id": in.ID,
			"kind":   in.Kind,
		},
	})
}

// AnnounceSelf emits a single system-kind claude/channel notification
// telling the session what peer name this adapter bound under. meta.kind is
// "system" so the consuming agent can ignore it from human-style escalation
// logic.
//
// CALLER CONTRACT: this is the unconditional push primitive. The cc adapter
// MUST gate the call on Initialized() — Claude Code silently drops
// server-initiated notifications received before the client signals
// notifications/initialized (CHANNELS_SCHEMA.md §3), so a pre-handshake
// announce never reaches turn 1 of the session. See
// internal/adapter/cc.go's Run for the wiring.
func (s *Server) AnnounceSelf(self string) {
	s.mcp.Notify(PushMethod, PushParams{
		Content: fmt.Sprintf("\U0001F4E1 peerbus: connected as %s", self),
		Meta: map[string]string{
			"kind": "system",
			"self": self,
		},
	})
}

// Initialized returns a channel that is closed once the MCP client has sent
// notifications/initialized (the handshake completion signal). The cc
// adapter's startup self-announce waits on this before pushing — see
// AnnounceSelf's caller contract.
func (s *Server) Initialized() <-chan struct{} { return s.mcp.Initialized() }

// formatInbound renders the pretty multi-line channel content from one
// inbound delivery. The shape matches cc2cc's per-message banner so a
// session reading peerbus traffic and cc2cc traffic side-by-side sees a
// uniform layout. The "kind" line is "msg" for direct messages and
// "broadcast" for fan-outs (defaults to "msg" if unset for safety — an
// older sender that pre-dates the kind field still renders cleanly).
func formatInbound(in Inbound) string {
	kind := in.Kind
	if kind == "" {
		kind = "msg"
	}
	var b strings.Builder
	b.WriteString("\U0001F4E8 peerbus message\n")
	b.WriteString("From: ")
	b.WriteString(in.From)
	b.WriteString("\n")
	b.WriteString("Type: ")
	b.WriteString(kind)
	b.WriteString("\n")
	b.WriteString("Content: ")
	b.WriteString(decodeBody(in.Body))
	return b.String()
}

// decodeBody renders an opaque body for the pretty channel content. Rules,
// applied IN ORDER:
//
//  1. body is a JSON string  -> unwrap to the inner string.
//  2. body is a JSON object containing "text" / "message" / "content"
//     (first match wins) -> use that field's value (stringify if non-string).
//  3. otherwise -> compact-encode the body JSON as-is.
//
// An empty body renders as the empty string.
func decodeBody(body json.RawMessage) string {
	if len(body) == 0 {
		return ""
	}
	// (1) plain JSON string.
	var s string
	if err := json.Unmarshal(body, &s); err == nil {
		return s
	}
	// (2) JSON object with a known text-bearing field.
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err == nil {
		for _, key := range []string{"text", "message", "content"} {
			if raw, ok := obj[key]; ok {
				var str string
				if err := json.Unmarshal(raw, &str); err == nil {
					return str
				}
				// non-string field value: pass through as compact JSON.
				return string(raw)
			}
		}
	}
	// (3) compact JSON verbatim.
	return string(body)
}

// nameSuffixAlphabet is the base36 alphabet used for the 3-char random
// suffix in UniqueName. Lowercase to match the rest of the name.
const nameSuffixAlphabet = "0123456789abcdefghijklmnopqrstuvwxyz"

// nameSuffixLen is the length of the random suffix in a generated name.
// 3 chars in base36 = 46656 distinct suffixes per (adjective, noun) pair.
// Combined with ~200 adjectives and ~200 nouns the total keyspace is
// ~1.86 billion combinations, so a same-token collision (two adapters
// independently minting the same name and then needing to reconcile via
// the registry's same-token takeover) is essentially impossible. The
// adapter's collision-retry loop on ErrNameClaimed (a different-token
// claim) is therefore a defence-in-depth backstop, not the primary safety
// mechanism — see internal/adapter/cc.go.
const nameSuffixLen = 3

// UniqueName mints a friendly, lowercase peer name in the shape
// "<adjective>-<noun>-<3 base36>" (e.g. "wild-wasp-3kx"). It honours the
// PEERBUS_NAME environment variable verbatim when set (operator override),
// otherwise draws fresh entropy via crypto/rand.
//
// The scheme replaces the older "cc-<hostname>-<pid>-<rand>" identifier —
// friendlier to read in logs / lists / Claude Code's <channel> tag while
// keeping a huge keyspace.
func UniqueName() string {
	if override := os.Getenv("PEERBUS_NAME"); override != "" {
		return override
	}
	return generateName()
}

// generateName produces a fresh "<adjective>-<noun>-<3 base36>" name.
// Split out of UniqueName so the env-override path can be tested
// independently of the random draw.
func generateName() string {
	adj := pickWord(nameAdjectives)
	noun := pickWord(nameNouns)
	return fmt.Sprintf("%s-%s-%s", adj, noun, randomSuffix())
}

// pickWord picks one entry uniformly at random from words. The list must
// be non-empty (the curated wordlist.go guarantees this).
func pickWord(words []string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	// 64-bit unbiased index: modulo a non-power-of-two introduces a
	// negligible bias for our list sizes (~200), well below the noise
	// floor of name collisions we already tolerate.
	n := uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
	return words[n%uint64(len(words))]
}

// randomSuffix returns a nameSuffixLen-char lowercase base36 string drawn
// from crypto/rand.
func randomSuffix() string {
	var b [nameSuffixLen]byte
	_, _ = rand.Read(b[:])
	out := make([]byte, nameSuffixLen)
	for i, v := range b {
		out[i] = nameSuffixAlphabet[int(v)%len(nameSuffixAlphabet)]
	}
	return string(out)
}
