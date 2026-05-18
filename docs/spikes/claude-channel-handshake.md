# Spike: Claude `claude/channel` handshake (DOCUMENTED)

> **Status: DOCUMENTED — from official Claude Code `channels-reference`.**
> No live capture was required. The full, exact `claude/channel` wire schema
> was obtained from official Claude Code documentation
> (`channels-reference.md`, Claude Code v2.1.80+) and is recorded verbatim in
> [`CHANNELS_SCHEMA.md`](../../CHANNELS_SCHEMA.md) at the repo root. That file
> is the authoritative source; this doc summarizes it for the cc adapter and
> records the (now-unnecessary) capture procedure for history.
>
> The earlier BLOCKED/PROVISIONAL status is rescinded. The generic-only
> reduced plan variant in `docs/plans/20260518-peerbus.md` is RESCINDED;
> Task 11 and the cc rows of Task 12 are back in scope (see the Task 1
> `✅ RESOLVED` note in the plan).

## (a) Authoritative schema (DOCUMENTED — see CHANNELS_SCHEMA.md)

Sourced from `https://code.claude.com/docs/en/channels-reference.md` (Claude
Code v2.1.80+). The full reference with field tables, delivery guarantees,
and the permission-relay protocol lives in
[`CHANNELS_SCHEMA.md`](../../CHANNELS_SCHEMA.md). Summary of what the cc
adapter implements:

### Capability declaration (initialize result, server → client)

The MCP server declares the channel capability under
`capabilities.experimental`:

```json
{
  "capabilities": {
    "experimental": { "claude/channel": {} },
    "tools": {}
  }
}
```

- Key `experimental["claude/channel"]` (exact string), value always `{}`.
- `tools: {}` is the standard MCP capability; it must be present because the
  cc adapter also exposes the `bus.*` reply tools (two-way channel).
- The presence of `experimental["claude/channel"]` is what registers the
  notification listener so Claude Code accepts `notifications/claude/channel`.
- peerbus deliberately does **not** declare
  `experimental["claude/channel/permission"]`: the optional permission relay
  is intentionally out of scope — escalation policy lives in the consuming
  agent's prompt, never in the bus.

### Push / wake notification (server → client)

A JSON-RPC **notification** (no `id`):

```json
{
  "jsonrpc": "2.0",
  "method": "notifications/claude/channel",
  "params": {
    "content": "<message body as text>",
    "meta": { "from": "...", "source": "peer-bus", "msg_id": "..." }
  }
}
```

- **Method**: `notifications/claude/channel` (exact string).
- **`params.content`**: required string — the event body. Claude receives it
  as the text content of an injected `<channel>` XML block.
- **`params.meta`**: optional `Record<string, string>` — each key/value
  becomes an XML attribute on the `<channel>` tag. **All values must be
  strings.** Keys must be valid identifiers (letters, digits, underscores
  only); keys with hyphens or special characters are silently dropped
  (CHANNELS_SCHEMA.md §3). The cc adapter therefore uses identifier-safe
  meta keys `from`, `source`, `msg_id`.
- `source` is also set automatically by Claude Code from the MCP server's
  configured name; peerbus additionally carries the envelope `source`
  (`peer-bus`) in `meta.source` so the consuming agent's prompt can key its
  escalation policy off it regardless of the server name.

### Reply path (two-way channel)

Standard MCP — there is **no** special channel notification for replies.
Claude calls ordinary MCP tools the server advertises via
`ListToolsRequestSchema` / `CallToolRequestSchema`. peerbus exposes the same
`bus.send` / `bus.broadcast` / `bus.peers` tool surface as the generic
adapter. The tool call and its result are the complete reply protocol; no
turn/correlation id is echoed.

## (b) Capture procedure (retained for history — NOT needed)

The schema is fully documented; no live capture is necessary. The original
procedure (build a tee MCP server, launch `claude
--dangerously-load-development-channels server:<tee>`, capture frames) is
preserved here only as historical context — it has been superseded by the
official documentation in `CHANNELS_SCHEMA.md`. The remaining manual step is
the **end-to-end behavioural** verification (does an idle session actually
wake and can Claude reach a second peer), which is the Post-Completion
manual checklist at `docs/manual-e2e-claude-channel.md` — that is a
behavioural smoke test, not a schema capture.

## (c) Malformed sample (error-path test fixture)

A push frame missing the required `method` field — must fail to parse as a
valid push notification:

```json
{
  "jsonrpc": "2.0",
  "params": { "content": "x" }
}
```

## References

- [`CHANNELS_SCHEMA.md`](../../CHANNELS_SCHEMA.md) — authoritative wire schema
- Official channels reference: https://code.claude.com/docs/en/channels-reference.md
- Official channels overview: https://code.claude.com/docs/en/channels.md
