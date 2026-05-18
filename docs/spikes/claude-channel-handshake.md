# Spike: Claude `claude/channel` handshake (PROVISIONAL — BLOCKED)

> **Status: BLOCKED — provisional schema only.**
> The live `claude/channel` MCP handshake could not be captured because this
> spike was executed by a **non-interactive agent** that cannot launch an
> interactive `claude --dangerously-load-development-channels` session. Per the
> Task 1 blocker clause in `docs/plans/20260518-peerbus.md`, the project now
> proceeds under the **generic-only reduced plan variant** (Tasks 2–10, 12
> generic rows only, 13, 15; Task 11 and the cc rows of Task 12 SKIPPED; Task 14
> uses the relaxed acceptance variant). Everything below the "Intended capture
> procedure" section is **reconstructed from public documentation and known
> MCP / `notifications/*` shapes — it is PROVISIONAL and MUST be re-verified
> against a real interactive session before any `--adapter=cc` work begins.**

## (a) Intended capture procedure (for a future interactive operator)

This is the procedure that *should* be run from a real, human-driven terminal
to capture the authoritative schema. It is documented here so the spike can be
completed later without re-deriving it.

1. Build a throwaway stdio "tee" MCP server that:
   - speaks the standard MCP stdio framing (newline-delimited / `Content-Length`
     framed JSON-RPC, whichever the client negotiates),
   - in its `initialize` response advertises
     `capabilities.experimental = { "claude/channel": {} }`,
   - logs **every** raw JSON-RPC frame (both directions, verbatim, before any
     parsing) to a file with a millisecond timestamp and a direction marker
     (`>>` client→server, `<<` server→client).
2. From a real interactive terminal run:

   ```
   claude --dangerously-load-development-channels server:/abs/path/to/tee-server
   ```

3. Let the session reach idle. From a *second* peer/process, trigger whatever
   path is expected to push-wake the session (in peerbus this will be a broker
   `deliver`). Capture the resulting `notifications/claude/channel` frame.
4. In the same session, exercise the reply path (the tool/notification Claude
   uses to answer) and capture those frames too.
5. Copy the verbatim frames into section (c) below, replacing the PROVISIONAL
   blocks, and flip this document's status to `VERIFIED <date> <claude version>`.
6. Re-enable Task 11 and the cc rows of Task 12; reconcile
   `internal/channel/handshake_notes.go` against the captured truth.

### What specifically must be captured

- The full `initialize` request params (`protocolVersion`, client
  `capabilities`, `clientInfo`).
- The full `initialize` result, especially the exact key path and value used to
  advertise the channel capability (`experimental` vs a dedicated namespace; the
  exact key string `claude/channel`; whether the value object is empty `{}` or
  carries options).
- The `notifications/claude/channel` push: method name (exact), `params` shape,
  how message **content** is carried, and how a **reply affordance** is surfaced
  (a tool name to call back? a notification to emit? a correlation/turn id?).
- Any per-turn correlation id that a reply must echo.
- Error / unsupported-capability behavior if the client does not accept the
  capability.

## (b) Why automated capture is not possible here

`claude --dangerously-load-development-channels` requires an **interactive
Claude Code session** (a real REPL/turn loop with a human-attended terminal).
This spike runs inside a **non-interactive subagent** with no ability to:

- launch or attach to an interactive `claude` process,
- drive its turn loop,
- observe a live push-wake.

Therefore the only honest output is a clearly-labeled provisional schema plus
the exact procedure (above) for a human operator to finish the spike later.
This matches the plan's explicit Task 1 blocker clause, which anticipates this
outcome and prescribes the generic-only reduced plan — the bus is fully useful
generic-only; cc push-wake is deferred, the plan is not dead-ended.

## (c) PROVISIONAL reconstructed schema

> **PROVISIONAL — NOT CAPTURED FROM A LIVE SESSION.** Reconstructed from public
> Claude Code "channels" research-preview notes and the general shape of MCP
> `initialize` + JSON-RPC `notifications/*`. Field names, nesting, and the reply
> path are best-effort guesses and are very likely wrong in detail. Do not rely
> on these for the cc adapter without completing the capture in (a).

### Initialize request (client → server), provisional

```json
{
  "jsonrpc": "2.0",
  "id": 0,
  "method": "initialize",
  "params": {
    "protocolVersion": "2025-06-18",
    "capabilities": {
      "experimental": {
        "claude/channel": {}
      }
    },
    "clientInfo": {
      "name": "claude-code",
      "version": "2.1.80"
    }
  }
}
```

### Initialize result (server → client), provisional

The server advertises the channel capability so Claude treats it as a
push-capable channel:

```json
{
  "jsonrpc": "2.0",
  "id": 0,
  "result": {
    "protocolVersion": "2025-06-18",
    "capabilities": {
      "experimental": {
        "claude/channel": {}
      },
      "tools": {}
    },
    "serverInfo": {
      "name": "peerbus-adapter",
      "version": "0.0.0"
    }
  }
}
```

### Push-wake notification (server → client), provisional

The server pushes this to wake an idle session and create a turn. Provisional
guess: a JSON-RPC notification (no `id`) named `notifications/claude/channel`
with a free-form `params` object carrying the message payload and a
correlation id for the reply.

```json
{
  "jsonrpc": "2.0",
  "method": "notifications/claude/channel",
  "params": {
    "channelId": "peerbus",
    "turnId": "01J9Z...ULID",
    "content": [
      { "type": "text", "text": "message body surfaced to the session" }
    ],
    "metadata": {
      "from": "peer-name",
      "source": "peer-bus"
    }
  }
}
```

### Reply path, provisional

Provisional assumption: the session replies by **calling an MCP tool** the
server exposes (e.g. `bus.send` / `bus.broadcast`) — i.e. the reply path is an
ordinary `tools/call`, NOT a special channel notification — and the server
correlates it back via `turnId` if needed. This is the lowest-risk assumption
and aligns with peerbus's design (`bus.*` tools for outbound). **Unverified.**

### Malformed sample (for the error-path test)

A push frame missing the required `method` field — must fail to parse as a
valid push notification:

```json
{
  "jsonrpc": "2.0",
  "params": { "channelId": "peerbus" }
}
```

## Open questions to resolve at capture time

- Is the capability under `capabilities.experimental` or a top-level
  `capabilities["claude/channel"]`? (Plan says `experimental: { "claude/channel": {} }`.)
- Exact notification method string and whether content is `content[]` blocks
  (MCP-style) or a flat string.
- Is there a turn/correlation id the reply must echo, or is reply purely a
  fire-and-forget tool call?
- Behavior when the client declines the capability (hard error vs silent
  no-push fallback).
