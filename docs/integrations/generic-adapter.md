# Integration: the generic MCP adapter (`peerbus adapter --adapter=generic`)

> **v0.2.0 rename.** v0.1.0 invoked this as `peerbus-adapter
> --adapter=generic`; v0.2.0 ships ONE multi-command `peerbus` binary, so the
> invocation is now `peerbus adapter --adapter=generic` (`adapter` is a
> subcommand; `--adapter=<mode>` is its flag). Flags, env vars, and behaviour
> are otherwise unchanged.

How any agent runtime — Hermes, OpenClaw, Codex CLI, a bespoke bot — joins
the peerbus fabric. This is the universal path: every agent except a real
interactive Claude Code session uses it. (Claude Code has its own push-wake
variant — see [the cc variant](#the-cc-variant) below.)

## Model

`peerbus adapter --adapter=generic` is a **plain stdio MCP server**. The host
agent spawns it as a child process and speaks JSON-RPC 2.0 over the child's
stdin/stdout. The adapter dials the long-lived broker over WebSocket, signs
and verifies every message end-to-end with the shared HMAC secret, and
exposes four MCP tools.

Critically: **there is no push.** The generic adapter never interrupts the
host. Inbound messages sit in the broker's durable queue until the host
*chooses* to call `bus.drain`. The host owns its own drain schedule. (Push
-wake is a Claude-Code-only capability and lives in the `cc` variant.)

Lifecycle: the adapter process is owned by the host agent runtime — one
adapter child per drain-agent, spawned and reaped by that runtime. The broker
is a separate, managed, long-lived service that outlives every adapter (see
`deploy/compose.yml` / `deploy/peerbus-broker.run`). Do **not** run the broker
per session — that is the cc2cc orphaned-`server.mjs` failure mode the
broker/adapter split exists to prevent.

## Wiring it as an MCP server (`.mcp.json`)

Register the adapter as a stdio MCP server in the host's MCP config. Shape:

```json
{
  "mcpServers": {
    "peerbus": {
      "command": "peerbus",
      "args": ["adapter", "--adapter=generic"],
      "env": {
        "PEERBUS_URL": "ws://broker-host:8080",
        "PEERBUS_NAME": "hermes-prod",
        "PEERBUS_TOKEN": "<static bearer token>",
        "PEERBUS_HMAC_SECRET": "<shared end-to-end HMAC secret>"
      }
    }
  }
}
```

Environment the adapter consumes:

| Variable              | Meaning                                                                 |
| --------------------- | ----------------------------------------------------------------------- |
| `PEERBUS_URL`         | Broker WebSocket URL (`ws://host:port`), the managed broker service.    |
| `PEERBUS_NAME`        | This peer's unique name on the bus (`to:<name>` addresses it). Stable.  |
| `PEERBUS_TOKEN`       | One of the broker's static bearer tokens (provisioned out-of-band).     |
| `PEERBUS_HMAC_SECRET` | Shared HMAC-SHA256 secret; signs outbound, verifies inbound end-to-end. |

The token and HMAC secret are provisioned **out-of-band** (a secret store,
not committed to the repo or the MCP config in git). The HMAC secret must
meet the broker's minimum length or registration fails. The name must be
unique on the bus: a duplicate-name claim under the same token is a takeover
(the older connection is closed); under a different token it is rejected.

## Tool surface

The generic adapter advertises exactly four tools:

| Tool            | Arguments                | Effect                                                                                          |
| --------------- | ------------------------ | ----------------------------------------------------------------------------------------------- |
| `bus.send`      | `to` (string), `body` (object) | Direct message to one peer. Body is opaque application JSON, hashed verbatim.              |
| `bus.broadcast` | `body` (object)          | Fan-out to every currently-registered peer except yourself. No backfill for late joiners.       |
| `bus.peers`     | —                        | List the peers currently registered on the bus.                                                 |
| `bus.drain`     | —                        | Return **and acknowledge** every message received since the last drain.                         |

`bus.drain` is the entire inbound path for a generic peer. It returns each
pending message with its `from` and `source` (e.g. `source: peer-bus`),
filtered through the adapter's consumer-side dedupe cache (at-least-once
delivery plus reconnect redelivery means duplicates are expected and
suppressed), HMAC-verified before being surfaced, then acked so the broker
stops redelivering. The host calls `bus.drain` **on its own schedule** — a
timer, an idle hook, the top of each turn, whatever the host's policy is.
peerbus does not dictate that cadence; see
[`hermes-drain-skill.md`](./hermes-drain-skill.md) for the recommended
pattern and the escalation-policy guidance.

Outbound (`bus.send` / `bus.broadcast`) signs with the HMAC secret so
recipients can verify integrity independently of broker trust — a compromised
broker cannot forge a peer.

## The cc variant

A real interactive **Claude Code** session uses `peerbus adapter --adapter=cc`
instead. That variant *is* the MCP `claude/channel` server: inbound arrives as
a `notifications/claude/channel` **push-wake** that creates a turn in an idle
session — no `bus.drain`, no polling. Outbound is the same `bus.send` /
`bus.broadcast` / `bus.peers` surface (no `bus.drain`, since cc is
push-driven). Claude Code spawns one short-lived `cc` adapter per session over
stdio. For the launch flags and the full manual end-to-end checklist, see
[`../manual-e2e-claude-channel.md`](../manual-e2e-claude-channel.md).

Both variants share one broker and the same end-to-end HMAC trust model;
heterogeneous peers (some `generic`, some `cc`) interoperate transparently.
