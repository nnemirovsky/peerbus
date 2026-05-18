# peerbus

An agent-agnostic message bus plus thin adapters that let heterogeneous AI
agents send and broadcast messages to each other through one broker.

One long-lived broker holds a durable, at-least-once queue; any number of
short-lived adapter processes connect to it on behalf of an agent runtime
(Claude Code, Hermes, OpenClaw, Codex CLI, a bespoke bot). Messages are
peer-to-peer and out-of-band: peerbus moves messages *between already-running
interactive agents*, it never drives one agent from another.

## Honest taxonomy

This is a **custom MCP-channel peer bus**. It is *conceptually* A2A-shaped —
peer agents, asynchronous messages, and human escalation handled by the peer
itself rather than by the bus — but it is **not** an implementation of Zed's
[Agent Client Protocol](https://github.com/zed-industries/agent-client-protocol)
nor of the Google / Linux Foundation
[Agent2Agent (A2A)](https://github.com/a2aproject/A2A) specification. peerbus
defines and implements its own small WebSocket wire protocol (see
[`docs/wire-protocol.md`](docs/wire-protocol.md)); it borrows the *shape* of
A2A-style peer messaging but ships none of those specs' types, handshakes, or
guarantees. State this plainly when describing the project: peerbus is its own
bus, not an ACP/A2A implementation.

## Architecture

Two parts: a **broker** and **adapters**.

```
                          managed long-lived service
                        +---------------------------+
                        |        peerbus-broker     |
                        |  WS server + token auth   |
                        |  durable SQLite queue     |
                        |  blake3 audit hash-chain  |
                        +---------------------------+
                            ^        ^         ^
                  WebSocket |        |         | WebSocket
                            |        |         |
              +-------------+        |         +-------------+
              |                      |                       |
   +----------------------+  +----------------+   +----------------------+
   | peerbus-adapter      |  | peerbus-adapter|   | peerbus-adapter      |
   |   --adapter=cc       |  |  --adapter=    |   |   --adapter=generic  |
   | (claude/channel MCP  |  |   generic      |   | (stdio MCP server)   |
   |  server, push-wake)  |  | (stdio MCP)    |   |                      |
   +----------------------+  +----------------+   +----------------------+
       spawned per Claude       spawned per          spawned per
       Code session             drain-agent          drain-agent
```

- **Broker** (`cmd/peerbus-broker`): a single, long-lived, **managed**
  service — operated under compose / s6, **never** spawned per session. It is
  100% agent-agnostic (zero per-agent code): a WebSocket server with static
  bearer-token auth, a durable SQLite queue (`modernc.org/sqlite`, pure-Go,
  WAL), and a blake3 hash-chain audit log. It owns delivery semantics
  (at-least-once, dedupe-by-id, per-sender FIFO, broadcast fan-out with no
  backfill) and outlives every adapter.
- **Adapters** (`cmd/peerbus-adapter --adapter=<mode>`): thin, mostly
  ephemeral processes whose lifecycle is owned by each agent runtime. One Go
  binary, mode selected by `--adapter`; the broker never knows the mode.
  - `--adapter=cc` — *is* the MCP `claude/channel` server, spawned per Claude
    Code session over stdio. Inbound arrives as a `claude/channel` **push-wake**
    that creates a turn in an idle session (no polling). Outbound is the MCP
    tools `bus.send` / `bus.broadcast` / `bus.peers`. N sessions ⇒ N
    short-lived adapters, each a distinct peer; the adapter dies with its
    stdio session while the broker lives on.
  - `--adapter=generic` — a plain stdio MCP server, spawned per drain-agent.
    Tools: `bus.send` / `bus.broadcast` / `bus.peers` / `bus.drain`. There is
    **no push**; the host agent calls `bus.drain` on its own schedule (a
    timer, an idle hook, the top of each turn — host policy).

The broker/adapter split is deliberate: the broker must outlive sessions to
hold the durable queue, while a per-session adapter must die with its session.
Conflating the two is exactly the orphaned per-session-server bug the split
designs out.

The "only escalate to the human when a real decision is needed" policy lives
entirely in the *consuming agent's* prompt (keyed off the `source` tag in the
message envelope, e.g. `source: peer-bus`) — **never** in peerbus. peerbus is a
role-neutral transport.

## Delivery guarantees

- **Durable, at-least-once delivery.** A message is persisted before any
  delivery attempt; an offline recipient's messages queue in SQLite and are
  flushed on its next reconnect/drain. Unacked messages are redelivered on
  reconnect.
- **Dedupe by message id.** Because delivery is at-least-once and reconnect
  triggers redelivery, duplicates are expected; every adapter runs a
  consumer-side seen-id cache that suppresses them, so the host sees each id
  once.
- **Per-sender FIFO.** Messages from a given sender are delivered in send
  order (a monotonic per-sender sequence). There is no global ordering across
  senders.
- **Broadcast fan-out, no backfill.** `to:*` fans out to the peers registered
  *at send time* except the sender; each recipient gets its own durable copy
  and acks independently. A peer that registers *after* a broadcast does not
  receive it.

## Security model

- **Per-connection bearer-token auth.** A peer name is bindable only under a
  valid static bearer token (broker config/env). A duplicate-name claim under
  the *same* token is a takeover (old connection closed); under a *different*
  token it is rejected.
- **End-to-end HMAC for direct messages.** Direct (`to:<name>`) messages carry
  an HMAC-SHA256 over the canonical envelope, computed with a shared secret
  distributed to peers out-of-band. The recipient reconstructs the canonical
  form from the received wire bytes and verifies before surfacing the message,
  so a compromised broker **cannot forge or tamper with a direct message**
  undetected. Direct-message integrity is genuinely end-to-end.
- **Broadcast integrity is broker-trusted, NOT end-to-end (known limitation).**
  For `to:*` the broker rewrites per-recipient envelope fields (it gives each
  copy its own id and concrete `to` so the copies are independently dedupable
  and ackable). Because a recipient verifies the HMAC over the *received* wire
  bytes — which now carry broker-rewritten fields — the original sender's HMAC
  no longer verifies on broadcast copies. Adapters log-and-skip the failed
  verification rather than reject the message. **Consequence: broadcast
  integrity depends on trusting the broker; only direct messages are
  end-to-end HMAC-protected.** This is a real limitation, stated honestly
  here; it is tracked for a future broker-side fix.
- **blake3 audit chain.** The broker appends an audit row for every
  send/deliver/ack; each row's hash is `blake3(prev_hash || canonical_event)`
  (genesis `blake3("")`). A single serialized writer keeps the chain well
  defined. `peerbus-broker audit verify` walks the chain and reports the first
  break — tamper-evident, append-only.

## Quickstart

### 1. Run the broker (managed, long-lived)

The broker is operated as a managed service. The shipped compose manifest runs
**only the broker** (never a per-session process):

```sh
# Provision the bearer token(s) and HMAC secret out-of-band, then:
PEERBUS_TOKENS=<token>            \
PEERBUS_HMAC_SECRET=<shared-secret> \
docker compose -f deploy/compose.yml up -d
```

Broker configuration (struct defaults, overridden by env):

| Env var               | Meaning                                                            |
| --------------------- | ------------------------------------------------------------------ |
| `PEERBUS_LISTEN`      | WS server bind address (`host:port`, default `127.0.0.1:8080`).    |
| `PEERBUS_TOKENS`      | Comma-separated accepted static bearer tokens (at least one).      |
| `PEERBUS_HMAC_SECRET` | Shared end-to-end HMAC-SHA256 secret (min length enforced).        |
| `PEERBUS_DB`          | Durable SQLite store path (default `peerbus.db`).                  |

Running directly instead of compose:

```sh
go build -o peerbus-broker ./cmd/peerbus-broker
PEERBUS_TOKENS=... PEERBUS_HMAC_SECRET=... ./peerbus-broker serve
./peerbus-broker audit verify   # walk the blake3 audit chain
```

`deploy/peerbus-broker.run` (s6) and `deploy/Dockerfile` (pure-Go static
image) are alternatives to compose. Do **not** run the broker per session.

### 2. Wire an adapter

Build the adapter once:

```sh
go build -o peerbus-adapter ./cmd/peerbus-adapter
```

**Generic agents (Hermes, OpenClaw, Codex CLI, bots)** register
`peerbus-adapter --adapter=generic` as a stdio MCP server. Example
`.mcp.json`:

```json
{
  "mcpServers": {
    "peerbus": {
      "command": "peerbus-adapter",
      "args": ["--adapter=generic"],
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

Tools: `bus.send` (direct), `bus.broadcast` (fan-out), `bus.peers` (list),
`bus.drain` (return + ack pending — the host calls this on its own schedule).
Full guide: [`docs/integrations/generic-adapter.md`](docs/integrations/generic-adapter.md).
Recommended timed self-drain + escalation pattern for Hermes:
[`docs/integrations/hermes-drain-skill.md`](docs/integrations/hermes-drain-skill.md).

**An interactive Claude Code session** uses `--adapter=cc` instead. It is the
MCP `claude/channel` server; inbound is a push-wake that creates a turn in an
idle session (no `bus.drain`). Launch:

```sh
claude --dangerously-load-development-channels server:peerbus-adapter --adapter=cc
```

(env: `PEERBUS_URL` / `PEERBUS_NAME` / `PEERBUS_TOKEN` / `PEERBUS_HMAC_SECRET`,
same as generic.) Manual end-to-end checklist:
[`docs/manual-e2e-claude-channel.md`](docs/manual-e2e-claude-channel.md).

## Write your own adapter

The broker speaks one small, language-neutral WebSocket protocol. Anyone can
implement an adapter in any language from
[`docs/wire-protocol.md`](docs/wire-protocol.md) alone — the register / ack /
peers / deliver control frames, the message envelope schema, the
newline-delimited JSON framing, the HMAC canonicalization rules, token auth,
and the at-least-once / dedupe / FIFO / no-backfill semantics are all
specified there without reference to the Go implementation.

## Inspired by / credit

- [`non4me/cc2cc`](https://github.com/non4me/cc2cc)
  ([README](https://github.com/non4me/cc2cc#readme)) — the **direct
  inspiration**. cc2cc pioneered Claude-Code-to-Claude-Code messaging
  (auto-registered unique names, peer discovery, direct + broadcast, HMAC
  signing, offline persistence, channel push-wake). peerbus is its
  generalized successor: one durable, cross-machine broker that subsumes
  cc2cc entirely and extends the same ergonomics to *heterogeneous*,
  non-Claude agents via adapters.
- [`louislva/claude-peers-mcp`](https://github.com/louislva/claude-peers-mcp)
  — prior art for **broker-backed Claude peer messaging over channels**;
  validated the broker + MCP-channel approach this project builds on.

## License

[MIT](LICENSE) © 2026 Nikita Nemirovsky.
