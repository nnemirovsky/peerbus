# CLAUDE.md — contributor / AI orientation

Knowledge base for working in this repo. Everything here is derived from the
current tree (code, `Makefile`, `cmd/`, `docs/`). When code and prose
disagree, the code wins — keep this file accurate.

## What this is

peerbus is an **agent-agnostic, durable message bus**: one long-lived
WebSocket broker holding an at-least-once SQLite queue, plus thin per-runtime
adapter processes that connect to it on behalf of an agent (Claude Code,
Hermes, Codex CLI, a bot). The broker is 100% agent-agnostic and **managed,
long-lived** (compose/s6) — never spawned per session. Adapters are thin and
mostly ephemeral, lifecycle owned by the host runtime: `--adapter=cc` *is* the
MCP `claude/channel` server (inbound = push-wake that creates a turn in an
idle Claude session; outbound = `bus.send`/`bus.broadcast`/`bus.peers`);
`--adapter=generic` is a plain stdio MCP server with no push — the host calls
`bus.drain` on its own schedule. Do not duplicate the protocol or rationale
here; read [`README.md`](README.md),
[`docs/wire-protocol.md`](docs/wire-protocol.md), and
[`docs/cc2cc-parity.md`](docs/cc2cc-parity.md).

## Architecture map

- `cmd/peerbus-broker` — broker CLI; subcommands `serve` and `audit verify`.
- `cmd/peerbus-adapter` — adapter CLI; `--adapter=<mode>` dispatch, env-driven.
- `internal/wire` — envelope + control frame types, codec, canonical HMAC
  form (load-bearing; see below).
- `internal/hmac` — HMAC-SHA256 sign/verify over the canonical form;
  `MinSecretLen = 32`.
- `internal/store` — durable pure-Go SQLite (`modernc.org/sqlite`, WAL)
  queue: at-least-once, dedupe-by-id, per-sender FIFO.
- `internal/audit` — append-only blake3 hash-chain audit log
  (`hash = blake3(prev_hash || canonical_event)`).
- `internal/broker` — WebSocket server, token auth, peer registry, routing
  (direct/broadcast/ack/redelivery), config from `PEERBUS_*` env.
- `internal/adapter` — broker-facing client, reconnect/resume, mandatory
  consumer dedupe cache, cc + generic mode implementations, mode registry.
- `internal/mcp` — stdio MCP server exposing `bus.*` tools for the generic
  adapter.
- `internal/channel` — Claude Code `claude/channel` MCP capability for the cc
  adapter; wire shapes mirror `CHANNELS_SCHEMA.md`.
- `internal/version` — build version string.
- `internal/integration` — in-process cross-package + cc2cc-parity tests.

## Build / test / run

```sh
make build            # builds dist/peerbus-broker and dist/peerbus-adapter
make test             # go test ./...
make lint             # golangci-lint if present, else gofmt -l + go vet
make deploy-validate  # docker compose config -q (skips+succeeds if no docker)
go test ./...
go test -race ./...   # CI runs this; must be clean
```

CI (`.github/workflows/ci.yml`) pins **Go 1.25** (= the `go` directive in
`go.mod`, `go 1.25.0`) and runs gofmt check, `go vet`, build, `go test ./...`,
then `go test -race ./...`. `modernc.org/sqlite` is pure-Go — **no cgo**, no C
toolchain required.

Broker:

```sh
PEERBUS_LISTEN=0.0.0.0:8080 PEERBUS_TOKENS=t1,t2 PEERBUS_HMAC_SECRET=... \
  ./peerbus-broker serve
./peerbus-broker [--db PATH] audit verify   # walks the blake3 chain
```

`serve` config loads from env: `PEERBUS_LISTEN` (default `127.0.0.1:8080`),
`PEERBUS_TOKENS` (comma-separated, ≥1 required), `PEERBUS_HMAC_SECRET`
(≥ `hmac.MinSecretLen` = 32 bytes), `PEERBUS_DB` (default `peerbus.db`; the
`--db` flag sets the base, env overrides). Missing tokens or a short secret
is a fatal config error.

Adapter:

```sh
peerbus-adapter --adapter=generic   # or --adapter=cc
```

Env: `PEERBUS_URL`, `PEERBUS_NAME`, `PEERBUS_TOKEN`, `PEERBUS_HMAC_SECRET`.
Fail-fast (exit 2): missing `PEERBUS_URL`, missing `PEERBUS_TOKEN`,
`PEERBUS_HMAC_SECRET` shorter than `hmac.MinSecretLen` (32), or empty
`PEERBUS_NAME` when `--adapter=generic` (cc auto-mints
`cc-<host>-<pid>-<rand>` when name is empty). Missing/unknown `--adapter`
is also exit 2.

## Load-bearing invariants — do NOT break

- **HMAC canonical form** (`internal/wire` codec): the signed bytes are a
  fixed-field-order subset (`protocol_version, id, from, to, ts, source,
  kind, body`) with `hmac` omitted and `body` spliced in **verbatim** as a
  `json.RawMessage` — never decoded and re-marshalled (re-marshalling
  reorders keys and breaks cross-language verification). Every field always
  present, no omitempty. Absent body ⇒ literal `null`.
- **Dedupe by signed `envelope.id`**; **ack by `wire.Deliver.DeliveryKey`**,
  never by `envelope.id`.
- The broker **MUST** set `DeliveryKey` on every deliver frame: direct =
  `envelope.id`; broadcast = `"<original-id>|<recipient-name>"`. Empty/absent
  `DeliveryKey` is a protocol error — the client drops without acking.
- The broker **never mutates the signed envelope**. Broadcast copies carry
  the sender's verbatim signed bytes; broadcast integrity is end-to-end HMAC.
  The per-recipient key rides on `DeliveryKey`, outside the HMAC subset.
- **Audit hash-chain needs a single serialized writer** — one broker-owned
  `audit.Appender`. Do not introduce a second concurrent audit writer.
- **Exact-match protocol version** (`"v1"`), no negotiation.
- **Broadcast: no backfill** — recipients snapshotted at send time.
- The broker is a **managed long-lived service, never per-session**. The
  broker/adapter split exists to design out the cc2cc orphaned
  per-session-server bug; do not collapse it.

## Testing conventions

- Table-driven tests; every package has tests; all must pass including
  `go test -race ./...` before a change lands.
- Broker/adapter tests use an in-process `httptest` broker plus a
  `coder/websocket` client harness.
- The `--adapter` mode registry (`internal/adapter/mode.go`) is a **package
  global**. Tests that `Register` a mode MUST restore state with
  `unregisterMode(name)` via `t.Cleanup` (see `client_test.go`,
  `parity_test.go`).

## Commit convention

Scoped Conventional Commits, **lowercase** description, matching existing
history: `feat(...)`, `fix:`, `refactor:`, `docs:`.

## Claude channel schema (authoritative source)

The authoritative `claude/channel` wire schema is
[`CHANNELS_SCHEMA.md`](CHANNELS_SCHEMA.md), summarized for the cc adapter in
[`docs/spikes/claude-channel-handshake.md`](docs/spikes/claude-channel-handshake.md).
Real interactive-Claude end-to-end verification is Post-Completion / manual
only — see
[`docs/manual-e2e-claude-channel.md`](docs/manual-e2e-claude-channel.md); the
schema itself is fully covered by automated tests
(`internal/channel/channel_test.go`).
