# Manual e2e: cc channel adapter (claude/channel push-wake)

> **Post-Completion — manual, NOT automated.** This is the only path that
> cannot be unit-tested: it needs a real interactive Claude Code session and
> a human-attended terminal to observe an idle session actually waking. The
> wire schema itself is fully covered by automated tests (see
> `internal/channel/channel_test.go`) against the DOCUMENTED schema in
> `CHANNELS_SCHEMA.md`; this checklist verifies the live behaviour
> end-to-end. Running it is the final go/no-go for the cc adapter.

## Prerequisites

- A broker reachable from this machine, running as a managed long-lived
  service (NOT per-session). Note its `ws://host:port`, bearer token, and
  HMAC secret (provisioned out-of-band).
- `peerbus-adapter` built (`make build`).
- A second peer to send from — use the generic adapter
  (`peerbus-adapter --adapter=generic`) wired into any agent, or a
  throwaway test client.
- Claude Code v2.1.80+ (channels research preview;
  `--dangerously-load-development-channels` available).

## 1. Register the cc adapter in `.mcp.json`

In the project where the Claude session will run:

```json
{
  "mcpServers": {
    "peerbus": {
      "command": "/abs/path/to/peerbus-adapter",
      "args": ["--adapter=cc"],
      "env": {
        "PEERBUS_URL": "ws://BROKER_HOST:PORT",
        "PEERBUS_TOKEN": "THE_BEARER_TOKEN",
        "PEERBUS_HMAC_SECRET": "THE_SHARED_SECRET"
      }
    }
  }
}
```

(Use the env/flag names the binary actually reads — confirm against
`cmd/peerbus-adapter`.) Leave the peer name unset to exercise
auto-registration; the adapter mints `cc-<hostname>-<pid>-<rand>` (see
`channel.UniqueName`).

## 2. Launch the real Claude session as a channel

From a real, human-attended terminal:

```bash
claude --dangerously-load-development-channels server:peerbus
```

Confirm at startup that Claude Code loaded the channel (no capability
error). The adapter advertises
`capabilities.experimental["claude/channel"]={}` and `tools={}`; Claude Code
registers the notification listener and discovers `bus.send`,
`bus.broadcast`, `bus.peers`.

## 3. Let the session go idle

Do not type anything. Wait until the session is idle (no active turn).

## 4. From the second peer, send a direct message

Using the generic adapter / test client registered under a different name,
send a direct message addressed to the cc adapter's peer name (read it from
the cc adapter's stderr log line, or call `bus.peers` from the second peer):

```
bus.send  to=<cc-adapter-peer-name>  body={"text":"ping from peer 2"}
```

## 5. Confirm the idle session WAKES

Expected: the idle Claude session wakes and a new turn is created
containing a `<channel>` XML block, e.g.:

```xml
<channel source="peer-bus" from="<peer-2-name>" msg_id="<id>">
ping from peer 2
</channel>
```

- `content` is the message body as text.
- `meta` becomes `<channel>` attributes: `from`, `source` (the envelope
  `source`, `peer-bus`), `msg_id`.

If the session does NOT wake: verify the broker delivered the frame (broker
audit log), that the adapter registered (its stderr), and that
`channelsEnabled` org policy is not blocking (CHANNELS_SCHEMA.md §6 —
silently dropped if disabled).

## 6. Confirm the reply path reaches the second peer

In the woken Claude session, have Claude call `bus.send` back to peer 2:

```
bus.send  to=<peer-2-name>  body={"text":"pong from claude"}
```

Confirm peer 2 receives `pong from claude`. This proves the two-way path:
push-wake in, ordinary MCP tool call out (no special reply notification, no
turn/correlation id — CHANNELS_SCHEMA.md §4).

## 7. (Optional) broadcast sanity

From peer 2, `bus.broadcast` a message. The cc session should NOT wake from
its own broadcast copy if it was the sender; a broadcast from a *different*
peer is expected to be HMAC-rejected and SKIPPED (logged at debug, no
notification, no crash) — this is the known broker-rewrites-broadcast-id
limitation documented in `internal/adapter/cc.go`, identical to the generic
adapter, tracked for the review phase / Task 12. Direct messages are the
verified push-wake path.

## Pass criteria

- [ ] Channel loaded with no capability error.
- [ ] Idle session woke on the direct message (turn created, `<channel>`
      block with correct content + `from`/`source`/`msg_id` attributes).
- [ ] Claude's `bus.send` reached peer 2.
- [ ] No orphaned adapter process after the session exits (lifecycle bound
      to stdio — kill the Claude session, confirm the adapter exits).
