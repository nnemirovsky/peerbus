# cc2cc → peerbus parity matrix

This is the validation matrix proving peerbus does not regress any of the
[`non4me/cc2cc`](https://github.com/non4me/cc2cc) launch/ergonomics behaviors
called out in the plan's discovery context. Each row states the cc2cc
behavior, the peerbus mechanism that subsumes it, and the **exact test in
`internal/integration/parity_test.go`** that proves it.

The full plan (Task 1 `✅ RESOLVED`) is in force: the cc adapter (Task 11) is
built and the `cc` parity rows (auto-register/unique-name, push-wake) are
**in scope**. The generic-only reduced variant was rescinded.

| # | Parity row | cc2cc behavior | peerbus mechanism | Proving test(s) |
|---|------------|----------------|-------------------|-----------------|
| 1 | Auto-register / unique name | each Claude session auto-claims a unique peer name on launch | broker `register` binds a unique name under a bearer token; the cc adapter mints `cc-<host>-<pid>-<rand>` via `channel.UniqueName` when no name is configured; duplicate-name claim under the same token is a takeover, under a different token is rejected | `TestParity_AutoRegisterUniqueName`, `TestParity_CCAutoRegisterUniqueName` |
| 2 | Peer discovery | a session can list the other live peers | broker `peers` control frame returns the current registry; exposed to agents as the `bus.peers` MCP tool | `TestParity_PeerDiscovery` |
| 3 | Direct message | session A messages session B by name | `send` to `<name>`: durable enqueue + deliver-if-connected else queue; HMAC-signed end-to-end | `TestParity_DirectMessage` |
| 4 | Broadcast | a session fans a message out to all other sessions | `to:*` broadcast fans out to every currently-registered peer **except the sender**, one durable copy + own ack each; no late-joiner backfill | `TestParity_BroadcastFanOutSenderExcluded` |
| 5 | HMAC signing / trust | messages are HMAC-SHA256 signed; recipients reject forged ones | per-message HMAC over the canonical envelope carried end-to-end; recipient verifies before surfacing; a compromised broker cannot forge a peer | `TestParity_HMACSignedDeliversAndForgedRejected` |
| 6 | Offline persistence + delivery on next session | a message to an offline peer is delivered when it next comes online | offline recipients' messages persist in SQLite (`delivered=0`); flushed on register/next drain; unacked redeliver on reconnect | `TestParity_OfflinePersistenceThenDrain` |
| 7 | Push-wake (cc) | an inbound message wakes an idle Claude session and creates a turn | the cc adapter is the `claude/channel` MCP server; a broker `deliver` maps to exactly one `notifications/claude/channel` JSON-RPC notification (`{content, meta:{from,source,msg_id}}`) — the in-process automatable proxy for push-wake | `TestParity_CCPushWakeNotification` (in-process proxy) + real interactive `claude` session confirmation: see [`docs/manual-e2e-claude-channel.md`](manual-e2e-claude-channel.md) (Post-Completion, not automatable) |

## Additional integration coverage (delivery-model correctness)

| Property | Why it matters | Proving test |
|----------|----------------|--------------|
| Bad-token register rejected | token auth gate; a peer name is bindable only under a valid token | `TestParity_BadTokenRegisterRejected` |
| Dedupe on redelivery | at-least-once + reconnect ⇒ duplicate redelivery is expected; the shared consumer dedupe must surface each id to the host exactly once | `TestParity_DedupeOnRedelivery` |

## Push-wake: in-process proxy vs. real session

The `claude/channel` push-wake cannot be unit-tested without a real
interactive `claude --dangerously-load-development-channels` session (no
non-interactive agent can launch one). `TestParity_CCPushWakeNotification`
asserts the **automatable proxy**: a broker `deliver` to a `cc` adapter
produces exactly one well-formed `notifications/claude/channel` notification
with the correct `content` + `meta`. That notification *is* the wire signal
Claude Code consumes to wake an idle session. The final go/no-go for the cc
adapter — confirming the notification actually creates a turn in a live idle
session and that `bus.*` replies work — is the manual checklist in
[`docs/manual-e2e-claude-channel.md`](manual-e2e-claude-channel.md)
(Post-Completion, documented, not automated).
