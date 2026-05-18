# peerbus wire protocol (v1)

This document fully specifies the peerbus broker WebSocket protocol so a third
party can write an adapter in **any language** without reading the Go source.
It is derived from `internal/wire` and `internal/broker`. Where this document
and the code disagree, the code is authoritative — but the intent is that they
do not disagree.

`protocol_version` is the string `"v1"` everywhere in this version.

## 1. Transport and framing

- The broker is a **WebSocket server**. An adapter (the "client" / "peer")
  dials it at a `ws://host:port` or `wss://host:port` URL.
- Every protocol message — every control frame and every message envelope —
  is sent as **one WebSocket text message containing exactly one JSON
  object**. WebSocket frames are already length-delimited, so over a live WS
  connection no extra framing is applied.
- **Newline-delimited JSON framing.** When the same shapes are written to a
  byte stream rather than a WS connection (e.g. a stdio/pipe transport or a
  test harness), each JSON object is written on its own line terminated by a
  single `\n`. A decoder reads one line, parses it as one JSON object. Lines up
  to 1 MiB are supported. JSON itself never contains a raw newline, so a line
  is always exactly one object. An adapter speaking raw WebSocket does **not**
  add the `\n`; the WS frame boundary already delimits the object. The two
  framings carry identical JSON object shapes.
- Non-text WS messages (binary, ping/pong) carry no protocol meaning and are
  ignored by both sides.
- The broker sets a read limit of 1 MiB per message; clients should do the
  same. Bodies are opaque application JSON — keep them within budget.

## 2. Control frames

Control frames are JSON objects discriminated by a `"type"` string field.
Every control frame also carries `"protocol_version"`. The four types:

### 2.1 `register` (client → broker, first frame, mandatory)

The very first frame on a new connection MUST be a `register`. Anything else
closes the connection.

```json
{
  "protocol_version": "v1",
  "type": "register",
  "token": "<static bearer token>",
  "name": "<unique peer name>"
}
```

- `token` — a static bearer token the broker was configured with. The HMAC
  secret is **never** sent on the wire; it is shared out-of-band.
- `name` — the unique peer name. `to:<name>` addresses this peer. Must be
  non-empty.

Broker behaviour on `register`:

1. If the frame is not text, not valid JSON, not a `register`, has an empty
   `name`, has a `protocol_version` other than `"v1"`, or presents an invalid
   `token`, the broker closes the WebSocket with a close status describing the
   reason and the connection ends. (Adapters should treat any handshake-time
   close as "rejected".)
2. **Name binding.** If `name` is free, it is bound to this connection. If
   `name` is already bound:
   - **same token** → *takeover*: the old connection is closed immediately,
     the new connection takes the name. Any message queued during the
     takeover window is not lost — it falls to the durable pending path and is
     delivered to the new connection.
   - **different token** → *reject*: the new connection is closed.
3. The peer is persisted durably, and its unacked messages are requeued.
4. The broker replies with a **`peers` frame as the handshake ack** (see 2.3)
   listing the currently-registered peer names. Receiving this frame confirms
   the register was accepted.
5. The broker then immediately delivers (as `deliver` frames, see 2.4) every
   message currently pending for this peer (offline queue + requeued unacked).

A client implementation: dial → send `register` → read one frame; if it is a
`peers` frame the register succeeded, otherwise (or on a read error / close)
the register was rejected. After that, deliver/peers frames stream in.

A re-sent `register` on an *already established* connection is ignored — to
re-register you open a fresh connection (reconnecting under the same name is
exactly what triggers the same-token takeover + pending flush).

### 2.2 `ack` (client → broker)

Acknowledges that the host has **consumed** the message with `id`. Until the
broker receives this ack the message stays unacked and WILL be redelivered on
the peer's next reconnect (the consumer dedupes — see §5).

```json
{ "protocol_version": "v1", "type": "ack", "id": "<message id>" }
```

- Send `ack` only **after** the host has actually consumed the message. This
  ordering is load-bearing for at-least-once.
- An `ack` for an unknown or already-acked id is a graceful no-op (a
  late/duplicate ack is expected under at-least-once).
- An empty `id` is ignored.

### 2.3 `peers` (client ↔ broker)

Request the registry (client sends with empty/absent `names`); the broker
replies with a `peers` frame whose `names` is the list of
currently-registered peer names. Also used as the **register handshake ack**
(§2.1 step 4).

Request:

```json
{ "protocol_version": "v1", "type": "peers" }
```

Reply:

```json
{ "protocol_version": "v1", "type": "peers", "names": ["alice", "bob"] }
```

A peers reply may arrive interleaved with `deliver` frames; a client that
multiplexes a single reader must route it by `type`.

### 2.4 `deliver` (broker → client only)

Wraps one message envelope being pushed to its recipient. Clients never send
`deliver`; the broker ignores it if a client does.

```json
{
  "protocol_version": "v1",
  "type": "deliver",
  "delivery_key": "<per-recipient durable row key>",
  "envelope": { ...see §3... }
}
```

`delivery_key` is the broker's per-recipient durable row key. It is carried
**outside the signed envelope** (it is NOT part of the HMAC canonical subset,
§4). For a **direct** message `delivery_key` equals the envelope `id`. For a
**broadcast** copy it is `"<original-id>|<recipient-name>"` while the
`envelope` stays **byte-identical to what the sender signed** (`to:"*"`,
original `id`, original `hmac`). The client **acks by `delivery_key`** (§2.2),
not by `envelope.id` — that is how each per-recipient broadcast copy is
independently ackable without mutating (and thus invalidating) the signed
envelope.

The broker **MUST** set `delivery_key` on every `deliver` frame: for a direct
message it is `envelope.id`; for a broadcast copy it is
`"<original-id>|<recipient-name>"`. A `deliver` frame with an empty or absent
`delivery_key` is a **protocol error**: the client **MUST drop the message
without acking** it — it cannot be acked, there is no row key to ack — exactly
as it drops an envelope that fails HMAC verification (§4).

On receipt the client MUST: HMAC-verify the envelope (§4) — which verifies
for broadcast too, because the envelope is the sender's verbatim signed bytes
— run it through the dedupe cache keyed on `envelope.id` (§5), surface it to
the host, then `ack` the `delivery_key` (§2.2) once the host has consumed it.

## 3. Message envelope

A message is a JSON object with **exactly these nine fields** (no field is
omitted; absent values are explicit):

| Field              | JSON type            | Meaning                                                                 |
| ------------------ | -------------------- | ----------------------------------------------------------------------- |
| `protocol_version` | string               | `"v1"`. Exact-match-or-reject (§6).                                      |
| `id`               | string               | Unique message id (ULID / UUIDv7 recommended). Dedupe key.              |
| `from`             | string               | Sender's peer name.                                                     |
| `to`               | string               | Recipient peer name, or `"*"` for broadcast.                            |
| `ts`               | string               | Timestamp (sender-supplied string, e.g. RFC 3339).                      |
| `source`           | string               | Provenance tag (e.g. `"peer-bus"`). Carried verbatim end-to-end.        |
| `kind`             | string               | `"msg"` (direct) or `"broadcast"` (fan-out).                            |
| `body`             | raw JSON             | Opaque application payload. **Hashed and carried verbatim** (§4).       |
| `hmac`             | string               | Hex-encoded HMAC-SHA256 over the canonical form (§4).                   |

Example direct message on the wire (inside a `deliver` frame's `envelope`, or
sent by a client as a bare data frame):

```json
{
  "protocol_version": "v1",
  "id": "01J9X8...",
  "from": "alice",
  "to": "bob",
  "ts": "2026-05-18T12:00:00Z",
  "source": "peer-bus",
  "kind": "msg",
  "body": {"text": "hello"},
  "hmac": "9f86d0818..."
}
```

### Sending a message (client → broker)

A client sends a message by writing the envelope JSON object **directly** as a
frame (it is not a control frame — it has no `type` field; the broker
classifies any frame that is not a recognised control type as an envelope).

- **Direct:** set `to` to the recipient's name and `kind` to `"msg"`.
- **Broadcast:** set `to` to `"*"` and `kind` to `"broadcast"`.

The broker requires a non-empty `id` and non-empty `to`; an envelope missing
either is dropped. The broker routes by the **connection's authenticated bound
name**, not by `from` (though `from` is still carried verbatim and is covered
by the HMAC).

### Broker routing

- **Direct (`to:<name>`):** the broker persists the message, then delivers it
  if the recipient is connected, else leaves it queued and delivers on the
  recipient's next register. A recipient name that was never a registered peer
  at all → dropped. A known-but-currently-offline peer → queued. A re-sent
  duplicate `id` → benign no-op (the original row stands).
- **Broadcast (`to:*`):** the broker fans out to every peer registered **at
  send time** except the sender. Each recipient gets its **own durable row**
  keyed `"<original-id>|<recipient-name>"`, so each copy is independently
  dedupable and ackable. The delivered/persisted **envelope is the sender's
  verbatim signed bytes** (`to:"*"`, original `id`, original `hmac`); the
  per-recipient row key is carried on the `deliver` frame's `delivery_key`
  (§2.4), **outside the HMAC**. **No backfill:** a peer that registers after
  the broadcast does not receive it. *HMAC:* because the broker never mutates
  the signed envelope, the sender's HMAC verifies on broadcast copies exactly
  as for direct messages — broadcast integrity is genuinely end-to-end (§4).

## 4. HMAC canonicalization (load-bearing)

The signature proves a message was not forged or tampered with by anyone —
including a compromised broker — for **direct messages**.

### Canonical form

The signed bytes are the JSON serialization of a **fixed-field-order subset**
of the envelope that **omits `hmac`**, with `body` spliced in **verbatim**.
The field order is exactly:

1. `protocol_version`
2. `id`
3. `from`
4. `to`
5. `ts`
6. `source`
7. `kind`
8. `body`

Rules an implementation MUST follow to be byte-compatible:

- Serialize these eight fields **in this exact order**, as a single JSON
  object, with JSON keys `protocol_version`, `id`, `from`, `to`, `ts`,
  `source`, `kind`, `body` (this order, these names).
- **Every field is always present** — no field is omitted even if empty
  (there is no "omitempty"). Empty strings serialize as `""`.
- `body` is opaque JSON and is included **verbatim — never decoded and
  re-encoded.** Re-marshalling JSON (e.g. parsing into a map and serializing
  again) reorders object keys and is not byte-stable across languages, which
  would break cross-implementation verification. Splice the raw `body` bytes
  in as-is. (Insignificant whitespace *compaction* of the raw bytes is
  permitted because it is deterministic and idempotent and never reorders
  members; the Go reference uses `encoding/json`'s RawMessage compaction. The
  safest portable choice is to transmit and sign `body` with no insignificant
  whitespace so compaction is a no-op.)
- An **absent/empty `body`** canonicalizes as the JSON literal `null` (the
  `body` field is still present, with value `null`).

The canonical bytes are then HMAC-SHA256'd with the shared secret; `hmac` is
the lowercase hex encoding of the resulting digest.

### Sign / verify

- **Sender:** build the envelope, compute the canonical bytes, HMAC-SHA256
  them with the shared secret, hex-encode into `hmac`, send.
- **Recipient:** take the *received wire bytes*, parse the envelope,
  reconstruct the canonical form from the parsed fields (re-serialize the
  eight fields in order, splicing the received raw `body` verbatim),
  HMAC-SHA256 with the shared secret, and **constant-time compare** against
  the hex-decoded `hmac`. Reject (drop, do not surface) on mismatch, short, or
  missing secret.

Because the canonical form fixes field order and never re-encodes `body`, the
sender's bytes and the recipient's reconstructed bytes are identical across
machines and languages — that is the cross-machine guarantee.

### Broadcast is end-to-end too

For `to:*` the broker delivers the sender's **verbatim signed envelope** to
every recipient — it does **not** rewrite any signed field. The per-recipient
durable row key rides on the `deliver` frame's `delivery_key` (§2.4), which is
**outside the canonical subset** and therefore not covered by the HMAC. A
recipient reconstructs the canonical form from the received envelope bytes
(unchanged from the sender) and the HMAC verifies, exactly as for a direct
message. **Broadcast integrity is end-to-end: a compromised broker cannot
forge or tamper with a broadcast copy undetected.** An adapter MUST ack by
`delivery_key`, not `envelope.id`, so each per-recipient copy clears
independently.

## 5. Delivery semantics

An adapter MUST implement these to be correct:

- **At-least-once.** The broker persists before delivering; it redelivers
  unacked messages on the peer's reconnect. A message is delivered *at least*
  once, possibly more.
- **Dedupe by `id`.** Because reconnect causes redelivery, duplicates are
  expected. The client MUST keep a bounded consumer-side seen-`id` cache
  (LRU/ring of configurable size) keyed on the **signed `envelope.id`** and
  suppress an id it has already surfaced, so the host sees each id exactly
  once. Broadcast copies all carry the sender's original `envelope.id` (the
  broker no longer rewrites it); the per-recipient `delivery_key` is used for
  acking, not for dedupe.
- **Per-sender FIFO.** The broker delivers messages from a given sender in
  send order (a monotonic per-sender sequence). There is **no** global
  ordering across different senders.
- **Broadcast: no backfill.** Recipients are snapshotted at send time;
  late-registering peers never receive a past broadcast.
- **Ack-after-consume.** Send `ack` only after the host has consumed the
  message. Unacked ⇒ redelivered on next reconnect (then dedupe-suppressed).
- **`delivery_key` is mandatory and load-bearing for acking.** Every
  `deliver` frame carries a non-empty `delivery_key` (§2.4); the client acks
  by it. A `deliver` frame with an empty/absent `delivery_key` is a protocol
  error — the client drops it without acking (it has no row key to ack),
  exactly as it drops an HMAC-verify failure.

### Reconnect / resume

There is no seq cursor to track. On a dropped connection: redial → send
`register` with the **same name** (this triggers the broker's same-token
takeover and a flush of all pending + unacked messages for that name). Every
received message passes through the dedupe cache before being surfaced and is
acked only after the host consumed it. The broker redelivers everything
unacked; the client deduplicates.

## 6. Versioning and auth

- **`protocol_version` is exact-match-or-reject** at `register` and is present
  on every frame. The only accepted value in this version is `"v1"`. Any other
  value is rejected at register (the connection is closed). There is **no
  negotiation engine** in v1 — the field exists so future negotiation can be
  added additively.
- **Auth** is a static bearer token presented in the `register` frame's
  `token` field. The broker accepts a fixed set of tokens (its config/env). A
  peer name is bindable only under a valid token; same-name + same-token =
  takeover, same-name + different-token = reject. The HMAC secret is a
  separate shared secret, distributed out-of-band, and is **never transmitted
  on the wire**.

### Audit / persistence durability boundary (accepted)

The broker persists a message (`store.Enqueue`) and appends its `send` audit
row in **separate transactions**. A broker crash in the narrow window between
the two leaves a durably-queued message with **no `send` audit row**. This is
an accepted, documented boundary, **not** a delivery bug: the message is still
delivered at-least-once (the queue row committed), and the blake3 audit chain
**stays hash-valid** — it is append-only and never references the message row,
so it may simply omit a single `send` event for a message that was
nonetheless delivered. Coupling the audit hash-chain write (a separate
single-writer subsystem) into the store transaction was rejected as strictly
worse than this narrow gap. Audit `deliver`/`ack` events are unaffected.

## 7. Minimal adapter checklist

To write a conforming adapter in any language:

1. Open a WebSocket to the broker URL.
2. Send a `register` frame (`protocol_version:"v1"`, `type:"register"`,
   `token`, unique `name`).
3. Read one frame: a `peers` frame ⇒ accepted; a read error / close ⇒
   rejected.
4. Pump frames: for each `deliver`, verify HMAC over the canonical form
   reconstructed from the received `envelope` bytes (§4) — this verifies for
   broadcast too — dedupe by `envelope.id` (§5), surface to the host, then
   `ack` the frame's `delivery_key` after consumption.
5. To send: write a bare envelope object — set `to`/`kind` for direct vs
   broadcast, fill all nine fields, compute `hmac` over the canonical form.
6. To list peers: send a `peers` request, read the `peers` reply.
7. On disconnect: redial and re-`register` with the same name; rely on broker
   redelivery + your dedupe cache.
