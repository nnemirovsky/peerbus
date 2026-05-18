-- peerbus durable store schema.
-- Applied idempotently on Open(); see migrations.go.

-- Registered peers. A peer is known by its unique name.
CREATE TABLE IF NOT EXISTS peers (
    name       TEXT PRIMARY KEY,
    registered INTEGER NOT NULL DEFAULT 0, -- unix nanos of (re-)registration
    ts         INTEGER NOT NULL            -- unix nanos of first registration
);

-- Durable message queue.
--
--   id        : envelope id (ULID/uuidv7). UNIQUE -> dedupe by id.
--   sender    : "from" peer name.
--   recipient : "to" peer name (a literal name; broadcast is fanned out by the
--               broker into one row per concrete recipient, so this column is
--               always a concrete name here).
--   envelope  : opaque message bytes (the full wire envelope blob), stored
--               verbatim; the store never re-encodes it.
--   delivered : 0 = pending/offline for recipient, 1 = handed to recipient.
--   acked     : 0 = not acked, 1 = recipient acked.
--   attempts  : delivery attempt counter.
--   seq       : per-sender monotonic sequence assigned at enqueue; orders
--               PendingFor() so each sender's messages are FIFO.
--   ts        : unix nanos at enqueue.
CREATE TABLE IF NOT EXISTS messages (
    id        TEXT NOT NULL UNIQUE,
    sender    TEXT NOT NULL,
    recipient TEXT NOT NULL,
    envelope  BLOB NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0,
    acked     INTEGER NOT NULL DEFAULT 0,
    attempts  INTEGER NOT NULL DEFAULT 0,
    seq       INTEGER NOT NULL,
    ts        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_pending
    ON messages (recipient, delivered, sender, seq);

-- Per-sender monotonic sequence counters. seq for a sender is allocated by
-- bumping next here inside the Enqueue transaction so concurrent enqueues
-- never collide and order is stable.
CREATE TABLE IF NOT EXISTS sender_seq (
    sender TEXT PRIMARY KEY,
    next   INTEGER NOT NULL
);

-- Append-only audit log. The blake3 hash-chain logic is NOT implemented here
-- (that is Task 6); this table only provides the durable shape it needs:
--
--   seq       : monotonic chain position (0 = genesis).
--   prev_hash : hex hash of the previous row (genesis = blake3("")).
--   hash      : hex blake3(prev_hash || canonical_event).
--   event     : opaque canonical event blob.
--   ts        : unix nanos at append.
CREATE TABLE IF NOT EXISTS audit (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    seq       INTEGER NOT NULL UNIQUE,
    prev_hash TEXT NOT NULL,
    hash      TEXT NOT NULL,
    event     BLOB NOT NULL,
    ts        INTEGER NOT NULL
);
