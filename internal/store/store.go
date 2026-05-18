// Package store is peerbus's durable message store: a pure-Go SQLite
// (modernc.org/sqlite, WAL mode) queue with dedupe-by-id and per-sender FIFO.
//
// Delivery model (locked by the plan): at-least-once, durable,
// dedupe-by-message-id, per-sender FIFO (no global order). A message for an
// offline recipient is simply a row with delivered=0; it is flushed when the
// recipient registers/drains. A delivered-but-unacked message is requeued for
// redelivery on reconnect (consumers dedupe).
package store

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	// Pure-Go SQLite driver (no cgo); registers the "sqlite" database/sql
	// driver as an import side effect.
	_ "modernc.org/sqlite"
)

// nowNanos returns the current time as unix nanoseconds.
func nowNanos() int64 { return time.Now().UnixNano() }

// ErrDuplicateID is returned by Enqueue when a message with the same id has
// already been stored. It is a distinct sentinel (not a silent success) so
// callers can tell a real insert from a dedupe hit; the row is left untouched.
var ErrDuplicateID = errors.New("store: duplicate message id")

// ErrUnknownPeer is returned by operations that require a registered peer
// when no such peer exists.
var ErrUnknownPeer = errors.New("store: unknown peer")

// ErrClosed is returned by any operation invoked after Close.
var ErrClosed = errors.New("store: closed")

// Peer is a registered participant on the bus.
type Peer struct {
	Name string
}

// Message is one durable queue row.
type Message struct {
	ID        string
	From      string
	To        string
	Envelope  []byte
	Delivered bool
	Acked     bool
	Attempts  int
	Seq       int64
	TS        int64
}

// Store is a durable SQLite-backed message queue. It is safe for concurrent
// use; SQLite serialises writes and an internal mutex guards the closed flag.
type Store struct {
	db *sql.DB

	mu     sync.RWMutex
	closed bool
}

// Open opens (creating if needed) the SQLite database at path and applies the
// schema idempotently. Pass ":memory:" for an ephemeral database. WAL journal
// mode and sane durability/concurrency pragmas are set on the connection.
func Open(path string) (*Store, error) {
	// Single shared connection: WAL + a modest busy timeout keep concurrent
	// callers from tripping SQLITE_BUSY, and a single writer keeps the
	// per-sender seq allocation race-free.
	dsn := fmt.Sprintf(
		"file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)&_pragma=synchronous(NORMAL)",
		path,
	)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// :memory: databases are per-connection; pin to one so the schema and
	// data are visible across calls.
	db.SetMaxOpenConns(1)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	if err := migrate(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close closes the underlying database. Subsequent operations return
// ErrClosed. Calling Close more than once is a no-op.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.db.Close()
}

func (s *Store) okToUse() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrClosed
	}
	return nil
}

// Register records a peer by name. It is idempotent: re-registering an
// existing peer refreshes its registration timestamp but keeps the original
// first-seen ts.
func (s *Store) Register(p Peer) error {
	if err := s.okToUse(); err != nil {
		return err
	}
	if p.Name == "" {
		return fmt.Errorf("register: empty peer name")
	}
	now := nowNanos()
	_, err := s.db.Exec(`
		INSERT INTO peers (name, registered, ts) VALUES (?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET registered = excluded.registered`,
		p.Name, now, now)
	if err != nil {
		return fmt.Errorf("register peer: %w", err)
	}
	return nil
}

// peerExists reports whether name is a registered peer.
func (s *Store) peerExists(name string) (bool, error) {
	var one int
	err := s.db.QueryRow(`SELECT 1 FROM peers WHERE name = ?`, name).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Enqueue durably stores msg for delivery. The recipient (msg.To) must be a
// registered peer (ErrUnknownPeer otherwise). Dedupe is by msg.ID: if a
// message with the same id already exists the row is left untouched and
// ErrDuplicateID is returned (a distinct sentinel, not a silent success).
//
// A per-sender monotonic seq is assigned inside the transaction so that
// PendingFor returns each sender's messages in FIFO order even under
// concurrent enqueues.
func (s *Store) Enqueue(msg Message) error {
	if err := s.okToUse(); err != nil {
		return err
	}
	if msg.ID == "" || msg.From == "" || msg.To == "" {
		return fmt.Errorf("enqueue: id, from and to are required")
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("enqueue begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Recipient must be a known peer.
	var one int
	err = tx.QueryRow(`SELECT 1 FROM peers WHERE name = ?`, msg.To).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrUnknownPeer
	}
	if err != nil {
		return fmt.Errorf("enqueue peer check: %w", err)
	}

	// Dedupe by id.
	err = tx.QueryRow(`SELECT 1 FROM messages WHERE id = ?`, msg.ID).Scan(&one)
	switch {
	case err == nil:
		return ErrDuplicateID
	case errors.Is(err, sql.ErrNoRows):
		// fresh id, continue
	default:
		return fmt.Errorf("enqueue dedupe check: %w", err)
	}

	// Allocate a per-sender monotonic seq.
	var seq int64
	err = tx.QueryRow(`SELECT next FROM sender_seq WHERE sender = ?`, msg.From).Scan(&seq)
	switch {
	case err == nil:
		// have a counter
	case errors.Is(err, sql.ErrNoRows):
		seq = 0
	default:
		return fmt.Errorf("enqueue seq read: %w", err)
	}
	if _, err = tx.Exec(`
		INSERT INTO sender_seq (sender, next) VALUES (?, ?)
		ON CONFLICT(sender) DO UPDATE SET next = excluded.next`,
		msg.From, seq+1); err != nil {
		return fmt.Errorf("enqueue seq bump: %w", err)
	}

	if _, err = tx.Exec(`
		INSERT INTO messages
			(id, sender, recipient, envelope, delivered, acked, attempts, seq, ts)
		VALUES (?, ?, ?, ?, 0, 0, 0, ?, ?)`,
		msg.ID, msg.From, msg.To, msg.Envelope, seq, nowNanos()); err != nil {
		return fmt.Errorf("enqueue insert: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("enqueue commit: %w", err)
	}
	return nil
}

// PendingFor returns the undelivered (delivered=0) messages for recipient
// name, ordered per-sender FIFO via the monotonic seq. The recipient must be
// a registered peer (ErrUnknownPeer otherwise). An empty slice (not an error)
// means nothing is queued.
//
// Ordering note: rows are ordered by (sender, seq) then ts so each sender's
// stream is strictly FIFO; there is intentionally no global cross-sender
// order (matches the locked per-sender-FIFO delivery model).
func (s *Store) PendingFor(name string) ([]Message, error) {
	if err := s.okToUse(); err != nil {
		return nil, err
	}
	exists, err := s.peerExists(name)
	if err != nil {
		return nil, fmt.Errorf("pending peer check: %w", err)
	}
	if !exists {
		return nil, ErrUnknownPeer
	}

	rows, err := s.db.Query(`
		SELECT id, sender, recipient, envelope, delivered, acked, attempts, seq, ts
		FROM messages
		WHERE recipient = ? AND delivered = 0
		ORDER BY sender ASC, seq ASC, ts ASC`, name)
	if err != nil {
		return nil, fmt.Errorf("pending query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Message
	for rows.Next() {
		var m Message
		var delivered, acked int
		if err := rows.Scan(&m.ID, &m.From, &m.To, &m.Envelope,
			&delivered, &acked, &m.Attempts, &m.Seq, &m.TS); err != nil {
			return nil, fmt.Errorf("pending scan: %w", err)
		}
		m.Delivered = delivered != 0
		m.Acked = acked != 0
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pending rows: %w", err)
	}
	return out, nil
}

// MarkDelivered marks the message with the given id as delivered and bumps
// its attempt counter. Unknown ids are a no-op (at-least-once delivery means
// callers may retry; an unknown id is not an error here).
func (s *Store) MarkDelivered(id string) error {
	if err := s.okToUse(); err != nil {
		return err
	}
	_, err := s.db.Exec(`
		UPDATE messages SET delivered = 1, attempts = attempts + 1
		WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("mark delivered: %w", err)
	}
	return nil
}

// MarkAcked marks the message with the given id as acked. Once acked a
// message is never requeued by RequeueUnacked. Unknown ids are a no-op.
func (s *Store) MarkAcked(id string) error {
	if err := s.okToUse(); err != nil {
		return err
	}
	_, err := s.db.Exec(`UPDATE messages SET acked = 1 WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("mark acked: %w", err)
	}
	return nil
}

// AuditRow is one append-only audit-log row. The blake3 hash-chain semantics
// live in internal/audit; the store only persists and reads back the rows.
//
//	Seq      : monotonic chain position (0 = genesis/first row).
//	PrevHash : hex hash of the previous row (first row chains off blake3("")).
//	Hash     : hex blake3(prev_hash || canonical event bytes).
//	Event    : opaque canonical event blob, stored verbatim.
//	TS       : unix nanos at append.
type AuditRow struct {
	Seq      int64
	PrevHash string
	Hash     string
	Event    []byte
	TS       int64
}

// AuditAppend durably appends one audit row. It does NOT compute or validate
// the hash chain (that is internal/audit's job); it only persists the row as
// given. The UNIQUE constraint on audit.seq is the last line of defence
// against a duplicated chain position — internal/audit serialises appends so
// this should never fire in practice, but a violation surfaces as an error
// rather than a silent corruption.
func (s *Store) AuditAppend(r AuditRow) error {
	if err := s.okToUse(); err != nil {
		return err
	}
	if r.Hash == "" || r.PrevHash == "" {
		return fmt.Errorf("audit append: prev_hash and hash are required")
	}
	_, err := s.db.Exec(`
		INSERT INTO audit (seq, prev_hash, hash, event, ts)
		VALUES (?, ?, ?, ?, ?)`,
		r.Seq, r.PrevHash, r.Hash, r.Event, nowNanos())
	if err != nil {
		return fmt.Errorf("audit append: %w", err)
	}
	return nil
}

// AuditRows returns every audit row ordered by seq ascending (genesis first).
// An empty slice (not an error) means the log is empty.
func (s *Store) AuditRows() ([]AuditRow, error) {
	if err := s.okToUse(); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(`
		SELECT seq, prev_hash, hash, event, ts
		FROM audit ORDER BY seq ASC`)
	if err != nil {
		return nil, fmt.Errorf("audit rows query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []AuditRow
	for rows.Next() {
		var r AuditRow
		if err := rows.Scan(&r.Seq, &r.PrevHash, &r.Hash, &r.Event, &r.TS); err != nil {
			return nil, fmt.Errorf("audit rows scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("audit rows: %w", err)
	}
	return out, nil
}

// AuditCount returns the number of audit rows. Used by internal/audit to find
// the next chain position without loading the whole log.
func (s *Store) AuditCount() (int64, error) {
	if err := s.okToUse(); err != nil {
		return 0, err
	}
	var n int64
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM audit`).Scan(&n); err != nil {
		return 0, fmt.Errorf("audit count: %w", err)
	}
	return n, nil
}

// AuditLast returns the most recent audit row (highest seq) and ok=true, or
// ok=false when the log is empty.
func (s *Store) AuditLast() (AuditRow, bool, error) {
	if err := s.okToUse(); err != nil {
		return AuditRow{}, false, err
	}
	var r AuditRow
	err := s.db.QueryRow(`
		SELECT seq, prev_hash, hash, event, ts
		FROM audit ORDER BY seq DESC LIMIT 1`).
		Scan(&r.Seq, &r.PrevHash, &r.Hash, &r.Event, &r.TS)
	if errors.Is(err, sql.ErrNoRows) {
		return AuditRow{}, false, nil
	}
	if err != nil {
		return AuditRow{}, false, fmt.Errorf("audit last: %w", err)
	}
	return r, true, nil
}

// AuditTamper overwrites the event and hash of the audit row at the given seq.
// It exists ONLY to let tests simulate on-disk corruption of the chain; it is
// never called by production code (the audit log is otherwise append-only).
func (s *Store) AuditTamper(seq int64, event []byte, hash string) error {
	if err := s.okToUse(); err != nil {
		return err
	}
	_, err := s.db.Exec(`UPDATE audit SET event = ?, hash = ? WHERE seq = ?`,
		event, hash, seq)
	if err != nil {
		return fmt.Errorf("audit tamper: %w", err)
	}
	return nil
}

// RequeueUnacked makes every delivered-but-unacked message for recipient name
// eligible for redelivery again (delivered -> 0). Acked messages are left
// alone. Called when a peer reconnects so in-flight-but-unconfirmed messages
// are redelivered (consumers dedupe by id). The recipient must be a
// registered peer (ErrUnknownPeer otherwise). Returns the number of rows
// requeued.
func (s *Store) RequeueUnacked(name string) (int, error) {
	if err := s.okToUse(); err != nil {
		return 0, err
	}
	exists, err := s.peerExists(name)
	if err != nil {
		return 0, fmt.Errorf("requeue peer check: %w", err)
	}
	if !exists {
		return 0, ErrUnknownPeer
	}
	res, err := s.db.Exec(`
		UPDATE messages SET delivered = 0
		WHERE recipient = ? AND delivered = 1 AND acked = 0`, name)
	if err != nil {
		return 0, fmt.Errorf("requeue unacked: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("requeue rows affected: %w", err)
	}
	return int(n), nil
}
