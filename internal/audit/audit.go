// Package audit is peerbus's append-only, tamper-evident audit log.
//
// Every appended event is chained: each row's hash is
//
//	hash = blake3( prev_hash_bytes || canonical_event_bytes )
//
// where the genesis row chains off blake3("") (the blake3 digest of the empty
// input). Hashes are stored hex-encoded; the bytes fed into blake3 for the
// chain link are the *hex string bytes* of the previous hash followed by the
// raw canonical event bytes, so the link is reproducible from what is stored.
//
// Hash function choice: lukechampine.com/blake3 — a maintained, pure-Go
// BLAKE3 implementation (no cgo), consistent with the project's pure-Go
// constraint (modernc.org/sqlite). The borrowed design (sluice) used a blake3
// hash-chain audit log; this mirrors it.
//
// Single-writer invariant: the hash chain is only well defined if appends are
// serialised — concurrent appends would race on "what is the previous hash",
// fork the chain, and collide on the UNIQUE audit.seq constraint. Appender
// guards every Append with a mutex so the broker (Task 8) can call it from
// multiple goroutines while the chain stays a single linear sequence.
package audit

import (
	"encoding/hex"
	"fmt"
	"sync"

	"lukechampine.com/blake3"

	"github.com/nnemirovsky/peerbus/internal/store"
)

// auditStore is the slice of internal/store the audit log needs. It keeps the
// package testable and states the exact persistence surface used.
type auditStore interface {
	AuditAppend(store.AuditRow) error
	AuditRows() ([]store.AuditRow, error)
	AuditLast() (store.AuditRow, bool, error)
}

// genesisPrevHash is the prev_hash of the very first (seq 0) row: the hex
// blake3 digest of the empty input.
func genesisPrevHash() string {
	sum := blake3.Sum256(nil)
	return hex.EncodeToString(sum[:])
}

// chainHash computes the hex hash linking an event onto prevHash:
// blake3( prevHash-as-ascii-bytes || event ). prevHash is the hex string of
// the previous row's hash (or the genesis prev hash for seq 0).
func chainHash(prevHash string, event []byte) string {
	h := blake3.New(32, nil)
	_, _ = h.Write([]byte(prevHash))
	_, _ = h.Write(event)
	var sum [32]byte
	h.Sum(sum[:0])
	return hex.EncodeToString(sum[:])
}

// Appender serialises append-only writes to the audit hash-chain. It is safe
// for concurrent use; every Append is mutually exclusive (the single-writer
// invariant the chain requires).
type Appender struct {
	mu sync.Mutex
	st auditStore
}

// NewAppender returns an Appender backed by st.
func NewAppender(st *store.Store) *Appender {
	return &Appender{st: st}
}

// newAppenderFor is the testable constructor (any auditStore).
func newAppenderFor(st auditStore) *Appender {
	return &Appender{st: st}
}

// Append durably appends one event to the chain and returns the row written.
// Appends are serialised by a mutex: the chain's "next hash depends on the
// previous hash" link makes a single writer mandatory. The event bytes are
// stored verbatim and hashed verbatim (the caller owns canonicalisation).
func (a *Appender) Append(event []byte) (store.AuditRow, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	last, ok, err := a.st.AuditLast()
	if err != nil {
		return store.AuditRow{}, fmt.Errorf("audit append: read tail: %w", err)
	}

	var seq int64
	var prevHash string
	if ok {
		seq = last.Seq + 1
		prevHash = last.Hash
	} else {
		seq = 0
		prevHash = genesisPrevHash()
	}

	row := store.AuditRow{
		Seq:      seq,
		PrevHash: prevHash,
		Hash:     chainHash(prevHash, event),
		Event:    event,
	}
	if err := a.st.AuditAppend(row); err != nil {
		return store.AuditRow{}, fmt.Errorf("audit append: persist: %w", err)
	}
	return row, nil
}

// Break describes the first point at which the chain fails verification.
type Break struct {
	// Index is the position in the ordered row list where the break was
	// found (0-based).
	Index int
	// Seq is the audit.seq of the offending row (or the expected seq when a
	// row is missing/out of order).
	Seq int64
	// Detail is a human-readable explanation of the break.
	Detail string
}

func (b *Break) Error() string {
	return fmt.Sprintf("audit chain broken at index %d (seq %d): %s", b.Index, b.Seq, b.Detail)
}

// Verify walks the whole chain from genesis and recomputes every link. It
// returns the first break found, or nil if the chain is intact (an empty log
// verifies clean — genesis is implicit until the first Append).
func Verify(st *store.Store) (*Break, error) {
	return verify(st)
}

func verify(st auditStore) (*Break, error) {
	rows, err := st.AuditRows()
	if err != nil {
		return nil, fmt.Errorf("audit verify: read rows: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil // empty log: genesis only, nothing to break
	}

	want := genesisPrevHash()
	for i, r := range rows {
		expectedSeq := int64(i)
		if r.Seq != expectedSeq {
			return &Break{
				Index:  i,
				Seq:    expectedSeq,
				Detail: fmt.Sprintf("seq out of order: row has seq %d, expected %d", r.Seq, expectedSeq),
			}, nil
		}
		if r.PrevHash != want {
			return &Break{
				Index:  i,
				Seq:    r.Seq,
				Detail: fmt.Sprintf("prev_hash mismatch: stored %q, expected %q", r.PrevHash, want),
			}, nil
		}
		got := chainHash(r.PrevHash, r.Event)
		if got != r.Hash {
			return &Break{
				Index:  i,
				Seq:    r.Seq,
				Detail: fmt.Sprintf("hash mismatch: stored %q, recomputed %q (event tampered)", r.Hash, got),
			}, nil
		}
		want = r.Hash
	}
	return nil, nil
}
