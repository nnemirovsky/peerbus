package store

import (
	"errors"
	"path/filepath"
	"testing"
)

// newStore opens a fresh temp-file store for a test and registers it for
// cleanup. A temp file (not :memory:) is used so WAL behaviour is exercised.
func newStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "peerbus-test.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustRegister(t *testing.T, s *Store, names ...string) {
	t.Helper()
	for _, n := range names {
		if err := s.Register(Peer{Name: n}); err != nil {
			t.Fatalf("Register(%q): %v", n, err)
		}
	}
}

func msg(id, from, to, body string) Message {
	return Message{ID: id, From: from, To: to, Envelope: []byte(body)}
}

func TestRegisterIdempotent(t *testing.T) {
	s := newStore(t)
	mustRegister(t, s, "alice")
	// Re-register: must not error.
	if err := s.Register(Peer{Name: "alice"}); err != nil {
		t.Fatalf("re-Register: %v", err)
	}
	if err := s.Register(Peer{Name: ""}); err == nil {
		t.Fatal("Register with empty name: want error, got nil")
	}
}

func TestEnqueueAndDedupe(t *testing.T) {
	s := newStore(t)
	mustRegister(t, s, "alice", "bob")

	cases := []struct {
		name    string
		m       Message
		wantErr error
	}{
		{"first insert", msg("m1", "alice", "bob", "hi"), nil},
		{"distinct id", msg("m2", "alice", "bob", "yo"), nil},
		{"duplicate id is sentinel", msg("m1", "alice", "bob", "dup"), ErrDuplicateID},
		{"unknown recipient", msg("m3", "alice", "carol", "x"), ErrUnknownPeer},
		{"missing fields", Message{ID: "", From: "alice", To: "bob"}, errSentinelAny},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := s.Enqueue(tc.m)
			switch {
			case tc.wantErr == nil && err != nil:
				t.Fatalf("Enqueue: unexpected error %v", err)
			case tc.wantErr == errSentinelAny && err == nil:
				t.Fatal("Enqueue: want some error, got nil")
			case tc.wantErr != nil && tc.wantErr != errSentinelAny && !errors.Is(err, tc.wantErr):
				t.Fatalf("Enqueue: want %v, got %v", tc.wantErr, err)
			}
		})
	}

	// The duplicate must not have created a second row / mutated the first.
	pend, err := s.PendingFor("bob")
	if err != nil {
		t.Fatalf("PendingFor: %v", err)
	}
	if len(pend) != 2 {
		t.Fatalf("want 2 pending after dedupe, got %d", len(pend))
	}
	for _, m := range pend {
		if m.ID == "m1" && string(m.Envelope) != "hi" {
			t.Fatalf("dedupe mutated original row: envelope=%q", m.Envelope)
		}
	}
}

// errSentinelAny is a marker meaning "any non-nil error is acceptable".
var errSentinelAny = errors.New("any error")

func TestPerSenderFIFO(t *testing.T) {
	s := newStore(t)
	mustRegister(t, s, "alice", "carol", "bob")

	// Interleave two senders to bob.
	enq := []Message{
		msg("a1", "alice", "bob", "a-1"),
		msg("c1", "carol", "bob", "c-1"),
		msg("a2", "alice", "bob", "a-2"),
		msg("c2", "carol", "bob", "c-2"),
		msg("a3", "alice", "bob", "a-3"),
	}
	for _, m := range enq {
		if err := s.Enqueue(m); err != nil {
			t.Fatalf("Enqueue %s: %v", m.ID, err)
		}
	}

	pend, err := s.PendingFor("bob")
	if err != nil {
		t.Fatalf("PendingFor: %v", err)
	}

	// Within each sender, seq must be strictly increasing in the result.
	lastSeq := map[string]int64{}
	seen := map[string]int{}
	for _, m := range pend {
		seen[m.From]++
		if prev, ok := lastSeq[m.From]; ok && m.Seq <= prev {
			t.Fatalf("sender %s not FIFO: seq %d after %d", m.From, m.Seq, prev)
		}
		lastSeq[m.From] = m.Seq
	}
	if seen["alice"] != 3 || seen["carol"] != 2 {
		t.Fatalf("wrong per-sender counts: %v", seen)
	}
	// alice's stream order must be a1,a2,a3.
	var aOrder []string
	for _, m := range pend {
		if m.From == "alice" {
			aOrder = append(aOrder, m.ID)
		}
	}
	if got := aOrder; len(got) != 3 || got[0] != "a1" || got[1] != "a2" || got[2] != "a3" {
		t.Fatalf("alice FIFO order wrong: %v", got)
	}
}

func TestOfflineThenPendingFor(t *testing.T) {
	s := newStore(t)
	mustRegister(t, s, "alice", "bob")

	// bob is "offline": messages just sit at delivered=0.
	for _, id := range []string{"o1", "o2"} {
		if err := s.Enqueue(msg(id, "alice", "bob", id)); err != nil {
			t.Fatalf("Enqueue %s: %v", id, err)
		}
	}
	pend, err := s.PendingFor("bob")
	if err != nil {
		t.Fatalf("PendingFor: %v", err)
	}
	if len(pend) != 2 {
		t.Fatalf("offline recipient: want 2 pending, got %d", len(pend))
	}
	// A recipient with nothing queued -> empty, not error.
	mustRegister(t, s, "dave")
	pd, err := s.PendingFor("dave")
	if err != nil {
		t.Fatalf("PendingFor(dave): %v", err)
	}
	if len(pd) != 0 {
		t.Fatalf("want 0 pending for dave, got %d", len(pd))
	}
}

func TestAckStopsRedelivery(t *testing.T) {
	s := newStore(t)
	mustRegister(t, s, "alice", "bob")
	if err := s.Enqueue(msg("k1", "alice", "bob", "x")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := s.MarkDelivered("k1"); err != nil {
		t.Fatalf("MarkDelivered: %v", err)
	}
	// Delivered -> no longer pending.
	if pend, _ := s.PendingFor("bob"); len(pend) != 0 {
		t.Fatalf("after deliver want 0 pending, got %d", len(pend))
	}
	if err := s.MarkAcked("k1"); err != nil {
		t.Fatalf("MarkAcked: %v", err)
	}
	// Acked message must NOT be requeued.
	n, err := s.RequeueUnacked("bob")
	if err != nil {
		t.Fatalf("RequeueUnacked: %v", err)
	}
	if n != 0 {
		t.Fatalf("acked message requeued: n=%d", n)
	}
	if pend, _ := s.PendingFor("bob"); len(pend) != 0 {
		t.Fatalf("acked message redelivered: %d pending", len(pend))
	}
}

func TestUnackedRequeueOnReconnect(t *testing.T) {
	s := newStore(t)
	mustRegister(t, s, "alice", "bob")
	for _, id := range []string{"u1", "u2"} {
		if err := s.Enqueue(msg(id, "alice", "bob", id)); err != nil {
			t.Fatalf("Enqueue %s: %v", id, err)
		}
		if err := s.MarkDelivered(id); err != nil {
			t.Fatalf("MarkDelivered %s: %v", id, err)
		}
	}
	// ack only one.
	if err := s.MarkAcked("u1"); err != nil {
		t.Fatalf("MarkAcked: %v", err)
	}

	n, err := s.RequeueUnacked("bob")
	if err != nil {
		t.Fatalf("RequeueUnacked: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 requeued (u2), got %d", n)
	}
	pend, err := s.PendingFor("bob")
	if err != nil {
		t.Fatalf("PendingFor: %v", err)
	}
	if len(pend) != 1 || pend[0].ID != "u2" {
		t.Fatalf("want only u2 pending after requeue, got %+v", pend)
	}
	// Attempts should have been bumped by the earlier MarkDelivered.
	if pend[0].Attempts < 1 {
		t.Fatalf("want attempts >=1 on requeued msg, got %d", pend[0].Attempts)
	}
}

func TestErrorCases(t *testing.T) {
	t.Run("unknown peer on PendingFor and Requeue", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.PendingFor("ghost"); !errors.Is(err, ErrUnknownPeer) {
			t.Fatalf("PendingFor unknown: want ErrUnknownPeer, got %v", err)
		}
		if _, err := s.RequeueUnacked("ghost"); !errors.Is(err, ErrUnknownPeer) {
			t.Fatalf("RequeueUnacked unknown: want ErrUnknownPeer, got %v", err)
		}
	})

	t.Run("duplicate id sentinel", func(t *testing.T) {
		s := newStore(t)
		mustRegister(t, s, "alice", "bob")
		if err := s.Enqueue(msg("d1", "alice", "bob", "a")); err != nil {
			t.Fatalf("first Enqueue: %v", err)
		}
		err := s.Enqueue(msg("d1", "alice", "bob", "b"))
		if !errors.Is(err, ErrDuplicateID) {
			t.Fatalf("want ErrDuplicateID, got %v", err)
		}
	})

	t.Run("operations on a closed DB", func(t *testing.T) {
		s := newStore(t)
		mustRegister(t, s, "alice", "bob")
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		// Double close is a no-op.
		if err := s.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
		if err := s.Register(Peer{Name: "z"}); !errors.Is(err, ErrClosed) {
			t.Fatalf("Register after close: want ErrClosed, got %v", err)
		}
		if err := s.Enqueue(msg("x", "alice", "bob", "x")); !errors.Is(err, ErrClosed) {
			t.Fatalf("Enqueue after close: want ErrClosed, got %v", err)
		}
		if _, err := s.PendingFor("bob"); !errors.Is(err, ErrClosed) {
			t.Fatalf("PendingFor after close: want ErrClosed, got %v", err)
		}
		if err := s.MarkDelivered("x"); !errors.Is(err, ErrClosed) {
			t.Fatalf("MarkDelivered after close: want ErrClosed, got %v", err)
		}
		if err := s.MarkAcked("x"); !errors.Is(err, ErrClosed) {
			t.Fatalf("MarkAcked after close: want ErrClosed, got %v", err)
		}
		if _, err := s.RequeueUnacked("bob"); !errors.Is(err, ErrClosed) {
			t.Fatalf("RequeueUnacked after close: want ErrClosed, got %v", err)
		}
	})
}

func TestMemoryDSN(t *testing.T) {
	s, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open(:memory:): %v", err)
	}
	defer func() { _ = s.Close() }()
	mustRegister(t, s, "alice", "bob")
	if err := s.Enqueue(msg("m1", "alice", "bob", "hi")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	pend, err := s.PendingFor("bob")
	if err != nil {
		t.Fatalf("PendingFor: %v", err)
	}
	if len(pend) != 1 {
		t.Fatalf("want 1 pending in :memory:, got %d", len(pend))
	}
}

func TestAuditAppendAndRead(t *testing.T) {
	s := newStore(t)

	// Empty log: no rows, count 0, no last.
	rows, err := s.AuditRows()
	if err != nil {
		t.Fatalf("AuditRows empty: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("want 0 audit rows, got %d", len(rows))
	}
	if n, err := s.AuditCount(); err != nil || n != 0 {
		t.Fatalf("AuditCount empty = (%d, %v), want (0, nil)", n, err)
	}
	if _, ok, err := s.AuditLast(); err != nil || ok {
		t.Fatalf("AuditLast empty = (ok=%v, %v), want (false, nil)", ok, err)
	}

	// Append two rows.
	for i, r := range []AuditRow{
		{Seq: 0, PrevHash: "g", Hash: "h0", Event: []byte("e0")},
		{Seq: 1, PrevHash: "h0", Hash: "h1", Event: []byte("e1")},
	} {
		if err := s.AuditAppend(r); err != nil {
			t.Fatalf("AuditAppend #%d: %v", i, err)
		}
	}

	rows, err = s.AuditRows()
	if err != nil {
		t.Fatalf("AuditRows: %v", err)
	}
	if len(rows) != 2 || rows[0].Seq != 0 || rows[1].Seq != 1 {
		t.Fatalf("AuditRows ordering wrong: %+v", rows)
	}
	if string(rows[1].Event) != "e1" || rows[1].Hash != "h1" {
		t.Fatalf("AuditRows content wrong: %+v", rows[1])
	}

	last, ok, err := s.AuditLast()
	if err != nil || !ok {
		t.Fatalf("AuditLast = (ok=%v, %v)", ok, err)
	}
	if last.Seq != 1 || last.Hash != "h1" {
		t.Fatalf("AuditLast wrong row: %+v", last)
	}
	if n, err := s.AuditCount(); err != nil || n != 2 {
		t.Fatalf("AuditCount = (%d, %v), want (2, nil)", n, err)
	}
}

func TestAuditAppendDuplicateSeqRejected(t *testing.T) {
	s := newStore(t)
	if err := s.AuditAppend(AuditRow{Seq: 0, PrevHash: "g", Hash: "h0", Event: []byte("e")}); err != nil {
		t.Fatalf("AuditAppend: %v", err)
	}
	if err := s.AuditAppend(AuditRow{Seq: 0, PrevHash: "g", Hash: "h0b", Event: []byte("e")}); err == nil {
		t.Fatalf("AuditAppend duplicate seq: want error, got nil")
	}
}

func TestAuditTamper(t *testing.T) {
	s := newStore(t)
	if err := s.AuditAppend(AuditRow{Seq: 0, PrevHash: "g", Hash: "h0", Event: []byte("orig")}); err != nil {
		t.Fatalf("AuditAppend: %v", err)
	}
	if err := s.AuditTamper(0, []byte("EVIL"), "badhash"); err != nil {
		t.Fatalf("AuditTamper: %v", err)
	}
	rows, err := s.AuditRows()
	if err != nil {
		t.Fatalf("AuditRows: %v", err)
	}
	if string(rows[0].Event) != "EVIL" || rows[0].Hash != "badhash" {
		t.Fatalf("AuditTamper did not mutate row: %+v", rows[0])
	}
}

func TestAuditClosedStore(t *testing.T) {
	s := newStore(t)
	_ = s.Close()
	if err := s.AuditAppend(AuditRow{Seq: 0, PrevHash: "g", Hash: "h"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("AuditAppend after close: want ErrClosed, got %v", err)
	}
	if _, err := s.AuditRows(); !errors.Is(err, ErrClosed) {
		t.Fatalf("AuditRows after close: want ErrClosed, got %v", err)
	}
}
