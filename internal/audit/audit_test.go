package audit

import (
	"path/filepath"
	"testing"

	"github.com/nnemirovsky/peerbus/internal/store"
)

// newStore opens a fresh temp-file store for a test.
func newStore(t *testing.T) *store.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "audit-test.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestGenesisAndChainContinuity covers the empty-log genesis case and chain
// continuity across multiple appends.
func TestVerify(t *testing.T) {
	tests := []struct {
		name string
		// appends is the list of event payloads to append in order.
		appends [][]byte
		// tamper, if non-nil, mutates a row directly in the store after the
		// appends but before Verify.
		tamper func(t *testing.T, s *store.Store)
		// wantBreak is true when Verify must report a break.
		wantBreak bool
		// wantSeq is the seq the break must point at (only if wantBreak).
		wantSeq int64
	}{
		{
			name:      "empty log verifies clean (genesis only)",
			appends:   nil,
			wantBreak: false,
		},
		{
			name:      "single append chains off genesis",
			appends:   [][]byte{[]byte(`{"e":"register","peer":"alice"}`)},
			wantBreak: false,
		},
		{
			name: "chain continuity across multiple appends",
			appends: [][]byte{
				[]byte(`{"e":"register","peer":"alice"}`),
				[]byte(`{"e":"send","id":"01"}`),
				[]byte(`{"e":"deliver","id":"01"}`),
				[]byte(`{"e":"ack","id":"01"}`),
				[]byte(`{"e":"broadcast","id":"02"}`),
			},
			wantBreak: false,
		},
		{
			name: "tamper a middle row's event is detected",
			appends: [][]byte{
				[]byte(`{"e":"a"}`),
				[]byte(`{"e":"b"}`),
				[]byte(`{"e":"c"}`),
				[]byte(`{"e":"d"}`),
			},
			tamper: func(t *testing.T, s *store.Store) {
				t.Helper()
				// Mutate the event of seq 1 but keep its (now stale) hash.
				rows, err := s.AuditRows()
				if err != nil {
					t.Fatalf("AuditRows: %v", err)
				}
				if err := s.AuditTamper(1, []byte(`{"e":"EVIL"}`), rows[1].Hash); err != nil {
					t.Fatalf("AuditTamper: %v", err)
				}
			},
			wantBreak: true,
			wantSeq:   1,
		},
		{
			name: "tamper a middle row's hash is detected",
			appends: [][]byte{
				[]byte(`{"e":"a"}`),
				[]byte(`{"e":"b"}`),
				[]byte(`{"e":"c"}`),
			},
			tamper: func(t *testing.T, s *store.Store) {
				t.Helper()
				rows, err := s.AuditRows()
				if err != nil {
					t.Fatalf("AuditRows: %v", err)
				}
				// Keep the event, corrupt the stored hash of seq 1.
				if err := s.AuditTamper(1, rows[1].Event, "deadbeef"); err != nil {
					t.Fatalf("AuditTamper: %v", err)
				}
			},
			wantBreak: true,
			wantSeq:   1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newStore(t)
			ap := NewAppender(s)

			var prevHash string
			for i, ev := range tc.appends {
				row, err := ap.Append(ev)
				if err != nil {
					t.Fatalf("Append #%d: %v", i, err)
				}
				if row.Seq != int64(i) {
					t.Fatalf("Append #%d: seq = %d, want %d", i, row.Seq, i)
				}
				if i == 0 {
					if row.PrevHash != genesisPrevHash() {
						t.Fatalf("first row prev_hash = %q, want genesis %q", row.PrevHash, genesisPrevHash())
					}
				} else if row.PrevHash != prevHash {
					t.Fatalf("row #%d prev_hash = %q, want previous hash %q", i, row.PrevHash, prevHash)
				}
				prevHash = row.Hash
			}

			if tc.tamper != nil {
				tc.tamper(t, s)
			}

			brk, err := Verify(s)
			if err != nil {
				t.Fatalf("Verify: unexpected error %v", err)
			}
			if tc.wantBreak {
				if brk == nil {
					t.Fatalf("Verify: expected a break, got nil (chain reported intact)")
				}
				if brk.Seq != tc.wantSeq {
					t.Fatalf("Verify: break at seq %d, want %d (%s)", brk.Seq, tc.wantSeq, brk.Error())
				}
			} else if brk != nil {
				t.Fatalf("Verify: expected intact chain, got break: %s", brk.Error())
			}
		})
	}
}

// TestAppendIsDeterministic asserts the genesis prev-hash and the chain hash
// are stable (the cross-machine reproducibility property).
func TestChainHashDeterministic(t *testing.T) {
	g1 := genesisPrevHash()
	g2 := genesisPrevHash()
	if g1 != g2 {
		t.Fatalf("genesisPrevHash not deterministic: %q vs %q", g1, g2)
	}
	if len(g1) != 64 {
		t.Fatalf("genesis hash hex length = %d, want 64 (32 bytes)", len(g1))
	}

	ev := []byte(`{"e":"x"}`)
	h1 := chainHash(g1, ev)
	h2 := chainHash(g1, ev)
	if h1 != h2 {
		t.Fatalf("chainHash not deterministic: %q vs %q", h1, h2)
	}
	if chainHash(g1, ev) == chainHash(g1, []byte(`{"e":"y"}`)) {
		t.Fatalf("chainHash collided on distinct events")
	}
	if chainHash(g1, ev) == chainHash("00", ev) {
		t.Fatalf("chainHash ignored prev_hash")
	}
}

// TestConcurrentAppendKeepsChainValid stresses the single-writer mutex: many
// goroutines append concurrently and the resulting chain must still verify.
func TestConcurrentAppendKeepsChainValid(t *testing.T) {
	s := newStore(t)
	ap := NewAppender(s)

	const n = 50
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			_, err := ap.Append([]byte(`{"e":"concurrent"}`))
			errCh <- err
		}(i)
	}
	for i := 0; i < n; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent Append: %v", err)
		}
	}

	rows, err := s.AuditRows()
	if err != nil {
		t.Fatalf("AuditRows: %v", err)
	}
	if len(rows) != n {
		t.Fatalf("row count = %d, want %d", len(rows), n)
	}
	brk, err := Verify(s)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if brk != nil {
		t.Fatalf("Verify after concurrent appends: chain broken: %s", brk.Error())
	}
}
