package adapter

import (
	"fmt"
	"testing"
)

// TestDedupe_RepeatIDSuppressed: the first sighting is new (Seen=false),
// every repeat is suppressed (Seen=true).
func TestDedupe_RepeatIDSuppressed(t *testing.T) {
	d := NewDedupe(16)
	if d.Seen("a") {
		t.Fatalf("first sighting of a must be new (Seen=false)")
	}
	if !d.Seen("a") {
		t.Fatalf("repeat of a must be suppressed (Seen=true)")
	}
	if !d.Seen("a") {
		t.Fatalf("second repeat of a must still be suppressed")
	}
}

// TestDedupe_DistinctIDsPass: different ids are each new on first sight.
func TestDedupe_DistinctIDsPass(t *testing.T) {
	d := NewDedupe(16)
	for _, id := range []string{"x", "y", "z"} {
		if d.Seen(id) {
			t.Fatalf("distinct id %q wrongly reported as already seen", id)
		}
	}
}

// TestDedupe_EvictionBeyondBound: filling past the bound evicts the
// least-recently-seen id; a re-sighting of an evicted id is treated as new
// again, while the cache never exceeds its bound.
func TestDedupe_EvictionBeyondBound(t *testing.T) {
	const bound = 4
	d := NewDedupe(bound)
	for i := 0; i < bound; i++ {
		id := fmt.Sprintf("id-%d", i)
		if d.Seen(id) {
			t.Fatalf("%q should be new", id)
		}
	}
	if d.Len() != bound {
		t.Fatalf("len = %d, want %d", d.Len(), bound)
	}
	// Push bound more distinct ids — evicts id-0..id-3 (LRU).
	for i := bound; i < bound*2; i++ {
		if d.Seen(fmt.Sprintf("id-%d", i)) {
			t.Fatalf("id-%d should be new", i)
		}
	}
	if d.Len() != bound {
		t.Fatalf("len after overflow = %d, want %d (bounded)", d.Len(), bound)
	}
	// id-0 was evicted: it is new again (no false-positive retained).
	if d.Seen("id-0") {
		t.Fatalf("evicted id-0 should be treated as new again")
	}
}

// TestDedupe_LRURecencyRefresh: re-seeing an id refreshes its recency so a
// frequently-seen id survives eviction.
func TestDedupe_LRURecencyRefresh(t *testing.T) {
	d := NewDedupe(3)
	d.Seen("a")
	d.Seen("b")
	d.Seen("c")
	// Touch "a" so it becomes most-recent; "b" is now LRU.
	if !d.Seen("a") {
		t.Fatalf("a should already be seen")
	}
	// Insert "d" -> evicts the LRU which is "b", not "a".
	d.Seen("d")
	if d.Seen("a") == false {
		t.Fatalf("a was refreshed and must survive eviction")
	}
	if d.Seen("b") == true {
		t.Fatalf("b was LRU and must have been evicted (treated as new)")
	}
}

// TestDedupe_DefaultSize: a non-positive size falls back to the default.
func TestDedupe_DefaultSize(t *testing.T) {
	d := NewDedupe(0)
	if d.max != DefaultDedupeSize {
		t.Fatalf("max = %d, want default %d", d.max, DefaultDedupeSize)
	}
	dn := NewDedupe(-5)
	if dn.max != DefaultDedupeSize {
		t.Fatalf("negative size: max = %d, want default %d", dn.max, DefaultDedupeSize)
	}
}

// TestDedupe_Forget: forgetting an id makes a later sighting new again
// (the "redeliver until consumed" mechanism in reconnect.go).
func TestDedupe_Forget(t *testing.T) {
	d := NewDedupe(8)
	d.Seen("k")
	if !d.Seen("k") {
		t.Fatalf("k should be seen before forget")
	}
	d.forget("k")
	if d.Seen("k") {
		t.Fatalf("after forget, k must be treated as new again")
	}
	// forget of an absent id is a no-op.
	d.forget("absent")
}
