package adapter

import (
	"container/list"
	"sync"
)

// Consumer-side dedupe — WHY THIS EXISTS (load-bearing rationale).
//
// peerbus's locked delivery model (see the plan's Solution Overview
// "Delivery" bullet and internal/broker/router.go) is *at-least-once*:
//
//   - a sender may retry a send (the broker drops the duplicate by the
//     store's UNIQUE(id), so the producer side is idempotent), but
//   - the CONSUMER side is NOT idempotent for free: the reconnect/resume
//     protocol (reconnect.go) deliberately re-registers under the same
//     name, which triggers the broker's same-token takeover plus a
//     PendingFor flush + RequeueUnacked of every delivered-but-unacked
//     message. A message that was delivered, surfaced to the host, and
//     consumed but whose ack did NOT reach the broker before the drop
//     WILL be redelivered after reconnect.
//
// Therefore duplicates at the consumer are EXPECTED, not exceptional, and
// dedupe is MANDATORY: every received message passes through this cache
// before being surfaced to the host, so the host sees each id exactly
// once even though the wire delivers it at-least-once.
//
// This is the SINGLE implementation. Both the generic adapter (Task 10)
// and the cc adapter (Task 11) reuse this exact cache via the shared
// client — neither reimplements dedupe per mode.
//
// Bound: the seen-id set is memory-bounded with LRU eviction. An unbounded
// set would grow forever for a long-lived drain agent. The bound is a
// pragmatic trade: the only ids that can be redelivered are those still
// unacked at the broker, and an adapter acks promptly after the host
// consumes, so the in-flight unacked window is tiny relative to any
// reasonable cache size. Evicting the oldest ids cannot cause a
// false-negative for a realistically-sized cache because an id old enough
// to be evicted was acked long ago and the broker will never resend it.

// DefaultDedupeSize is the default bound for the seen-id cache when a
// non-positive size is requested. It is comfortably larger than any
// plausible unacked in-flight window.
const DefaultDedupeSize = 4096

// Dedupe is a bounded, LRU-evicting set of message ids already surfaced to
// the host. Safe for concurrent use. Seen reports whether an id was
// already observed and records it; the most-recently-seen ids are kept and
// the oldest are evicted once the configured bound is exceeded.
type Dedupe struct {
	mu  sync.Mutex
	max int
	ll  *list.List               // front = most recently seen
	idx map[string]*list.Element // id -> element in ll
}

// NewDedupe constructs a Dedupe holding at most size ids. A non-positive
// size falls back to DefaultDedupeSize.
func NewDedupe(size int) *Dedupe {
	if size <= 0 {
		size = DefaultDedupeSize
	}
	return &Dedupe{
		max: size,
		ll:  list.New(),
		idx: make(map[string]*list.Element, size),
	}
}

// Seen reports whether id was already recorded. On first sighting it
// records id (evicting the least-recently-seen id if the cache is full)
// and returns false; on a repeat it refreshes the id's recency and
// returns true. The caller surfaces the message to the host only when
// Seen returns false.
func (d *Dedupe) Seen(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, ok := d.idx[id]; ok {
		d.ll.MoveToFront(el)
		return true
	}
	el := d.ll.PushFront(id)
	d.idx[id] = el
	if d.ll.Len() > d.max {
		oldest := d.ll.Back()
		if oldest != nil {
			d.ll.Remove(oldest)
			delete(d.idx, oldest.Value.(string))
		}
	}
	return false
}

// Len returns the number of ids currently retained (bounded by the
// configured max). Primarily for tests/observability.
func (d *Dedupe) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ll.Len()
}
