package broker

import (
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

// fakeConn is a registry.Conn that records whether it was taken over.
type fakeConn struct {
	closed atomic.Bool
}

func (f *fakeConn) CloseTakenOver() { f.closed.Store(true) }

func TestRegistry_AddRemoveList(t *testing.T) {
	r := NewRegistry()

	if got := r.List(); len(got) != 0 {
		t.Fatalf("empty registry List = %v, want []", got)
	}

	a, b := &fakeConn{}, &fakeConn{}
	if _, err := r.Bind("alice", "tok", a); err != nil {
		t.Fatalf("bind alice: %v", err)
	}
	if _, err := r.Bind("bob", "tok", b); err != nil {
		t.Fatalf("bind bob: %v", err)
	}

	if got, want := r.List(), []string{"alice", "bob"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("List = %v, want %v", got, want)
	}
	if c, ok := r.Get("alice"); !ok || c != a {
		t.Fatalf("Get(alice) = %v,%v, want %v,true", c, ok, a)
	}

	r.Remove("alice", a)
	if _, ok := r.Get("alice"); ok {
		t.Fatalf("alice still bound after Remove")
	}
	if got, want := r.List(), []string{"bob"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("List after remove = %v, want %v", got, want)
	}
}

func TestRegistry_SameTokenTakeover(t *testing.T) {
	r := NewRegistry()
	old := &fakeConn{}
	if _, err := r.Bind("alice", "tok", old); err != nil {
		t.Fatalf("first bind: %v", err)
	}

	newc := &fakeConn{}
	takenOver, err := r.Bind("alice", "tok", newc)
	if err != nil {
		t.Fatalf("same-token re-bind: %v", err)
	}
	if !takenOver {
		t.Fatalf("takenOver = false, want true for same-token re-claim")
	}
	if !old.closed.Load() {
		t.Fatalf("old connection was not closed on takeover")
	}
	if c, _ := r.Get("alice"); c != newc {
		t.Fatalf("alice not bound to new conn after takeover")
	}
}

func TestRegistry_DifferentTokenReject(t *testing.T) {
	r := NewRegistry()
	old := &fakeConn{}
	if _, err := r.Bind("alice", "tok-A", old); err != nil {
		t.Fatalf("first bind: %v", err)
	}

	newc := &fakeConn{}
	_, err := r.Bind("alice", "tok-B", newc)
	if !errors.Is(err, ErrNameClaimed) {
		t.Fatalf("different-token re-bind err = %v, want ErrNameClaimed", err)
	}
	if old.closed.Load() {
		t.Fatalf("old conn must NOT be closed on a different-token reject")
	}
	if c, _ := r.Get("alice"); c != old {
		t.Fatalf("alice binding must be untouched on reject")
	}
}

func TestRegistry_RemoveStaleConnIsNoop(t *testing.T) {
	r := NewRegistry()
	old := &fakeConn{}
	newc := &fakeConn{}
	_, _ = r.Bind("alice", "tok", old)
	_, _ = r.Bind("alice", "tok", newc) // takeover

	// The displaced old conn later notices it was closed and calls Remove;
	// it must NOT evict the live new binding.
	r.Remove("alice", old)
	if c, ok := r.Get("alice"); !ok || c != newc {
		t.Fatalf("stale Remove evicted the live binding")
	}
}

func TestRegistry_ConcurrentBind(t *testing.T) {
	r := NewRegistry()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = r.Bind("alice", "tok", &fakeConn{})
		}()
	}
	wg.Wait()
	if got := r.List(); len(got) != 1 || got[0] != "alice" {
		t.Fatalf("after concurrent binds List = %v, want [alice]", got)
	}
}
