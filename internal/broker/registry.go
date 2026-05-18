package broker

import (
	"crypto/subtle"
	"errors"
	"sort"
	"sync"
)

// ErrNameClaimed is returned by Bind when the name is already held under a
// DIFFERENT bearer token (a different-token claim is always rejected; only a
// same-token claim is a takeover).
var ErrNameClaimed = errors.New("broker: peer name already claimed under a different token")

// Conn is the registry's view of a bound peer connection. The concrete WS
// transport implements it; the registry only needs to be able to close a
// superseded connection during a same-token takeover.
//
// CloseTakenOver is called by the registry, while holding no lock, on the OLD
// connection when its name is taken over by a new same-token Bind. It must be
// safe to call from any goroutine and idempotent.
type Conn interface {
	CloseTakenOver()
}

// binding is one registered peer: the live connection plus the token it was
// bound under (so a re-claim can be classified same-token vs different-token).
type binding struct {
	conn  Conn
	token string
}

// Registry is the in-memory peer-name → connection registry.
//
// Binding rules (locked by the plan / Technical Details "Auth"):
//   - unique-name binding: one live connection per peer name.
//   - duplicate-name claim under the SAME token = takeover: the old
//     connection is closed (CloseTakenOver) and the new one takes the name.
//   - duplicate-name claim under a DIFFERENT token = reject (ErrNameClaimed);
//     the existing binding is left untouched.
//
// All methods are safe for concurrent use (guarded by a single mutex). The
// superseded connection is closed OUTSIDE the lock so a slow Close cannot
// stall other registry operations.
type Registry struct {
	mu    sync.Mutex
	peers map[string]binding
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{peers: make(map[string]binding)}
}

// tokenEq is a constant-time token comparison (the registry classifies a
// re-claim by token equality; timing must not leak the bound token).
func tokenEq(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// Bind associates name with conn under token. On success it returns
// takenOver=true and the superseded connection iff this was a same-token
// takeover of an existing binding (the caller may inspect/close it; Bind has
// already called CloseTakenOver on it). A different-token claim returns
// ErrNameClaimed and does not disturb the existing binding.
func (r *Registry) Bind(name, token string, conn Conn) (takenOver bool, old Conn, err error) {
	r.mu.Lock()
	existing, ok := r.peers[name]
	if ok {
		if !tokenEq(existing.token, token) {
			r.mu.Unlock()
			return false, nil, ErrNameClaimed
		}
		// Same-token takeover: install the new conn, then close the old
		// one outside the lock.
		r.peers[name] = binding{conn: conn, token: token}
		r.mu.Unlock()
		existing.conn.CloseTakenOver()
		return true, existing.conn, nil
	}
	r.peers[name] = binding{conn: conn, token: token}
	r.mu.Unlock()
	return false, nil, nil
}

// Remove unbinds name iff it is currently bound to conn. The conn-identity
// guard prevents a stale connection (already superseded by a takeover) from
// evicting the live binding when it later notices it was closed.
func (r *Registry) Remove(name string, conn Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok := r.peers[name]; ok && b.conn == conn {
		delete(r.peers, name)
	}
}

// Get returns the connection bound to name and ok=true, or ok=false if the
// name is not currently bound.
func (r *Registry) Get(name string) (Conn, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.peers[name]
	if !ok {
		return nil, false
	}
	return b.conn, true
}

// List returns the currently-bound peer names, sorted ascending.
func (r *Registry) List() []string {
	r.mu.Lock()
	out := make([]string, 0, len(r.peers))
	for n := range r.peers {
		out = append(out, n)
	}
	r.mu.Unlock()
	sort.Strings(out)
	return out
}
