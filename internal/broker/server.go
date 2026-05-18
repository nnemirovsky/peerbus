// Package broker is peerbus's long-lived, agent-agnostic message broker.
//
// This file implements the broker core: a WebSocket server, the static
// bearer-token register handshake, and binding the authenticated peer into
// the in-memory registry. Routing (direct/broadcast/ack/redelivery) is Task 8
// and lives in router.go; this file deliberately stops at "peer is registered
// and its pending queue has been flushed".
//
// WebSocket library choice: github.com/coder/websocket (formerly nhooyr.io/
// websocket). Rationale: pure-Go (no cgo — keeps the modernc.org/sqlite
// pure-Go build story intact and cross-compilation trivial), minimal API,
// context-aware reads/writes, actively maintained, and idiomatic
// net/http.Handler integration that works directly with httptest for the
// in-process server tests. gorilla/websocket is heavier and its maintenance
// has been intermittent; coder/websocket is the better fit for a small,
// embeddable broker.
//
// Frame model: each control frame and each Envelope is sent as ONE WebSocket
// text message containing a single JSON object. WS frames are already
// length-delimited so the newline-delimited wire.Codec framing is not layered
// on top here; the same JSON object shapes (wire.Register/Ack/Peers/Deliver,
// wire.Envelope) are used verbatim.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"

	"github.com/coder/websocket"

	"github.com/nnemirovsky/peerbus/internal/audit"
	"github.com/nnemirovsky/peerbus/internal/store"
	"github.com/nnemirovsky/peerbus/internal/wire"
)

// Server is the broker WebSocket server. It is an http.Handler so it can be
// mounted on any net/http server (production) or httptest server (tests).
type Server struct {
	auth     *Authenticator
	registry *Registry
	store    *store.Store
	log      *slog.Logger

	// audit is the SINGLE broker-owned audit-log writer. The blake3 hash
	// chain (internal/audit) is only well defined under a single serialised
	// writer; audit.Appender guards every Append with its own mutex, so all
	// connection goroutines funnel send/deliver/ack events through this one
	// instance (see router.go's auditEvent). Never construct a second
	// Appender or write audit rows by any other path. May be nil: audit is
	// then disabled and routing still works (auditEvent is a no-op).
	audit *audit.Appender
}

// NewServer constructs a broker Server over the given authenticator, registry
// and durable store. log may be nil (a discarding logger is used). Audit is
// derived from the same store via a single broker-owned Appender (the
// single-writer invariant the hash chain requires).
func NewServer(auth *Authenticator, reg *Registry, st *store.Store, log *slog.Logger) *Server {
	if log == nil {
		log = slog.New(slog.NewTextHandler(discardWriter{}, nil))
	}
	return &Server{
		auth:     auth,
		registry: reg,
		store:    st,
		log:      log,
		audit:    audit.NewAppender(st),
	}
}

// discardWriter is an io.Writer sink for the default no-op logger.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// peerConn is the live WS connection for one registered peer. It implements
// registry.Conn so the registry can close it when it is taken over.
type peerConn struct {
	ws   *websocket.Conn
	name string

	mu       sync.Mutex
	wmu      sync.Mutex // serialises writes (one WS writer at a time)
	closed   bool
	takeover bool
	// done is closed exactly once when the connection is being torn down so
	// the serve loop can return promptly on a takeover.
	done chan struct{}
}

func newPeerConn(ws *websocket.Conn) *peerConn {
	return &peerConn{ws: ws, done: make(chan struct{})}
}

// CloseTakenOver implements registry.Conn. It is called by the registry on the
// OLD connection during a same-token takeover. It marks the connection as
// superseded and closes the WS with a policy-violation code so the displaced
// client can distinguish a takeover from a transport error. Idempotent and
// safe from any goroutine.
func (p *peerConn) CloseTakenOver() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.takeover = true
	close(p.done)
	p.mu.Unlock()
	// CloseNow tears the connection down immediately without a close
	// handshake. A graceful Close (which writes a close frame then waits to
	// read the peer's echo) cannot be used here: it is invoked from the
	// NEW connection's handshake goroutine while the OLD connection's serve
	// goroutine owns the reader, and the close-handshake read would race /
	// deadlock with that in-flight Read. The displaced client observes an
	// abnormal closure, which the adapter's reconnect logic (Task 9) treats
	// the same as any drop: redial + re-register.
	_ = p.ws.CloseNow()
}

// closeNormal tears the connection down for a non-takeover reason.
func (p *peerConn) closeNormal(code websocket.StatusCode, reason string) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.done)
	p.mu.Unlock()
	_ = p.ws.Close(code, reason)
}

// writeJSON marshals v and writes it as a single WS text message. Writes are
// serialised by wmu so the broker (which may push deliveries concurrently
// with handshake replies) never interleaves two WS writers.
func (p *peerConn) writeJSON(ctx context.Context, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	p.wmu.Lock()
	defer p.wmu.Unlock()
	return p.ws.Write(ctx, websocket.MessageText, b)
}

// ServeHTTP upgrades the request to WebSocket and runs the connection
// lifecycle: register handshake → registry bind → pending-queue flush → read
// loop (routing of post-handshake frames is Task 8; here the loop simply
// keeps the connection alive and exits cleanly on close/takeover).
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{})
	if err != nil {
		s.log.Warn("ws accept failed", "err", err)
		return
	}
	// Generous frame budget — envelopes carry opaque application JSON.
	ws.SetReadLimit(1 << 20)

	ctx := r.Context()
	pc := newPeerConn(ws)

	name, ok := s.handshake(ctx, pc)
	if !ok {
		// handshake already closed the WS with a specific status.
		return
	}

	defer s.registry.Remove(name, pc)
	defer pc.closeNormal(websocket.StatusNormalClosure, "bye") // close WS when serve returns

	s.serve(ctx, pc)
}

// handshake reads the first frame, which MUST be a wire.Register. It enforces
// the exact-match protocol version policy (wire.CheckVersion), validates the
// bearer token, then binds the name in the registry (same-token takeover /
// different-token reject) and flushes the recipient's offline+unacked queue
// so a message that arrived (or was requeued) during a takeover window is not
// lost. Returns the bound name and ok=true on success; on any failure it
// closes the WS with a descriptive status and returns ok=false.
func (s *Server) handshake(ctx context.Context, pc *peerConn) (string, bool) {
	typ, data, err := pc.ws.Read(ctx)
	if err != nil {
		pc.closeNormal(websocket.StatusProtocolError, "no handshake frame")
		return "", false
	}
	if typ != websocket.MessageText {
		pc.closeNormal(websocket.StatusUnsupportedData, "handshake must be text")
		return "", false
	}

	var reg wire.Register
	if err := json.Unmarshal(data, &reg); err != nil {
		pc.closeNormal(websocket.StatusInvalidFramePayloadData, "malformed handshake")
		return "", false
	}
	if reg.Type != wire.ControlRegister {
		pc.closeNormal(websocket.StatusPolicyViolation, "first frame must be register")
		return "", false
	}
	if err := wire.CheckVersion(reg.ProtocolVersion); err != nil {
		pc.closeNormal(websocket.StatusPolicyViolation, "unsupported protocol version")
		return "", false
	}
	if reg.Name == "" {
		pc.closeNormal(websocket.StatusPolicyViolation, "empty peer name")
		return "", false
	}
	if !s.auth.Valid(reg.Token) {
		pc.closeNormal(websocket.StatusPolicyViolation, "invalid bearer token")
		return "", false
	}

	// Bind in the registry: same-token => takeover (old conn closed by the
	// registry), different-token => reject.
	takenOver, _, err := s.registry.Bind(reg.Name, reg.Token, pc)
	if err != nil {
		if errors.Is(err, ErrNameClaimed) {
			pc.closeNormal(websocket.StatusPolicyViolation, "name claimed under a different token")
			return "", false
		}
		pc.closeNormal(websocket.StatusInternalError, "registry bind failed")
		return "", false
	}

	// Persist the peer durably and flush its queue. RequeueUnacked +
	// PendingFor together guarantee that a message enqueued or left
	// in-flight during a same-token takeover window falls to the offline/
	// pending store path and is delivered to the NEW connection.
	if err := s.store.Register(store.Peer{Name: reg.Name}); err != nil {
		s.registry.Remove(reg.Name, pc)
		pc.closeNormal(websocket.StatusInternalError, "peer persistence failed")
		return "", false
	}
	if _, err := s.store.RequeueUnacked(reg.Name); err != nil {
		s.log.Warn("requeue unacked failed", "name", reg.Name, "err", err)
	}

	// Acknowledge the handshake.
	ack := wire.Peers{
		ProtocolVersion: wire.ProtocolVersion,
		Type:            wire.ControlPeers,
		Names:           s.registry.List(),
	}
	if err := pc.writeJSON(ctx, ack); err != nil {
		s.registry.Remove(reg.Name, pc)
		pc.closeNormal(websocket.StatusInternalError, "handshake ack failed")
		return "", false
	}
	pc.name = reg.Name

	s.flushPending(ctx, pc)

	if takenOver {
		s.log.Info("peer name taken over (same token)", "name", reg.Name)
	} else {
		s.log.Info("peer registered", "name", reg.Name)
	}
	return reg.Name, true
}

// flushPending delivers every currently-pending message for the peer to its
// new connection and marks them delivered. This is the offline/pending store
// path: it is exactly what makes a takeover-race message (queued to the name
// while the old conn was being displaced) reach the new conn rather than
// being lost. Full ack/redelivery routing is Task 8; here we deliver + mark
// delivered so the takeover-race guarantee holds and is testable now.
func (s *Server) flushPending(ctx context.Context, pc *peerConn) {
	pend, err := s.store.PendingFor(pc.name)
	if err != nil {
		s.log.Warn("pending lookup failed", "name", pc.name, "err", err)
		return
	}
	for _, m := range pend {
		var env wire.Envelope
		if err := json.Unmarshal(m.Envelope, &env); err != nil {
			s.log.Warn("skip unparseable pending envelope", "id", m.ID, "err", err)
			continue
		}
		del := wire.Deliver{
			ProtocolVersion: wire.ProtocolVersion,
			Type:            wire.ControlDeliver,
			Envelope:        env,
		}
		if err := pc.writeJSON(ctx, del); err != nil {
			s.log.Warn("deliver pending failed", "id", m.ID, "err", err)
			return
		}
		if err := s.store.MarkDelivered(m.ID); err != nil {
			s.log.Warn("mark delivered failed", "id", m.ID, "err", err)
		}
		// Audit the flushed (re)delivery through the single broker-owned
		// Appender, same as a live deliverTo, so the chain reflects every
		// delivery including offline-drain and reconnect redelivery.
		s.auditEvent("deliver", m.ID, m.From, pc.name)
	}
}

// serve runs until the connection is closed (peer disconnect) or taken over.
// Each post-handshake frame is dispatched by routeFrame (router.go): a typed
// control object (ack/peers) or a data-channel Envelope (direct/broadcast).
// The loop exits promptly when the connection is torn down so the goroutine
// never leaks.
func (s *Server) serve(ctx context.Context, pc *peerConn) {
	for {
		select {
		case <-pc.done:
			return
		case <-ctx.Done():
			pc.closeNormal(websocket.StatusGoingAway, "server shutting down")
			return
		default:
		}
		typ, data, err := pc.ws.Read(ctx)
		if err != nil {
			// Closed (takeover, client disconnect, or context cancel).
			return
		}
		if typ != websocket.MessageText {
			// Only text JSON frames are routable; ignore anything else.
			continue
		}
		s.routeFrame(ctx, pc, data)
	}
}

// ListenAndServe binds cfg.ListenAddr and serves the broker until ctx is
// cancelled. It is the production entrypoint; tests use httptest with the
// Server's ServeHTTP directly.
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	hs := &http.Server{
		Addr:    addr,
		Handler: s,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}
	errCh := make(chan error, 1)
	go func() { errCh <- hs.ListenAndServe() }()
	select {
	case <-ctx.Done():
		_ = hs.Close()
		<-errCh
		return ctx.Err()
	case err := <-errCh:
		return fmt.Errorf("broker listen: %w", err)
	}
}
