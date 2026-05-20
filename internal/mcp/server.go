// Package mcp is the stdio MCP server for the generic adapter, exposing the
// bus.* tools (bus.send / bus.broadcast / bus.peers / bus.drain).
//
// Transport / dependency choice (documented per Task 10):
//
// peerbus has NO MCP Go SDK as a dependency and we deliberately do not add
// one. The MCP wire protocol we need is a thin slice of JSON-RPC 2.0 over
// stdio — exactly three request methods (initialize, tools/list,
// tools/call) plus the initialized notification — so the minimal server is
// implemented directly here. Adding a heavy SDK to cover four tools would
// be unjustified weight for an open-source adapter binary; the protocol is
// small and stable enough to own. This keeps the binary dependency-light
// (rationale mirrors the coder/websocket choice documented in
// internal/broker/server.go).
//
// Framing: MCP stdio transport frames each JSON-RPC message. We accept both
// supported framings on input and emit newline-delimited JSON on output:
//
//   - newline-delimited JSON (one compact JSON object per line) — the
//     framing this server emits and the common stdio framing; and
//   - LSP-style "Content-Length: N\r\n\r\n<body>" headers — accepted on
//     input for hosts that prefer it.
//
// Concurrency: requests are handled one at a time off a single stdin
// reader (a stdio MCP server has exactly one peer, the host). Tool calls
// may block (bus.drain waits for the broker round-trip the host asked
// for); that is intentional and matches the host's synchronous tools/call
// expectation.
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
)

// JSON-RPC 2.0 error codes used by this server (subset of the spec plus the
// MCP convention of returning tool failures as isError results, not
// protocol errors — see callTool).
const (
	errParse          = -32700
	errInvalidRequest = -32600
	errMethodNotFound = -32601
	errInvalidParams  = -32602
	errInternal       = -32603
)

// protocolVersion is the MCP protocol version this server advertises. It is
// echoed from the client when the client sends one (forward-compatible);
// this is the fallback when the client omits it.
const protocolVersion = "2025-06-18"

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// rpcNotification is a server -> client JSON-RPC notification (no id, no
// result). It shares the same single serialised writer as responses so a
// push notification never interleaves with a tools/call reply on stdout.
// Added (additively, no fork) for the cc adapter's claude/channel push path
// — the generic adapter never emits notifications and is unaffected.
type rpcNotification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

// Server is the minimal stdio MCP server. It owns the JSON-RPC framing and
// dispatch; all bus behaviour is delegated to the injected Bus (the generic
// adapter wires a broker-backed Bus in internal/adapter/generic.go).
type Server struct {
	bus Bus

	wmu sync.Mutex // serialises writes to out
	out *bufio.Writer
	in  *bufio.Reader
	// inCloser is the underlying input if it is an io.Closer (os.Stdin is).
	// Serve closes it on ctx cancellation so the blocked, non-ctx-aware
	// readMessage on stdin unblocks and Serve returns promptly instead of
	// hanging until SIGKILL (the adapter shutdown-hang fix). nil when the
	// input is not a Closer (e.g. an io.Pipe reader in tests, which is
	// closed by the test harness on cancel anyway).
	inCloser io.Closer

	initialized bool
	// initializedCh is closed exactly once on the first
	// notifications/initialized client message. Exposed via Initialized()
	// so the cc adapter's self-announce can wait for the MCP handshake to
	// complete before pushing a server-initiated notification — Claude Code
	// silently drops claude/channel notifications received before the
	// client has signalled initialized (CHANNELS_SCHEMA.md §3). Lazily
	// allocated by Initialized() so the generic adapter (which never asks)
	// pays nothing.
	initializedCh   chan struct{}
	initializedOnce sync.Once
	initializedMu   sync.Mutex

	// serverName is the serverInfo.name advertised at initialize. Defaults
	// to the generic adapter name; the cc adapter overrides it.
	serverName string
	// extraCaps holds additional capabilities merged into the initialize
	// result's `capabilities` object (e.g. the cc adapter's
	// experimental["claude/channel"]). nil for the generic adapter — its
	// behaviour is byte-identical to before this field existed.
	extraCaps map[string]any
	// hideDrain omits bus.drain from tools/list. The cc adapter is
	// push-driven (inbound arrives as claude/channel notifications, not via
	// host-driven drain) so it must not advertise a no-op bus.drain. false
	// for the generic adapter — unchanged behaviour.
	hideDrain bool
}

// ServerOption configures a Server at construction. Options are purely
// additive — with no options the Server behaves exactly as the generic
// adapter always has (tools-only capability, generic serverInfo name).
type ServerOption func(*Server)

// WithServerName overrides the serverInfo.name advertised at initialize.
func WithServerName(name string) ServerOption {
	return func(s *Server) { s.serverName = name }
}

// channelCapabilityKey is the experimental capability key Claude Code's
// channels feature registers its push-wake listener under. It is fixed by
// the claude/channel schema (CHANNELS_SCHEMA.md); the cc adapter is the
// only caller and there is exactly one value, so this is a parameterless
// option rather than an open map.
const channelCapabilityKey = "claude/channel"

// WithChannelCapability advertises experimental["claude/channel"]={} in the
// initialize result's `capabilities` object (in addition to the
// always-present `tools`). The cc adapter uses this so Claude Code
// registers its claude/channel push-wake listener. The generic adapter
// never sets it (tools-only).
func WithChannelCapability() ServerOption {
	return func(s *Server) {
		s.extraCaps = map[string]any{
			"experimental": map[string]any{
				channelCapabilityKey: map[string]any{},
			},
		}
	}
}

// WithoutDrain omits bus.drain from tools/list. The cc adapter (push-driven
// via claude/channel notifications) uses this so it does not advertise a
// no-op drain tool.
func WithoutDrain() ServerOption {
	return func(s *Server) { s.hideDrain = true }
}

// NewServer builds a Server reading framed JSON-RPC from in and writing
// newline-delimited JSON-RPC to out, delegating tool calls to bus. Options
// are additive; with none it is the original generic-adapter server.
func NewServer(bus Bus, in io.Reader, out io.Writer, opts ...ServerOption) *Server {
	s := &Server{
		bus:        bus,
		in:         bufio.NewReader(in),
		out:        bufio.NewWriter(out),
		serverName: "peerbus-generic-adapter",
	}
	if c, ok := in.(io.Closer); ok {
		s.inCloser = c
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Notify emits a server -> client JSON-RPC notification (no id). It shares
// the Server's single serialised writer, so a push never interleaves with a
// tools/call reply. This is the additive server->client path the cc
// adapter's claude/channel push uses; the generic adapter never calls it.
func (s *Server) Notify(method string, params any) {
	b, err := json.Marshal(rpcNotification{JSONRPC: "2.0", Method: method, Params: params})
	if err != nil {
		return // a marshal failure on our own notification is unrecoverable; drop
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	_, _ = s.out.Write(b)
	_ = s.out.WriteByte('\n')
	_ = s.out.Flush()
}

// Initialized returns a channel that is closed once the client has sent the
// MCP notifications/initialized message (the standard MCP handshake
// completion signal). The channel is lazily allocated and idempotent: every
// caller observes the same channel and a second initialized notification is
// a no-op. The cc adapter waits on this before emitting its
// claude/channel self-announce notification — Claude Code silently drops
// server-initiated notifications that arrive before the client has signalled
// initialized (CHANNELS_SCHEMA.md §3). The generic adapter never asks and
// is unaffected.
func (s *Server) Initialized() <-chan struct{} {
	s.initializedMu.Lock()
	if s.initializedCh == nil {
		s.initializedCh = make(chan struct{})
		// A late Initialized() call after the client already signalled
		// initialized must observe an already-closed channel, otherwise the
		// caller would block forever on a signal that has already fired.
		if s.initialized {
			close(s.initializedCh)
		}
	}
	ch := s.initializedCh
	s.initializedMu.Unlock()
	return ch
}

// signalInitialized closes the initialized channel exactly once. Safe to
// call before or after Initialized(): if the channel hasn't been allocated
// yet, the next Initialized() call will see s.initialized=true and allocate
// an already-closed channel.
func (s *Server) signalInitialized() {
	s.initializedOnce.Do(func() {
		s.initializedMu.Lock()
		defer s.initializedMu.Unlock()
		if s.initializedCh != nil {
			close(s.initializedCh)
		}
	})
}

// Serve runs the read/dispatch loop until ctx is cancelled, stdin reaches
// EOF, or an unrecoverable framing error occurs. A clean EOF (host closed
// the pipe) returns nil; ctx cancellation returns ctx.Err().
//
// readMessage blocks on stdin and is NOT itself ctx-aware, so the loop must
// not call it inline and only check ctx at the top — on SIGTERM with stdin
// held open that would hang until SIGKILL (the adapter shutdown-hang bug).
// Instead each read runs in a goroutine feeding a channel and Serve selects
// on ctx.Done(); on cancellation it closes the underlying input (os.Stdin)
// so the in-flight read unblocks, then returns ctx.Err() promptly. The
// per-read goroutine is safe because reads are strictly sequential (one peer,
// one outstanding read at a time) — the next read is only started after the
// previous result is consumed.
func (s *Server) Serve(ctx context.Context) error {
	type readResult struct {
		raw []byte
		err error
	}
	for {
		if ctx.Err() != nil {
			s.closeInput()
			return ctx.Err()
		}
		ch := make(chan readResult, 1)
		go func() {
			raw, err := s.readMessage()
			ch <- readResult{raw, err}
		}()
		select {
		case <-ctx.Done():
			// Unblock the in-flight readMessage so its goroutine exits and
			// Serve returns immediately instead of hanging on idle stdin.
			s.closeInput()
			<-ch // reap the goroutine (read now fails on the closed input)
			return ctx.Err()
		case r := <-ch:
			if r.err != nil {
				if errors.Is(r.err, io.EOF) {
					return nil
				}
				// A read error after ctx cancellation is the expected
				// consequence of closeInput, not a real failure.
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return r.err
			}
			if len(r.raw) == 0 {
				continue
			}
			s.dispatch(ctx, r.raw)
		}
	}
}

// closeInput closes the underlying input if it is an io.Closer (os.Stdin in
// the real binary), unblocking a readMessage stuck on an idle stdin. A nil
// closer (non-Closer input, e.g. test pipes) is a no-op — those are closed by
// the harness on cancel. Safe to call more than once: an already-closed
// stdin just returns an error we ignore.
func (s *Server) closeInput() {
	if s.inCloser != nil {
		_ = s.inCloser.Close()
	}
}

// readMessage reads one framed JSON-RPC message. It auto-detects LSP-style
// Content-Length headers; otherwise it reads a single newline-delimited
// JSON line. Returns io.EOF when the input is exhausted.
func (s *Server) readMessage() ([]byte, error) {
	// Peek the first non-empty byte to decide the framing.
	for {
		b, err := s.in.Peek(1)
		if err != nil {
			return nil, err
		}
		if b[0] == '\n' || b[0] == '\r' {
			// Leading blank line between newline-delimited messages.
			if _, err := s.in.ReadByte(); err != nil {
				return nil, err
			}
			continue
		}
		break
	}

	b, err := s.in.Peek(1)
	if err != nil {
		return nil, err
	}
	if b[0] == 'C' { // "Content-Length:" — LSP-style framing.
		return s.readContentLengthFramed()
	}
	// Newline-delimited JSON.
	line, err := s.in.ReadBytes('\n')
	if err != nil && len(line) == 0 {
		return nil, err
	}
	return []byte(strings.TrimRight(string(line), "\r\n")), nil
}

// readContentLengthFramed reads an LSP-style header block followed by
// exactly Content-Length bytes of body.
func (s *Server) readContentLengthFramed() ([]byte, error) {
	contentLen := -1
	for {
		line, err := s.in.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break // end of headers
		}
		if k, v, ok := strings.Cut(line, ":"); ok && strings.EqualFold(strings.TrimSpace(k), "Content-Length") {
			n, perr := strconv.Atoi(strings.TrimSpace(v))
			if perr != nil {
				return nil, fmt.Errorf("mcp: bad Content-Length %q: %w", v, perr)
			}
			contentLen = n
		}
	}
	if contentLen < 0 {
		return nil, errors.New("mcp: Content-Length header missing")
	}
	body := make([]byte, contentLen)
	if _, err := io.ReadFull(s.in, body); err != nil {
		return nil, err
	}
	return body, nil
}

// dispatch parses one raw message and routes it. Notifications (no id) get
// no response; requests get exactly one response (result or error).
func (s *Server) dispatch(ctx context.Context, raw []byte) {
	var req rpcRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		s.writeError(nil, errParse, "parse error", err.Error())
		return
	}
	isNotification := len(req.ID) == 0

	switch req.Method {
	case "initialize":
		s.handleInitialize(req)
	case "notifications/initialized", "initialized":
		s.initialized = true // notification — no response
		s.signalInitialized()
	case "ping":
		if !isNotification {
			s.writeResult(req.ID, struct{}{})
		}
	case "tools/list":
		s.writeResult(req.ID, toolsListResult(s.hideDrain))
	case "tools/call":
		s.handleToolsCall(ctx, req)
	default:
		if !isNotification {
			s.writeError(req.ID, errMethodNotFound, "method not found", req.Method)
		}
	}
}

func (s *Server) handleInitialize(req rpcRequest) {
	var p struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	_ = json.Unmarshal(req.Params, &p) // params optional/forgiving
	pv := p.ProtocolVersion
	if pv == "" {
		pv = protocolVersion
	}
	caps := map[string]any{
		"tools": map[string]any{},
	}
	for k, v := range s.extraCaps {
		caps[k] = v
	}
	s.writeResult(req.ID, map[string]any{
		"protocolVersion": pv,
		"capabilities":    caps,
		"serverInfo": map[string]any{
			"name":    s.serverName,
			"version": "1",
		},
	})
}

func (s *Server) handleToolsCall(ctx context.Context, req rpcRequest) {
	if len(req.ID) == 0 {
		return // a tools/call without id is malformed; ignore (no response possible)
	}
	var call struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(req.Params, &call); err != nil {
		s.writeError(req.ID, errInvalidParams, "invalid params", err.Error())
		return
	}
	if call.Name == "" {
		s.writeError(req.ID, errInvalidParams, "invalid params", "tools/call: missing tool name")
		return
	}
	// MCP tools/call arguments MUST be a JSON object (or absent). Reject a
	// non-object (string/array/number) as invalid params before dispatch so
	// a malformed call is a protocol error, not a tool isError result.
	if a := bytesTrimSpace(call.Arguments); len(a) > 0 && string(a) != "null" && a[0] != '{' {
		s.writeError(req.ID, errInvalidParams, "invalid params", "tools/call: arguments must be an object")
		return
	}
	res, err := s.callTool(ctx, call.Name, call.Arguments)
	if err != nil {
		// Argument/availability problems are JSON-RPC errors; runtime tool
		// failures are surfaced as an isError tool result (see callTool).
		var argErr *toolArgError
		if errors.As(err, &argErr) {
			s.writeError(req.ID, errInvalidParams, "invalid params", err.Error())
			return
		}
		if errors.Is(err, errUnknownTool) {
			s.writeError(req.ID, errMethodNotFound, "unknown tool", call.Name)
			return
		}
		s.writeResult(req.ID, toolErrorResult(err.Error()))
		return
	}
	s.writeResult(req.ID, res)
}

// bytesTrimSpace trims leading ASCII JSON whitespace so the first
// significant byte can be inspected for the arguments-shape check.
func bytesTrimSpace(b []byte) []byte {
	i := 0
	for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
		i++
	}
	return b[i:]
}

func (s *Server) writeResult(id json.RawMessage, result any) {
	s.write(rpcResponse{JSONRPC: "2.0", ID: id, Result: result})
}

func (s *Server) writeError(id json.RawMessage, code int, msg string, data any) {
	s.write(rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: msg, Data: data}})
}

// write emits one newline-delimited JSON-RPC message and flushes so the
// host sees it immediately (stdio is unbuffered from the host's view).
func (s *Server) write(resp rpcResponse) {
	b, err := json.Marshal(resp)
	if err != nil {
		// Last-ditch: a marshal failure on our own response is an internal
		// bug; emit a minimal hand-built error so the host is not left
		// hanging on the request id.
		b = []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":` +
			strconv.Itoa(errInternal) + `,"message":"internal marshal error"}}`)
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	_, _ = s.out.Write(b)
	_ = s.out.WriteByte('\n')
	_ = s.out.Flush()
}
