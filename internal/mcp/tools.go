package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Bus is the broker-facing behaviour the MCP tools delegate to. The generic
// adapter implements it over the shared resuming broker client + shared
// dedupe cache (internal/adapter/generic.go) — the MCP layer never talks to
// the broker or touches HMAC/dedupe itself; it is purely the JSON-RPC tool
// surface. Keeping this an interface keeps the server unit-testable with a
// fake bus and keeps the "reuse, do not reimplement" boundary explicit.
type Bus interface {
	// Send signs (via the shared client's HMAC) and sends a direct message
	// to peer `to`. body is opaque application JSON, hashed verbatim.
	Send(ctx context.Context, to string, body json.RawMessage) error
	// Broadcast signs and fans a message out to every currently-registered
	// peer except this one (no backfill).
	Broadcast(ctx context.Context, body json.RawMessage) error
	// Peers returns the broker's current peer registry.
	Peers(ctx context.Context) ([]string, error)
	// Drain returns every inbound message buffered since the last drain —
	// already HMAC-verified and already filtered through the SHARED dedupe
	// cache — and acks each one back to the broker. A repeat delivery of an
	// id the host already drained is suppressed by the shared dedupe and
	// never reappears here.
	Drain(ctx context.Context) ([]InboundMessage, error)
}

// InboundMessage is one drained message as the host sees it. body is the
// opaque application JSON verbatim; source/from carry the envelope's
// provenance (source is e.g. "peer-bus" — the tag the consuming agent's
// own prompt keys its escalation policy off; peerbus itself has no such
// logic).
type InboundMessage struct {
	ID     string          `json:"id"`
	From   string          `json:"from"`
	Source string          `json:"source"`
	Body   json.RawMessage `json:"body"`
}

// errUnknownTool is returned by callTool for a tools/call naming a tool
// this server does not expose (mapped to a JSON-RPC method-not-found).
var errUnknownTool = errors.New("mcp: unknown tool")

// toolArgError marks a tool argument problem (missing/invalid args). It is
// mapped to a JSON-RPC invalid-params error, distinct from a runtime tool
// failure (which is an isError tool result).
type toolArgError struct{ msg string }

func (e *toolArgError) Error() string { return e.msg }

func argErrorf(format string, a ...any) error {
	return &toolArgError{msg: fmt.Sprintf(format, a...)}
}

// toolsListResult is the static tools/list payload. Schemas are minimal but
// accurate so a host can validate calls before sending them.
func toolsListResult() map[string]any {
	strProp := func(desc string) map[string]any {
		return map[string]any{"type": "string", "description": desc}
	}
	objProp := func(desc string) map[string]any {
		// body is opaque JSON: an object is the common case but we do not
		// over-constrain it (the bus hashes it verbatim).
		return map[string]any{"type": "object", "description": desc}
	}
	return map[string]any{
		"tools": []map[string]any{
			{
				"name":        "bus.send",
				"description": "Send a direct message to one peer on the bus.",
				"inputSchema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"to":   strProp("recipient peer name"),
						"body": objProp("opaque application JSON payload"),
					},
					"required": []string{"to", "body"},
				},
			},
			{
				"name":        "bus.broadcast",
				"description": "Broadcast a message to every currently-registered peer except yourself.",
				"inputSchema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"body": objProp("opaque application JSON payload"),
					},
					"required": []string{"body"},
				},
			},
			{
				"name":        "bus.peers",
				"description": "List the peers currently registered on the bus.",
				"inputSchema": map[string]any{
					"type":       "object",
					"properties": map[string]any{},
				},
			},
			{
				"name":        "bus.drain",
				"description": "Return and acknowledge all messages received since the last drain.",
				"inputSchema": map[string]any{
					"type":       "object",
					"properties": map[string]any{},
				},
			},
		},
	}
}

// toolResult wraps a JSON value as an MCP tool result. MCP tool results are
// a content array; we return one text content holding compact JSON plus a
// structuredContent mirror so structured hosts can read it directly.
func toolResult(v any) map[string]any {
	js, _ := json.Marshal(v)
	return map[string]any{
		"content": []map[string]any{
			{"type": "text", "text": string(js)},
		},
		"structuredContent": v,
		"isError":           false,
	}
}

// toolErrorResult is a successful JSON-RPC response carrying an MCP tool
// failure (isError: true) — per MCP, recoverable tool errors are reported
// in-band so the model can react, not as protocol errors.
func toolErrorResult(msg string) map[string]any {
	return map[string]any{
		"content": []map[string]any{
			{"type": "text", "text": msg},
		},
		"isError": true,
	}
}

// callTool decodes args for name, invokes the bus, and shapes the result.
// It returns:
//   - (*toolArgError) for bad/missing arguments  -> JSON-RPC invalid params
//   - errUnknownTool                              -> JSON-RPC method not found
//   - any other error                            -> in-band isError result
//   - nil error + result map                     -> normal tool result
func (s *Server) callTool(ctx context.Context, name string, args json.RawMessage) (map[string]any, error) {
	switch name {
	case "bus.send":
		var a struct {
			To   string          `json:"to"`
			Body json.RawMessage `json:"body"`
		}
		if err := unmarshalArgs(args, &a); err != nil {
			return nil, err
		}
		if a.To == "" {
			return nil, argErrorf("bus.send: %q is required", "to")
		}
		if len(a.Body) == 0 {
			return nil, argErrorf("bus.send: %q is required", "body")
		}
		if err := s.bus.Send(ctx, a.To, a.Body); err != nil {
			return nil, err
		}
		return toolResult(map[string]any{"sent": true, "to": a.To}), nil

	case "bus.broadcast":
		var a struct {
			Body json.RawMessage `json:"body"`
		}
		if err := unmarshalArgs(args, &a); err != nil {
			return nil, err
		}
		if len(a.Body) == 0 {
			return nil, argErrorf("bus.broadcast: %q is required", "body")
		}
		if err := s.bus.Broadcast(ctx, a.Body); err != nil {
			return nil, err
		}
		return toolResult(map[string]any{"broadcast": true}), nil

	case "bus.peers":
		names, err := s.bus.Peers(ctx)
		if err != nil {
			return nil, err
		}
		if names == nil {
			names = []string{}
		}
		return toolResult(map[string]any{"peers": names}), nil

	case "bus.drain":
		msgs, err := s.bus.Drain(ctx)
		if err != nil {
			return nil, err
		}
		if msgs == nil {
			msgs = []InboundMessage{}
		}
		return toolResult(map[string]any{"messages": msgs}), nil

	default:
		return nil, errUnknownTool
	}
}

// unmarshalArgs decodes a tools/call arguments object. Absent arguments are
// treated as an empty object (tools with no required args). A malformed
// arguments value is a tool-argument error (invalid params).
func unmarshalArgs(raw json.RawMessage, v any) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return argErrorf("invalid tool arguments: %v", err)
	}
	return nil
}
