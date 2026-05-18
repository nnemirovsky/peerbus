// Package adapter holds the broker-facing pieces every adapter mode shares:
// the WebSocket client to the broker (this file), the reconnect/resume
// protocol (reconnect.go), the mandatory consumer-side dedupe cache
// (dedupe.go), and the additive --adapter mode dispatch table (mode.go).
//
// The broker is 100% agent-agnostic; all per-mode behaviour (MCP stdio
// server for generic, claude/channel for cc) is layered ON TOP of this
// client in Tasks 10/11. Those modes reuse this client and this dedupe —
// they do not reimplement either.
//
// Transport / frame model (mirrors internal/broker/server.go): each
// control frame and each Envelope is ONE WebSocket text message holding a
// single JSON object. coder/websocket is the client lib — the same library
// the broker and its tests use (pure-Go, no cgo, context-aware).
package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/coder/websocket"

	"github.com/nnemirovsky/peerbus/internal/hmac"
	"github.com/nnemirovsky/peerbus/internal/wire"
)

// ClientConfig is the static configuration for a broker WS client. The
// HMAC secret is shared out-of-band (never sent on the wire — register
// carries only the bearer token + chosen name).
type ClientConfig struct {
	// URL is the broker ws:// or wss:// endpoint.
	URL string
	// Token is the static bearer token presented at register.
	Token string
	// Name is the unique peer name to bind. Re-registering under the SAME
	// name after a drop is what triggers the broker's same-token takeover
	// + PendingFor flush (see reconnect.go).
	Name string
	// HMACSecret signs every outbound envelope and verifies every inbound
	// one. Must be at least hmac.MinSecretLen bytes.
	HMACSecret []byte
}

var (
	// ErrNotConnected is returned by send/broadcast/peers when the client
	// has no live broker connection.
	ErrNotConnected = errors.New("adapter: not connected to broker")
	// ErrInboundHMAC is returned (and the message dropped) when an inbound
	// delivered envelope fails HMAC verification — a compromised broker
	// cannot forge a peer, so a bad signature is rejected, never surfaced.
	ErrInboundHMAC = errors.New("adapter: inbound envelope failed HMAC verify")
)

// Client is a single broker WebSocket connection for one peer. It performs
// the register handshake, signs/sends outbound envelopes, verifies inbound
// deliveries end-to-end, and acks only AFTER the host has consumed a
// message. Reconnect/resume is layered on top in reconnect.go; this type
// models exactly one connection attempt's lifecycle.
//
// Concurrency: WS writes are serialised by wmu so a delivery-loop ack
// never interleaves with a host-driven send.
type Client struct {
	cfg ClientConfig

	mu   sync.Mutex
	ws   *websocket.Conn
	open bool

	wmu sync.Mutex // serialises WS writers (one frame at a time)
}

// NewClient constructs a Client over cfg. It does not dial; call Connect.
func NewClient(cfg ClientConfig) *Client {
	return &Client{cfg: cfg}
}

// Connect dials the broker and performs the register handshake (token +
// chosen name). The HMAC secret is validated locally but never sent. On
// success the connection is live and Recv can be pumped; the broker's
// handshake reply (a wire.Peers frame) plus any immediately-flushed
// pending deliveries follow on the same connection and are surfaced
// through Recv like any other frame.
func (c *Client) Connect(ctx context.Context) error {
	if len(c.cfg.HMACSecret) < hmac.MinSecretLen {
		return hmac.ErrShortSecret
	}
	ws, _, err := websocket.Dial(ctx, c.cfg.URL, nil)
	if err != nil {
		return fmt.Errorf("adapter: dial: %w", err)
	}
	ws.SetReadLimit(1 << 20)

	reg := wire.Register{
		ProtocolVersion: wire.ProtocolVersion,
		Type:            wire.ControlRegister,
		Token:           c.cfg.Token,
		Name:            c.cfg.Name,
	}
	if err := writeJSON(ctx, ws, &c.wmu, reg); err != nil {
		_ = ws.CloseNow()
		return fmt.Errorf("adapter: register write: %w", err)
	}
	// First frame back is the broker's handshake ack (wire.Peers). Reading
	// it here confirms the register was accepted (a rejected register
	// closes the WS, surfacing as a read error) and consumes the ack so
	// the Recv loop starts on the first real deliver/peers reply.
	typ, data, err := ws.Read(ctx)
	if err != nil {
		_ = ws.CloseNow()
		return fmt.Errorf("adapter: register rejected or no ack: %w", err)
	}
	if typ != websocket.MessageText {
		_ = ws.CloseNow()
		return errors.New("adapter: handshake ack not text")
	}
	ct, _ := wire.ControlTypeOf(data)
	if ct != wire.ControlPeers {
		_ = ws.CloseNow()
		return fmt.Errorf("adapter: unexpected handshake ack type %q", ct)
	}

	c.mu.Lock()
	c.ws = ws
	c.open = true
	c.mu.Unlock()
	return nil
}

// conn returns the live connection or nil.
func (c *Client) conn() *websocket.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.open {
		return nil
	}
	return c.ws
}

// Close tears the connection down (normal closure). Idempotent.
func (c *Client) Close() {
	c.mu.Lock()
	ws := c.ws
	c.open = false
	c.ws = nil
	c.mu.Unlock()
	if ws != nil {
		_ = ws.Close(websocket.StatusNormalClosure, "bye")
	}
}

// send signs and writes one envelope (kind decides direct vs broadcast).
func (c *Client) sendEnvelope(ctx context.Context, env wire.Envelope) error {
	ws := c.conn()
	if ws == nil {
		return ErrNotConnected
	}
	signed, err := hmac.SignEnvelope(c.cfg.HMACSecret, env)
	if err != nil {
		return fmt.Errorf("adapter: sign outbound: %w", err)
	}
	if err := writeJSON(ctx, ws, &c.wmu, signed); err != nil {
		return fmt.Errorf("adapter: send write: %w", err)
	}
	return nil
}

// Send signs and sends a direct message to peer `to`. id/ts/source are
// supplied by the caller (the adapter mode owns id generation); body is
// opaque application JSON, hashed verbatim.
func (c *Client) Send(ctx context.Context, id, to, ts, source string, body json.RawMessage) error {
	return c.sendEnvelope(ctx, wire.Envelope{
		ProtocolVersion: wire.ProtocolVersion,
		ID:              id,
		From:            c.cfg.Name,
		To:              to,
		TS:              ts,
		Source:          source,
		Kind:            wire.KindMsg,
		Body:            body,
	})
}

// Broadcast signs and sends a to:* fan-out message. Same field ownership
// as Send; the broker fans it out to every currently-registered peer
// except this sender (no backfill).
func (c *Client) Broadcast(ctx context.Context, id, ts, source string, body json.RawMessage) error {
	return c.sendEnvelope(ctx, wire.Envelope{
		ProtocolVersion: wire.ProtocolVersion,
		ID:              id,
		From:            c.cfg.Name,
		To:              "*",
		TS:              ts,
		Source:          source,
		Kind:            wire.KindBroadcast,
		Body:            body,
	})
}

// Peers requests the broker's current registry and returns the peer names.
// It writes a peers control frame then reads frames until the peers reply
// arrives. Any deliver frames seen while waiting are returned so the caller
// (the reconnect loop) does not drop them.
func (c *Client) Peers(ctx context.Context) (names []string, strays []wire.Deliver, err error) {
	ws := c.conn()
	if ws == nil {
		return nil, nil, ErrNotConnected
	}
	req := wire.Peers{
		ProtocolVersion: wire.ProtocolVersion,
		Type:            wire.ControlPeers,
	}
	if err := writeJSON(ctx, ws, &c.wmu, req); err != nil {
		return nil, nil, fmt.Errorf("adapter: peers write: %w", err)
	}
	for {
		typ, data, err := ws.Read(ctx)
		if err != nil {
			return nil, strays, fmt.Errorf("adapter: peers read: %w", err)
		}
		if typ != websocket.MessageText {
			continue
		}
		ct, _ := wire.ControlTypeOf(data)
		switch ct {
		case wire.ControlPeers:
			var p wire.Peers
			if err := json.Unmarshal(data, &p); err != nil {
				return nil, strays, fmt.Errorf("adapter: peers decode: %w", err)
			}
			return p.Names, strays, nil
		case wire.ControlDeliver:
			var d wire.Deliver
			if err := json.Unmarshal(data, &d); err == nil {
				strays = append(strays, d)
			}
		}
	}
}

// Recv blocks for the next inbound deliver frame, HMAC-verifies it
// end-to-end, and returns the carried envelope. A frame that fails
// verification is REJECTED (never surfaced) — the function returns
// ErrInboundHMAC so the caller can log/drop it and the loop continues. The
// broker handshake ack is consumed in Connect, so non-deliver frames here
// are skipped (a peers reply can arrive if Peers raced).
func (c *Client) Recv(ctx context.Context) (wire.Envelope, error) {
	ws := c.conn()
	if ws == nil {
		return wire.Envelope{}, ErrNotConnected
	}
	for {
		typ, data, err := ws.Read(ctx)
		if err != nil {
			return wire.Envelope{}, err
		}
		if typ != websocket.MessageText {
			continue
		}
		ct, _ := wire.ControlTypeOf(data)
		if ct != wire.ControlDeliver {
			// peers reply / other control noise — not a delivery.
			continue
		}
		var del wire.Deliver
		if err := json.Unmarshal(data, &del); err != nil {
			return wire.Envelope{}, fmt.Errorf("adapter: deliver decode: %w", err)
		}
		if err := hmac.VerifyEnvelope(c.cfg.HMACSecret, del.Envelope); err != nil {
			// Reject — a compromised broker cannot forge a peer. Surface
			// the typed error with the offending id so the caller can
			// drop+log and keep pumping.
			return del.Envelope, fmt.Errorf("%w (id=%q): %v", ErrInboundHMAC, del.Envelope.ID, err)
		}
		return del.Envelope, nil
	}
}

// Ack acknowledges that the host has CONSUMED the message with id. It is
// sent only after consumption: until the broker receives this ack the
// message stays unacked and WILL be redelivered on reconnect (which the
// dedupe cache then suppresses). This is the load-bearing ordering of the
// at-least-once model.
func (c *Client) Ack(ctx context.Context, id string) error {
	ws := c.conn()
	if ws == nil {
		return ErrNotConnected
	}
	ack := wire.Ack{
		ProtocolVersion: wire.ProtocolVersion,
		Type:            wire.ControlAck,
		ID:              id,
	}
	if err := writeJSON(ctx, ws, &c.wmu, ack); err != nil {
		return fmt.Errorf("adapter: ack write: %w", err)
	}
	return nil
}

// writeJSON marshals v and writes it as one WS text message, serialised by
// mu so concurrent senders/ackers never interleave a WS writer.
func writeJSON(ctx context.Context, ws *websocket.Conn, mu *sync.Mutex, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	mu.Lock()
	defer mu.Unlock()
	return ws.Write(ctx, websocket.MessageText, b)
}
