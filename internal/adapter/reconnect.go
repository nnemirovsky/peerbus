package adapter

import (
	"context"
	"errors"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/nnemirovsky/peerbus/internal/wire"
)

// Reconnect / resume protocol — SPECIFIED behaviour (do not weaken).
//
// On a connection drop the client:
//
//  1. redials the broker and re-registers under the SAME name. Re-using
//     the name is the entire resume mechanism: it triggers the broker's
//     same-token takeover (the old, now-dead conn is displaced) plus the
//     PendingFor flush + RequeueUnacked from Tasks 7/8. Every message that
//     was delivered but not yet acked is therefore re-pushed to the new
//     connection by the broker.
//  2. does NOT track seq or any client-side cursor. Resume is 100% broker
//     redelivery of all unacked — the client never asks "give me from N".
//     The client's only job is to dedupe the redelivered duplicates.
//  3. funnels EVERY received message through the shared dedupe cache
//     BEFORE surfacing it to the host, and acks a message ONLY AFTER the
//     host has consumed it. A message delivered+consumed but whose ack was
//     lost to the drop is redelivered after reconnect; dedupe suppresses
//     it so the host sees each id exactly once even though the wire
//     delivered it more than once.
//
// Backoff: redial uses bounded exponential backoff with jitter so a flapping
// broker is not hammered, capped at maxBackoff.

const (
	baseBackoff = 100 * time.Millisecond
	maxBackoff  = 5 * time.Second

	// peersReplyTimeout bounds the wait for a broker peers reply. Without
	// it a peers reply lost across a reconnect would block until ctx
	// cancel (forever for a long-lived session). Shared by genericBus.Peers
	// and the cc bus.
	peersReplyTimeout = 5 * time.Second
)

// HandlerFunc consumes one already-deduped, HMAC-verified inbound
// envelope. It returns nil once the host has CONSUMED the message; the run
// loop acks only after a nil return (consume-then-ack ordering). A non-nil
// return means "not consumed" — the message is left unacked and will be
// redelivered (and re-deduped) on the next reconnect.
type HandlerFunc func(ctx context.Context, env wire.Envelope) error

// ResumingClient wraps a Client with the reconnect/resume loop and the
// mandatory shared dedupe cache. Both adapter modes (generic, cc) embed
// this — neither reimplements reconnect or dedupe.
type ResumingClient struct {
	cfg    ClientConfig
	dedupe *Dedupe
	rng    *rand.Rand

	// cur is the current live Client, replaced on each reconnect. It is an
	// atomic.Pointer so Client() (called from outbound send/broadcast/peers
	// on the host's goroutine) and connect() (the resume loop's goroutine)
	// have a happens-before edge: connect fully constructs the *Client then
	// publishes it with a single atomic Store; Client() observes either the
	// previous fully-constructed pointer or the new one, never a torn or
	// partially-published value. (CRITICAL-3 data-race fix.)
	cur atomic.Pointer[Client]
}

// NewResumingClient builds a ResumingClient. dedupeSize bounds the shared
// seen-id cache (non-positive => DefaultDedupeSize). The same cache
// instance spans every reconnect — that is exactly what lets it suppress a
// duplicate that arrives only because of a reconnect redelivery.
func NewResumingClient(cfg ClientConfig, dedupeSize int) *ResumingClient {
	return &ResumingClient{
		cfg:    cfg,
		dedupe: NewDedupe(dedupeSize),
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Dedupe exposes the shared cache so a mode that drains on its own
// schedule (the generic adapter's bus.drain) filters through the SAME
// instance the reconnect loop uses.
func (rc *ResumingClient) Dedupe() *Dedupe { return rc.dedupe }

// Client returns the current live underlying Client (for outbound
// send/broadcast/peers). It may be nil between a drop and a successful
// redial; callers should treat ErrNotConnected as transient.
func (rc *ResumingClient) Client() *Client { return rc.cur.Load() }

// connect (re)establishes a live Client with bounded backoff, returning
// once connected or when ctx is done.
func (rc *ResumingClient) connect(ctx context.Context) (*Client, error) {
	backoff := baseBackoff
	for {
		c := NewClient(rc.cfg)
		err := c.Connect(ctx)
		if err == nil {
			// Publish the fully-constructed, connected Client with a single
			// atomic Store (happens-before for any concurrent Client()).
			rc.cur.Store(c)
			return c, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Jittered bounded exponential backoff.
		jitter := time.Duration(rc.rng.Int63n(int64(backoff)/2 + 1))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff + jitter):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// surface runs one delivery through dedupe then (if new) the handler, and
// acks after the host consumes. A duplicate (already seen) is acked
// straight away without re-invoking the handler — the host already saw it;
// acking clears it from the broker's unacked set so it stops being
// redelivered. A handler error leaves the message unacked (redelivered +
// re-deduped next reconnect).
//
// Dedupe keys off the SIGNED envelope id (del.Envelope.ID): that is the
// stable per-message identity the host must see exactly once. For a
// broadcast every recipient gets the same original envelope id (the broker
// no longer rewrites it), so a recipient's own redelivery is correctly
// suppressed while distinct broadcasts still pass.
//
// Ack keys off the broker's per-recipient DeliveryKey (del.DeliveryKey):
// that is the durable row the broker tracks as unacked/redeliverable. For
// a direct message DeliveryKey == Envelope.ID; for a broadcast copy it is
// "<origID>|<recipient>". Acking by envelope id would never clear a
// broadcast row and the message would redeliver forever.
func (rc *ResumingClient) surface(ctx context.Context, c *Client, h HandlerFunc, del wire.Deliver) {
	env := del.Envelope
	if rc.dedupe.Seen(env.ID) {
		// Already surfaced to the host on an earlier delivery; just
		// (re)ack the per-recipient row so the broker stops redelivering.
		_ = c.Ack(ctx, del.DeliveryKey)
		return
	}
	if err := h(ctx, env); err != nil {
		// Not consumed — do NOT ack. It stays unacked and will be
		// redelivered on the next reconnect, where dedupe will see it as
		// new again only if it was never recorded. We recorded it via
		// Seen above; to preserve "redeliver until consumed" we must
		// forget it so a later redelivery is treated as new.
		rc.dedupe.forget(env.ID)
		return
	}
	// Consumed — ack the per-recipient row only now (consume-then-ack).
	_ = c.Ack(ctx, del.DeliveryKey)
}

// Run connects and pumps inbound deliveries through dedupe+handler until
// ctx is cancelled, transparently reconnecting (same-name re-register =>
// broker takeover + unacked redelivery) on any drop. It blocks; run it in
// its own goroutine. It returns ctx.Err() on cancellation.
func (rc *ResumingClient) Run(ctx context.Context, h HandlerFunc) error {
	for {
		c, err := rc.connect(ctx)
		if err != nil {
			return err
		}
		// Pump this connection until it drops.
		for {
			del, rerr := c.Recv(ctx)
			if rerr != nil {
				if errors.Is(rerr, ErrInboundHMAC) ||
					errors.Is(rerr, ErrMissingDeliveryKey) {
					// Forged/corrupt inbound, or a broker protocol
					// violation (empty delivery_key): drop it, keep pumping the
					// same connection (do NOT reconnect — the transport
					// is fine, only this one frame is bad).
					continue
				}
				// Any other read error => connection is gone. Break to
				// the outer loop to redial + re-register (resume).
				break
			}
			rc.surface(ctx, c, h, del)
		}
		c.Close()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// loop: reconnect + re-register (same name) => broker flushes all
		// unacked; dedupe suppresses anything the host already consumed.
	}
}

// forget removes id from the dedupe cache so a subsequent redelivery is
// treated as new. Used only when the host did NOT consume a message, so
// that "redeliver until consumed" holds.
func (d *Dedupe) forget(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, ok := d.idx[id]; ok {
		d.ll.Remove(el)
		delete(d.idx, id)
	}
}
