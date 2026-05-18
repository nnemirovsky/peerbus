package wire

import "encoding/json"

// Kind enumerates the envelope delivery semantics.
type Kind string

const (
	// KindMsg is a direct (to:<name>) message.
	KindMsg Kind = "msg"
	// KindBroadcast is a fan-out (to:*) message.
	KindBroadcast Kind = "broadcast"
)

// Envelope is the end-to-end peerbus message carried over the WS data channel
// as one newline-delimited JSON object. body is opaque application JSON: it is
// hashed verbatim and MUST NOT be re-encoded (re-marshalling opaque JSON is not
// byte-stable). hmac is the hex-encoded HMAC-SHA256 over Canonical(env).
type Envelope struct {
	ProtocolVersion string          `json:"protocol_version"`
	ID              string          `json:"id"`
	From            string          `json:"from"`
	To              string          `json:"to"`
	TS              string          `json:"ts"`
	Source          string          `json:"source"`
	Kind            Kind            `json:"kind"`
	Body            json.RawMessage `json:"body"`
	HMAC            string          `json:"hmac"`
}
