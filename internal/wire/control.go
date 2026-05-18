package wire

import "encoding/json"

// Control message types. These are the JSON-RPC-ish typed control frames
// exchanged on the WS control path (distinct from data-channel Envelopes).
// Every control message carries protocol_version so the exact-match-or-reject
// policy (see CheckVersion) applies uniformly.

// ControlType discriminates a control frame.
type ControlType string

const (
	// ControlRegister is sent by an adapter to bind a peer name under a token.
	ControlRegister ControlType = "register"
	// ControlAck acknowledges consumption of a delivered message id.
	ControlAck ControlType = "ack"
	// ControlPeers requests / returns the current peer registry.
	ControlPeers ControlType = "peers"
	// ControlDeliver carries an Envelope from the broker to a recipient.
	ControlDeliver ControlType = "deliver"
)

// Register binds a unique peer name under a bearer token. The HMAC secret is
// shared out-of-band, never sent on the wire.
type Register struct {
	ProtocolVersion string      `json:"protocol_version"`
	Type            ControlType `json:"type"`
	Token           string      `json:"token"`
	Name            string      `json:"name"`
}

// Ack acknowledges that the recipient has consumed the message with ID.
type Ack struct {
	ProtocolVersion string      `json:"protocol_version"`
	Type            ControlType `json:"type"`
	ID              string      `json:"id"`
}

// Peers is the registry request/response. When sent by an adapter Names is
// empty; the broker replies with the currently-registered peer names.
type Peers struct {
	ProtocolVersion string      `json:"protocol_version"`
	Type            ControlType `json:"type"`
	Names           []string    `json:"names"`
}

// Deliver wraps an Envelope pushed from the broker to a recipient.
type Deliver struct {
	ProtocolVersion string      `json:"protocol_version"`
	Type            ControlType `json:"type"`
	Envelope        Envelope    `json:"envelope"`
}

// rawControl is used to peek the discriminator before full decode.
type rawControl struct {
	Type ControlType `json:"type"`
}

// ControlTypeOf returns the ControlType discriminator of a raw control frame.
func ControlTypeOf(b []byte) (ControlType, error) {
	var rc rawControl
	if err := json.Unmarshal(b, &rc); err != nil {
		return "", err
	}
	return rc.Type, nil
}
