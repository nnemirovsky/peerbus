package wire

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// ProtocolVersion is the single supported wire protocol version. Policy is
// exact-match-or-reject (see CheckVersion): the field exists so future
// negotiation is additive, but v1 implements no negotiation engine.
const ProtocolVersion = "v1"

// ErrUnsupportedVersion is returned by CheckVersion for any non-matching value.
var ErrUnsupportedVersion = errors.New("wire: unsupported protocol version")

// CheckVersion enforces the v1 exact-match-or-reject policy. Any value other
// than the single supported ProtocolVersion constant is rejected; there is no
// negotiation.
func CheckVersion(v string) error {
	if v != ProtocolVersion {
		return fmt.Errorf("%w: got %q, want %q", ErrUnsupportedVersion, v, ProtocolVersion)
	}
	return nil
}

// canonicalEnvelope is the fixed-field-order signed subset of Envelope. It
// OMITS the hmac field (the signature cannot cover itself) and carries body as
// a raw json.RawMessage so the opaque application JSON is spliced in as-is and
// NEVER decoded-then-re-encoded (decoding into a map and re-marshalling sorts
// object keys and is not byte-stable across implementations / machines).
// encoding/json compacts insignificant whitespace in a RawMessage but never
// reorders members; compaction is deterministic and idempotent, so every
// party that calls Canonical on the same logical envelope — sender or
// recipient, any machine, any protocol_version — produces identical bytes.
//
// CANONICAL FIELD ORDER (load-bearing — the project's primary correctness
// property; do NOT reorder, add omitempty, or rename without bumping the
// protocol version and updating every adapter implementation):
//
//  1. protocol_version
//  2. id
//  3. from
//  4. to
//  5. ts
//  6. source
//  7. kind
//  8. body   (verbatim raw bytes, never re-encoded)
//
// encoding/json emits struct fields in declaration order, so this struct's
// field declaration order IS the canonical byte order. No omitempty on any
// signed field — every field is always present.
type canonicalEnvelope struct {
	ProtocolVersion string          `json:"protocol_version"`
	ID              string          `json:"id"`
	From            string          `json:"from"`
	To              string          `json:"to"`
	TS              string          `json:"ts"`
	Source          string          `json:"source"`
	Kind            Kind            `json:"kind"`
	Body            json.RawMessage `json:"body"`
}

// Canonical returns the deterministic byte representation of env that is fed to
// HMAC-SHA256. The sender signs Canonical(env); a recipient reconstructs it
// from the received wire bytes (marshal→unmarshal→Canonical→verify). Both must
// agree across machines and across protocol_version values. body is spliced in
// verbatim — it is never re-marshalled.
func Canonical(env Envelope) ([]byte, error) {
	body := env.Body
	if len(body) == 0 {
		// Absent body canonicalizes to JSON null so the field is always
		// present and stable.
		body = json.RawMessage("null")
	}
	c := canonicalEnvelope{
		ProtocolVersion: env.ProtocolVersion,
		ID:              env.ID,
		From:            env.From,
		To:              env.To,
		TS:              env.TS,
		Source:          env.Source,
		Kind:            env.Kind,
		Body:            body,
	}
	return json.Marshal(c)
}

// Encoder writes newline-delimited JSON objects (one per line) to an
// io.Writer. Safe for sequential use by a single goroutine.
type Encoder struct {
	w io.Writer
}

// NewEncoder returns an Encoder over w.
func NewEncoder(w io.Writer) *Encoder { return &Encoder{w: w} }

// Encode marshals v and writes it followed by a single '\n'. v must marshal to
// a single-line JSON object (encoding/json does not emit newlines).
func (e *Encoder) Encode(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = e.w.Write(b)
	return err
}

// Decoder reads newline-delimited JSON objects from an io.Reader.
type Decoder struct {
	s *bufio.Scanner
}

// NewDecoder returns a Decoder over r. Lines up to 1 MiB are supported.
func NewDecoder(r io.Reader) *Decoder {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	return &Decoder{s: s}
}

// Decode reads the next newline-delimited JSON object into v. It returns
// io.EOF when the stream is exhausted with no further line.
func (d *Decoder) Decode(v any) error {
	if !d.s.Scan() {
		if err := d.s.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	return json.Unmarshal(d.s.Bytes(), v)
}
