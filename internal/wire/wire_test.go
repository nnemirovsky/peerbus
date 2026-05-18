package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

func sampleEnvelope() Envelope {
	return Envelope{
		ProtocolVersion: ProtocolVersion,
		ID:              "01HV000000000000000000000A",
		From:            "alice",
		To:              "bob",
		TS:              "2026-05-18T12:00:00Z",
		Source:          "peer-bus",
		Kind:            KindMsg,
		Body:            json.RawMessage(`{"z":1,"a":"hello","nested":{"k":[1,2,3]}}`),
		HMAC:            "deadbeef",
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	in := sampleEnvelope()
	var buf bytes.Buffer
	if err := NewEncoder(&buf).Encode(in); err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !strings.HasSuffix(buf.String(), "\n") {
		t.Fatalf("encoded frame must be newline-terminated")
	}
	var out Envelope
	if err := NewDecoder(&buf).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.ID != in.ID || out.From != in.From || out.To != in.To ||
		out.Kind != in.Kind || out.HMAC != in.HMAC ||
		out.ProtocolVersion != in.ProtocolVersion {
		t.Fatalf("envelope mismatch: %+v vs %+v", out, in)
	}
	if !bytes.Equal(out.Body, in.Body) {
		t.Fatalf("body not preserved verbatim: %s vs %s", out.Body, in.Body)
	}
}

func TestMultipleFramesPerStream(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	a := sampleEnvelope()
	b := sampleEnvelope()
	b.ID = "01HV000000000000000000000B"
	if err := enc.Encode(a); err != nil {
		t.Fatal(err)
	}
	if err := enc.Encode(b); err != nil {
		t.Fatal(err)
	}
	dec := NewDecoder(&buf)
	var x, y Envelope
	if err := dec.Decode(&x); err != nil {
		t.Fatal(err)
	}
	if err := dec.Decode(&y); err != nil {
		t.Fatal(err)
	}
	if x.ID != a.ID || y.ID != b.ID {
		t.Fatalf("frame order/identity wrong: %q %q", x.ID, y.ID)
	}
	if err := dec.Decode(&Envelope{}); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF after last frame, got %v", err)
	}
}

func TestControlRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  any
		typ  ControlType
	}{
		{"register", Register{ProtocolVersion: ProtocolVersion, Type: ControlRegister, Token: "tok", Name: "alice"}, ControlRegister},
		{"ack", Ack{ProtocolVersion: ProtocolVersion, Type: ControlAck, ID: "id-1"}, ControlAck},
		{"peers", Peers{ProtocolVersion: ProtocolVersion, Type: ControlPeers, Names: []string{"a", "b"}}, ControlPeers},
		{"deliver", Deliver{ProtocolVersion: ProtocolVersion, Type: ControlDeliver, Envelope: sampleEnvelope()}, ControlDeliver},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := NewEncoder(&buf).Encode(tc.msg); err != nil {
				t.Fatalf("encode: %v", err)
			}
			line := bytes.TrimRight(buf.Bytes(), "\n")
			got, err := ControlTypeOf(line)
			if err != nil {
				t.Fatalf("ControlTypeOf: %v", err)
			}
			if got != tc.typ {
				t.Fatalf("discriminator = %q, want %q", got, tc.typ)
			}
			switch tc.typ {
			case ControlRegister:
				var r Register
				if err := NewDecoder(&buf).Decode(&r); err != nil {
					t.Fatal(err)
				}
				if r.Name != "alice" || r.Token != "tok" {
					t.Fatalf("register round-trip mismatch: %+v", r)
				}
			case ControlAck:
				var a Ack
				if err := NewDecoder(&buf).Decode(&a); err != nil {
					t.Fatal(err)
				}
				if a.ID != "id-1" {
					t.Fatalf("ack round-trip mismatch: %+v", a)
				}
			case ControlPeers:
				var p Peers
				if err := NewDecoder(&buf).Decode(&p); err != nil {
					t.Fatal(err)
				}
				if len(p.Names) != 2 {
					t.Fatalf("peers round-trip mismatch: %+v", p)
				}
			case ControlDeliver:
				var d Deliver
				if err := NewDecoder(&buf).Decode(&d); err != nil {
					t.Fatal(err)
				}
				if d.Envelope.ID != sampleEnvelope().ID {
					t.Fatalf("deliver round-trip mismatch: %+v", d)
				}
			}
		})
	}
}

func TestCheckVersion(t *testing.T) {
	tests := []struct {
		name    string
		version string
		wantErr bool
	}{
		{"exact match accepted", ProtocolVersion, false},
		{"empty rejected", "", true},
		{"different version rejected", "v2", true},
		{"prefix not accepted", "v1.0", true},
		{"case sensitive", "V1", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckVersion(tc.version)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.version)
				}
				if !errors.Is(err, ErrUnsupportedVersion) {
					t.Fatalf("error should wrap ErrUnsupportedVersion, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.version, err)
			}
		})
	}
}

func mustCanonical(t *testing.T, env Envelope) []byte {
	t.Helper()
	b, err := Canonical(env)
	if err != nil {
		t.Fatalf("Canonical: %v", err)
	}
	return b
}

// (a) two independent marshals of the same envelope must be byte-identical.
func TestCanonicalStableAcrossIndependentMarshals(t *testing.T) {
	env := sampleEnvelope()
	c1 := mustCanonical(t, env)
	c2 := mustCanonical(t, env)
	if !bytes.Equal(c1, c2) {
		t.Fatalf("independent marshals differ:\n%s\n%s", c1, c2)
	}
	// hmac must not appear in the canonical form.
	if bytes.Contains(c1, []byte("hmac")) || bytes.Contains(c1, []byte("deadbeef")) {
		t.Fatalf("canonical form must omit hmac: %s", c1)
	}
}

// (b) the recipient path: a struct vs the same struct after wire
// marshal→unmarshal must canonicalize to identical bytes.
func TestCanonicalStableAcrossWireRoundTrip(t *testing.T) {
	env := sampleEnvelope()
	senderCanon := mustCanonical(t, env)

	var buf bytes.Buffer
	if err := NewEncoder(&buf).Encode(env); err != nil {
		t.Fatalf("encode: %v", err)
	}
	var received Envelope
	if err := NewDecoder(&buf).Decode(&received); err != nil {
		t.Fatalf("decode: %v", err)
	}
	recipientCanon := mustCanonical(t, received)

	if !bytes.Equal(senderCanon, recipientCanon) {
		t.Fatalf("sender vs recipient canonical differ:\n%s\n%s", senderCanon, recipientCanon)
	}
}

// (c) two envelopes differing ONLY in protocol_version (all other signed
// fields equal): their Canonical outputs must differ (version is signed) and
// each must itself be stable.
func TestCanonicalDiffersByProtocolVersionAndIsStable(t *testing.T) {
	base := sampleEnvelope()
	v1 := base
	v1.ProtocolVersion = "v1"
	v2 := base
	v2.ProtocolVersion = "v2"

	c1a := mustCanonical(t, v1)
	c1b := mustCanonical(t, v1)
	c2a := mustCanonical(t, v2)
	c2b := mustCanonical(t, v2)

	if !bytes.Equal(c1a, c1b) {
		t.Fatalf("v1 canonical not stable:\n%s\n%s", c1a, c1b)
	}
	if !bytes.Equal(c2a, c2b) {
		t.Fatalf("v2 canonical not stable:\n%s\n%s", c2a, c2b)
	}
	if bytes.Equal(c1a, c2a) {
		t.Fatalf("canonical must differ when protocol_version differs:\n%s", c1a)
	}
}

func TestCanonicalBodyKeyOrderPreserved(t *testing.T) {
	// The opaque body is spliced as the raw json.RawMessage value (not
	// decoded into a map and re-encoded, which would sort keys). Key order
	// of the original bytes must survive: "z" before "a". encoding/json
	// compacts insignificant whitespace in a RawMessage but never reorders
	// object members, and compaction is deterministic + idempotent — that
	// is the byte-stability property tests (a)/(b)/(c) rely on.
	env := sampleEnvelope()
	env.Body = json.RawMessage(`{ "z":1, "a":2 }`)
	c := mustCanonical(t, env)
	if !bytes.Contains(c, []byte(`"body":{"z":1,"a":2}`)) {
		t.Fatalf("body key order not preserved (must not be re-sorted): %s", c)
	}
	// A map-decoded body of the same content would emit sorted keys.
	if bytes.Contains(c, []byte(`{"a":2,"z":1}`)) {
		t.Fatalf("body appears re-marshalled via a map (keys sorted): %s", c)
	}
}

func TestCanonicalEmptyBodyIsNull(t *testing.T) {
	env := sampleEnvelope()
	env.Body = nil
	c := mustCanonical(t, env)
	if !bytes.HasSuffix(c, []byte(`"body":null}`)) {
		t.Fatalf("empty body must canonicalize to null: %s", c)
	}
}

func TestDecodeMalformedLine(t *testing.T) {
	r := strings.NewReader("{not valid json}\n")
	var env Envelope
	if err := NewDecoder(r).Decode(&env); err == nil {
		t.Fatalf("expected error decoding malformed JSON line")
	}
}

func TestDecodeMissingRequiredFields(t *testing.T) {
	// A JSON object with no envelope fields decodes structurally but yields
	// zero values; CheckVersion then rejects the (empty) protocol_version.
	r := strings.NewReader(`{}` + "\n")
	var env Envelope
	if err := NewDecoder(r).Decode(&env); err != nil {
		t.Fatalf("structurally-valid empty object should decode: %v", err)
	}
	if err := CheckVersion(env.ProtocolVersion); err == nil {
		t.Fatalf("missing protocol_version must be rejected by CheckVersion")
	}
	if env.ID != "" || env.From != "" {
		t.Fatalf("expected zero-value required fields, got %+v", env)
	}
}

func TestControlTypeOfMalformed(t *testing.T) {
	if _, err := ControlTypeOf([]byte("not json")); err == nil {
		t.Fatalf("expected error for malformed control frame")
	}
}
