package hmac

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/nnemirovsky/peerbus/internal/wire"
)

// secret is a fixed >= MinSecretLen test key.
var secret = []byte("0123456789abcdef0123456789abcdef") // 32 bytes

func sampleEnvelope() wire.Envelope {
	return wire.Envelope{
		ProtocolVersion: wire.ProtocolVersion,
		ID:              "01HZ0000000000000000000000",
		From:            "alice",
		To:              "bob",
		TS:              "2026-05-18T12:00:00Z",
		Source:          "peer-bus",
		Kind:            wire.KindMsg,
		Body:            json.RawMessage(`{"hello":"world"}`),
	}
}

func TestSignVerifyRoundTrip(t *testing.T) {
	env, err := SignEnvelope(secret, sampleEnvelope())
	if err != nil {
		t.Fatalf("SignEnvelope: %v", err)
	}
	if env.HMAC == "" {
		t.Fatal("SignEnvelope did not set HMAC")
	}
	if err := VerifyEnvelope(secret, env); err != nil {
		t.Fatalf("VerifyEnvelope on freshly signed envelope: %v", err)
	}
}

func TestSignDoesNotMutateInput(t *testing.T) {
	in := sampleEnvelope()
	if _, err := SignEnvelope(secret, in); err != nil {
		t.Fatalf("SignEnvelope: %v", err)
	}
	if in.HMAC != "" {
		t.Fatalf("input envelope was mutated: HMAC=%q", in.HMAC)
	}
}

func TestVerifyFailures(t *testing.T) {
	signed, err := SignEnvelope(secret, sampleEnvelope())
	if err != nil {
		t.Fatalf("SignEnvelope: %v", err)
	}

	tamperBody := signed
	tamperBody.Body = json.RawMessage(`{"hello":"WORLD"}`)

	tamperFrom := signed
	tamperFrom.From = "mallory"

	tamperTo := signed
	tamperTo.To = "carol"

	missingHMAC := signed
	missingHMAC.HMAC = ""

	badHex := signed
	badHex.HMAC = "zzzz"

	cases := []struct {
		name   string
		secret []byte
		env    wire.Envelope
		want   error
	}{
		{"tampered body", secret, tamperBody, ErrVerify},
		{"tampered from", secret, tamperFrom, ErrVerify},
		{"tampered to", secret, tamperTo, ErrVerify},
		{"wrong secret", []byte("ffffffffffffffffffffffffffffffff"), signed, ErrVerify},
		{"missing hmac", secret, missingHMAC, ErrMissingHMAC},
		{"bad hex hmac", secret, badHex, ErrBadHMACHex},
		{"short secret", []byte("too-short"), signed, ErrShortSecret},
		{"nil secret", nil, signed, ErrShortSecret},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Verify(tc.secret, tc.env)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Verify: got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestSignShortSecretRejected(t *testing.T) {
	for _, s := range [][]byte{nil, {}, []byte("short")} {
		if _, err := Sign(s, sampleEnvelope()); !errors.Is(err, ErrShortSecret) {
			t.Fatalf("Sign(%q): got %v, want ErrShortSecret", s, err)
		}
	}
	// Exactly MinSecretLen must be accepted.
	if _, err := Sign(bytes.Repeat([]byte("a"), MinSecretLen), sampleEnvelope()); err != nil {
		t.Fatalf("Sign with min-length secret: %v", err)
	}
}

// TestRecipientPath proves cross-machine canonicalization: sign, marshal to
// wire bytes, unmarshal a fresh envelope, and verify the recovered envelope.
// Repeated across distinct protocol_version values.
func TestRecipientPath(t *testing.T) {
	versions := []string{wire.ProtocolVersion, "v99-future"}
	for _, ver := range versions {
		t.Run(ver, func(t *testing.T) {
			env := sampleEnvelope()
			env.ProtocolVersion = ver

			signed, err := SignEnvelope(secret, env)
			if err != nil {
				t.Fatalf("SignEnvelope: %v", err)
			}

			// The WS transport carries exactly one JSON object per
			// message, so a plain marshal→unmarshal is the accurate
			// recipient-path round-trip.
			b, err := json.Marshal(signed)
			if err != nil {
				t.Fatalf("wire marshal: %v", err)
			}
			var recovered wire.Envelope
			if err := json.Unmarshal(b, &recovered); err != nil {
				t.Fatalf("wire unmarshal: %v", err)
			}

			if err := VerifyEnvelope(secret, recovered); err != nil {
				t.Fatalf("verify recipient-path envelope (version %s): %v", ver, err)
			}
		})
	}
}
