// Package hmac implements HMAC-SHA256 sign/verify over the wire canonical
// envelope form (see internal/wire.Canonical). Integrity is end-to-end: a
// recipient reconstructs the canonical bytes from the received wire envelope
// and verifies them, so a compromised broker cannot forge a peer.
package hmac

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/nnemirovsky/peerbus/internal/wire"
)

// MinSecretLen is the minimum accepted shared-secret length in bytes.
//
// Rationale: HMAC-SHA256's security is bounded by the entropy of the key. A
// key shorter than the 32-byte SHA-256 block-output size offers materially
// less than the construction's full strength and invites trivially
// brute-forceable secrets (typos, placeholders, "changeme"). 32 bytes (256
// bits) matches the HMAC-SHA256 output size and is the standard floor for a
// secret distributed out-of-band; we reject anything shorter rather than
// silently signing with a weak key.
const MinSecretLen = 32

var (
	// ErrShortSecret is returned when the shared secret is missing or shorter
	// than MinSecretLen.
	ErrShortSecret = fmt.Errorf("hmac: secret must be at least %d bytes", MinSecretLen)
	// ErrMissingHMAC is returned when an envelope carries no hmac field.
	ErrMissingHMAC = errors.New("hmac: envelope has no hmac")
	// ErrBadHMACHex is returned when the envelope hmac is not valid hex.
	ErrBadHMACHex = errors.New("hmac: envelope hmac is not valid hex")
	// ErrVerify is returned when the recomputed MAC does not match the one
	// carried in the envelope.
	ErrVerify = errors.New("hmac: signature mismatch")
)

func checkSecret(secret []byte) error {
	if len(secret) < MinSecretLen {
		return ErrShortSecret
	}
	return nil
}

// Sign computes the hex-encoded HMAC-SHA256 over wire.Canonical(env) using
// secret. The env.HMAC field is ignored (Canonical omits it) and not mutated;
// use SignEnvelope to also set it.
func Sign(secret []byte, env wire.Envelope) (string, error) {
	if err := checkSecret(secret); err != nil {
		return "", err
	}
	canon, err := wire.Canonical(env)
	if err != nil {
		return "", fmt.Errorf("hmac: canonicalize: %w", err)
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(canon)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// Verify recomputes the HMAC over wire.Canonical(env) and compares it, in
// constant time, against env.HMAC. It returns nil only on an exact match.
func Verify(secret []byte, env wire.Envelope) error {
	if err := checkSecret(secret); err != nil {
		return err
	}
	if env.HMAC == "" {
		return ErrMissingHMAC
	}
	want, err := hex.DecodeString(env.HMAC)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrBadHMACHex, err)
	}
	canon, err := wire.Canonical(env)
	if err != nil {
		return fmt.Errorf("hmac: canonicalize: %w", err)
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(canon)
	got := mac.Sum(nil)
	if !hmac.Equal(got, want) {
		return ErrVerify
	}
	return nil
}

// SignEnvelope signs env and writes the hex MAC into env.HMAC, returning the
// updated envelope. The input is taken by value so the caller's copy is
// untouched until it assigns the result.
func SignEnvelope(secret []byte, env wire.Envelope) (wire.Envelope, error) {
	sig, err := Sign(secret, env)
	if err != nil {
		return env, err
	}
	env.HMAC = sig
	return env, nil
}

// VerifyEnvelope verifies env.HMAC against the MAC recomputed from the
// canonical form of env. It is a thin alias of Verify expressing the
// recipient-side intent at call sites.
func VerifyEnvelope(secret []byte, env wire.Envelope) error {
	return Verify(secret, env)
}
