package broker

import "crypto/subtle"

// Authenticator validates static bearer tokens. A peer name is bindable only
// under a valid token (the registry additionally enforces that a takeover of
// an existing name must present the SAME token).
//
// The set of accepted tokens is fixed at construction (config/env, see
// Config); there is no dynamic token issuance in v1.
type Authenticator struct {
	tokens []string
}

// NewAuthenticator returns an Authenticator over the given accepted tokens.
func NewAuthenticator(tokens []string) *Authenticator {
	cp := make([]string, len(tokens))
	copy(cp, tokens)
	return &Authenticator{tokens: cp}
}

// Valid reports whether tok matches one of the configured bearer tokens. The
// comparison is constant-time per candidate so a caller cannot learn a valid
// token's length/prefix via timing.
func (a *Authenticator) Valid(tok string) bool {
	if tok == "" {
		return false
	}
	ok := false
	for _, t := range a.tokens {
		if subtle.ConstantTimeCompare([]byte(t), []byte(tok)) == 1 {
			ok = true
		}
	}
	return ok
}
