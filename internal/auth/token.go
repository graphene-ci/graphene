package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"strings"
)

// A token is the caller's name and a secret, joined.
//
//	agent-local.9f2c8a…
//
// The name is in it on purpose. Without it, finding out who a token
// belongs to means reading every identity there is and comparing against
// each — a scan of the whole kind on every single request. With it, the
// lookup is one read of an exact key, which is the same shape everything
// else in this system has.
//
// What the name does NOT do is prove anything. It says which identity to
// check against; the secret is what is checked. A caller who changes the
// name is a caller checking their secret against somebody else's digests,
// which fails.
const separator = "."

// digestBytes is how much of a hash is kept. The whole of it: a truncated
// digest is a smaller haystack for anybody who steals the store.
const digestBytes = sha256.Size

var (
	// ErrMalformedToken — a credential that is not one. Told apart from a
	// wrong one only in the log; a caller hears the same thing either way.
	ErrMalformedToken = errors.New("malformed token")
	// ErrBadToken — a credential that does not match.
	ErrBadToken = errors.New("token does not match")
)

// Split takes a token apart.
func Split(token string) (string, string, error) {
	name, secret, found := strings.Cut(strings.TrimSpace(token), separator)
	if !found || name == "" || secret == "" {
		return "", "", ErrMalformedToken
	}

	return name, secret, nil
}

// Digest is what an identity stores instead of a secret.
//
// A digest and not the secret itself, so that reading an identity — which
// anybody granted `get` on it can do — hands out nothing that could be
// used to become it. That is the whole reason identities are readable at
// all without being dangerous.
func Digest(secret string) string {
	sum := sha256.Sum256([]byte(secret))

	return hex.EncodeToString(sum[:])
}

// Matches reports whether a secret is one of the ones an identity knows.
//
// Constant time, and per candidate rather than short-circuiting on the
// first mismatch. Comparing digests with == would leak how much of one
// matched through how long the comparison took, and an attacker who can
// measure that can find a digest one byte at a time.
func Matches(secret string, digests []string) bool {
	want := []byte(Digest(secret))
	found := false

	for _, digest := range digests {
		if len(digest) != hex.EncodedLen(digestBytes) {
			continue
		}

		// No break: leaving early would say which of the digests matched
		// by how long the loop ran.
		if subtle.ConstantTimeCompare(want, []byte(digest)) == 1 {
			found = true
		}
	}

	return found
}
