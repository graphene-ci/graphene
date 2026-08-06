package link

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/graphene-ci/graphene/internal/common/str"
)

// A pin says WHICH kernel, and it is the whole of how one is recognized.
//
// There is no certificate authority here and no plan for one. A CA
// answers "is this certificate signed by somebody I trust", which is the
// right question when there are thousands of names nobody enumerated; a
// fleet is a list somebody wrote down, so the question is "is this the
// kernel I was told about", and the shortest true answer to that is the
// hash of its key.
//
// The cost is honest and worth saying: replacing a kernel's key means
// telling whoever points at it. That is a line in a file, and the thing
// it buys is that nothing has to be trusted to issue anything.
const (
	// digestLength is a SHA-256 written out.
	digestLength = sha256.Size * 2
	// prefix names the hash, so a pin cannot be silently reinterpreted
	// the day another one is added.
	prefix = "sha256:"

	maxPin = len(prefix) + digestLength
)

//nolint:gochecknoglobals // a validated value cannot be a const; treated as one
var pinRules = []str.Rule{
	str.NotEmpty(),
	str.ASCII(),
	str.TrimSpace(),
	str.MaxBytes(maxPin),
	str.MinBytes(maxPin),
	str.Allow("pin_is_lowercase_hex_after_its_hash_name", isPinRune),
}

// Pin is a kernel's key, named by its hash.
type Pin str.String

// NewPin checks one that arrived from a file or a command line.
//
// The two halves are checked separately because they are two different
// things: the first names the hash and has to be exactly the one this
// program uses, and the rest is that hash written out. A rule list can
// only say what characters are allowed anywhere, which would let the name
// of the hash appear in the middle of the digest.
func NewPin(raw string) (Pin, error) {
	pinned, err := str.New[Pin](raw, pinRules...)
	if err != nil {
		return "", err
	}

	digest, named := strings.CutPrefix(string(pinned), prefix)
	if !named || len(digest) != digestLength {
		return "", errNotAPin
	}

	if _, err := hex.DecodeString(digest); err != nil {
		return "", fmt.Errorf("%w: %w", errNotAPin, err)
	}

	return pinned, nil
}

// PinOf is the pin of a certificate — what its holder must be told to
// expect.
//
// Over the public key and not over the certificate, so a kernel can renew
// its certificate — a new expiry, a new address in it — without every
// client that points at it having to be edited. What is being recognized
// is the key, and the key is what does not change.
func PinOf(certificate *x509.Certificate) (Pin, error) {
	encoded, err := x509.MarshalPKIXPublicKey(certificate.PublicKey)
	if err != nil {
		return "", errUnusableKey
	}

	sum := sha256.Sum256(encoded)

	return Pin(prefix + hex.EncodeToString(sum[:])), nil
}

func (p Pin) String() string { return string(p) }

// IsZero reports a pin nobody stated.
func (p Pin) IsZero() bool { return p == "" }

// Eq compares two pins. Not in constant time, and deliberately: a pin is
// public — printed, pasted, committed — so there is no secret here for a
// timing difference to leak.
func (p Pin) Eq(other Pin) bool { return p == other }

// isPinRune admits the alphabet of "sha256:" and of a hex digest, which
// is one alphabet: the name of a hash and the hash itself, and nothing
// that could make a pin mean two things.
func isPinRune(candidate rune) bool {
	switch {
	case candidate >= '0' && candidate <= '9',
		candidate >= 'a' && candidate <= 'z',
		candidate == ':':
		return true
	default:
		return false
	}
}

// Errors a pin can be.
var (
	// errNotAPin — the right shape, the wrong hash, or no hash named.
	errNotAPin = errors.New("a pin is sha256: followed by a hex digest")
	// errUnusableKey — a certificate whose key cannot be encoded, which
	// means it cannot be recognized either.
	errUnusableKey = errors.New("the certificate carries a key that cannot be hashed")
)
