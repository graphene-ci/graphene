package blob

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/graphene-ci/graphene/internal/common/str"
)

// An id is issued by the store and typed by nobody. It is opaque on
// purpose: a caller that computed a checksum has not thereby learned
// where anything is, so two blobs with the same bytes are two blobs and
// deleting one does not delete the other.
//
// It is still constrained, and the reason is not tidiness. An id becomes
// a filename, and a filename is where "../" stops being a curiosity. The
// alphabet below has no separator, no dot and no case to fold, so an id
// names one file in one directory and can name nothing else.
const (
	// idBytes is the randomness in one id. Sixteen because ids are
	// guessed at by anyone who can read a spec they are written in, and
	// because a collision here would silently hand somebody else's bytes
	// to whoever asked.
	idBytes = 16
	maxId   = idBytes * 2
)

//nolint:gochecknoglobals // a validated value cannot be a const; treated as one
var idRules = []str.Rule{
	str.NotEmpty(),
	str.ASCII(),
	str.MaxBytes(maxId),
	str.MinBytes(maxId),
	str.Allow("id_is_lowercase_hex", isLowerHex),
}

// Id is where bytes are.
type Id str.String

// NewId checks an id that arrived from somewhere — a spec, a command
// line, a wire.
func NewId(raw string) (Id, error) {
	return str.New[Id](raw, idRules...)
}

// Issue mints one. Only a store calls it: an id that exists is an id some
// store made, which is what "opaque handle" has to mean if a caller is
// never to construct one that happens to work.
func Issue() (Id, error) {
	raw := make([]byte, idBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("blob: issue id: %w", err)
	}

	return Id(hex.EncodeToString(raw)), nil
}

func (i Id) String() string { return string(i) }

func isLowerHex(candidate rune) bool {
	return (candidate >= '0' && candidate <= '9') || (candidate >= 'a' && candidate <= 'f')
}
