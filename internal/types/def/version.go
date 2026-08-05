package def

import (
	"fmt"
	"strconv"
	"strings"
)

// versionPrefix is the letter a version is written with, so that a
// version segment in a path cannot be mistaken for anything else that
// happens to be a number.
const versionPrefix = "v"

// Version numbers a definition, monotonically per kind: v1, v2, v3.
//
// It is NOT the store's revision. A revision moves on every write to
// anything; a version moves when the shape of one kind changes, and an
// instance pins the version it was validated against. Confusing them
// would pin a resource to a moment in time instead of to a schema.
//
// It is not part of Definition either. A definition is a shape; the
// version is what the store called that shape when it accepted it. The
// old code had to zero the field out before comparing two definitions,
// which was the type saying it did not belong.
type Version uint32

// NoVersion is what a definition has before the store has seen it.
const NoVersion Version = 0

// ParseVersion reads a version back from a path segment.
//
// It is the inverse of String and refuses everything else — no bare
// number, no leading plus, no space to be lenient about. A version read
// wrong is a resource pinned to the wrong schema, so a caller guessing at
// the format gets an error rather than an off-by-one.
//
// Zero is refused too: NoVersion means "the store has not seen this", and
// a path segment saying so would be addressing a definition that by
// definition was never stored.
func ParseVersion(raw string) (Version, error) {
	digits, found := strings.CutPrefix(raw, versionPrefix)
	if !found {
		return NoVersion, fmt.Errorf("%w: %q does not begin with %q", ErrVersion, raw, versionPrefix)
	}

	parsed, err := strconv.ParseUint(digits, 10, 32)
	if err != nil {
		return NoVersion, fmt.Errorf("%w: %q", ErrVersion, raw)
	}

	if parsed == 0 {
		return NoVersion, fmt.Errorf("%w: %q names no version", ErrVersion, raw)
	}

	return Version(parsed), nil
}

// Next is the version after this one — what the store assigns when a
// kind's shape changes.
func (v Version) Next() Version { return v + 1 }

func (v Version) Eq(other Version) bool { return v == other }

// IsZero reports a definition the store has not accepted yet.
func (v Version) IsZero() bool { return v == NoVersion }

// String reads the way people write it: v1, v2.
func (v Version) String() string {
	return versionPrefix + strconv.FormatUint(uint64(v), 10)
}
