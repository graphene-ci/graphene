package def

import (
	"fmt"

	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
)

// How a published shape is recorded. See head.go for why it is a second
// record and not a field of the first.
var (
	// PublishedKind names the record holding one published shape.
	PublishedKind = kind.MustNew("Definition")

	// PublishedShape addresses it by the kind it describes and the
	// version it is: /Process/v2.
	PublishedShape = path.MustNewTPath("kind", "version")
)

// PublishedPath addresses one published version of one kind.
//
// A KIND KEEPS ITS CASE; ITS PATH DOES NOT. A kind name is written once
// by whoever declares it and read by everyone after, so "KernelLease"
// stays spelled that way — but a path segment is folded, because sameness
// is the question a key asks. Both of these therefore address one record:
//
//	KernelLease  →  Definition/kernellease/v2
//	kernellease  →  Definition/kernellease/v2
//
// That is the answer and not the problem. Two kinds differing only in
// case would be impossible to talk about and miserable to work with, and
// this makes them impossible to HAVE: the second one declared collides
// with the first at the store and is refused, without anybody having to
// write a uniqueness check. Case-preserving and case-insensitive, the way
// a sane filesystem is.
//
// What it costs is that the true spelling cannot be read back out of a
// path. It does not have to be — it travels in the value, which is where
// a decoder takes it from.
func PublishedPath(named kind.Kind, version Version) (path.Path, error) {
	return PublishedShape.New(named.String(), version.String())
}

// Published is a definition together with the version the store gave it.
//
// The pair exists because a Definition deliberately has no version — it
// is a shape, and what the store called that shape is the store's
// business — while everything that STORES one needs both. A key for a
// definition is Kind plus /<kind>/<version>, so a codec asked to name a
// bare Definition could not: it would be holding half the answer.
//
// Keeping the version out here rather than putting it back inside is what
// leaves Definition.Eq able to ask the only question anyone asks of two
// definitions — do they describe the same shape — without first zeroing a
// field out, which is what the old code had to do.
type Published struct {
	definition Definition
	version    Version
}

// Publish pairs a definition with its version.
//
// Both are required. A zero definition describes nothing, and a zero
// version is what a definition has BEFORE the store has seen it — so
// publishing one would claim the store accepted something it never did.
func Publish(definition Definition, version Version) (Published, error) {
	if definition.IsZero() {
		return Published{}, ErrNoKind
	}

	if version.IsZero() {
		return Published{}, fmt.Errorf("%w: %s", ErrNoVersion, definition.Kind())
	}

	return Published{definition: definition, version: version}, nil
}

// Definition is the shape that was published.
func (p Published) Definition() Definition { return p.definition }

// Version is what the store called it.
func (p Published) Version() Version { return p.version }

// Kind is what it defines. Reached through often enough to be worth
// asking directly.
func (p Published) Kind() kind.Kind { return p.definition.Kind() }

// IsZero reports a pair that was never published.
func (p Published) IsZero() bool { return p.definition.IsZero() }

// Eq compares shape and version both. Two publications of the same shape
// at different versions are different records, because an instance pins
// the version and not the shape.
func (p Published) Eq(other Published) bool {
	return p.version.Eq(other.version) && p.definition.Eq(other.definition)
}

func (p Published) String() string { return p.definition.String() + " " + p.version.String() }
