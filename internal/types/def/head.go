package def

import (
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
)

// A kind is recorded twice, under two kinds of its own:
//
//	Kind        /Process       the definition that is CURRENT
//	Definition  /Process/v2    the definition as it was at version 2
//
// Two records and not one because they are ADDRESSED differently: a head
// is one record per kind name, a published shape is one per version of
// that name. Keeping them apart is what makes "the current definition" a
// single Get on an exact key, rather than a scan for the largest of
// something — and the largest is not cheap here, because the byte layer
// walks forward only and a key sorts "v10" before "v2".
//
// The head holds the whole definition and not a pointer to a version.
// That costs one duplicate per kind — units of kilobytes across dozens of
// kinds — and buys two things. Reading the current definition is one Get
// instead of two, on the hottest path there is: every admission asks for
// it. And deleting an old version stops being dangerous, because there is
// no pointer left to dangle at it.
//
// The head is written AFTER the version it copies, never before. Crash in
// between and there is an orphan version nobody looks at, which is
// harmless; the other order would leave the current definition of a kind
// half-written. One line of ordering instead of a transaction in the
// port.
var (
	// HeadKind names the record holding a kind's current definition.
	HeadKind = kind.MustNew("Kind")

	// HeadShape addresses one by name: /Process.
	HeadShape = path.MustNewTPath("kind")
)

// Head is the definition of a kind that is current.
//
// It is a type of its own rather than a Published stored under a
// different key, and the difference is that the compiler can tell them
// apart. Both live in the same key space over the same byte store, and
// they differ only in where a value belongs — which is a codec's business
// and invisible in a signature. Store[Published] and Store[Published]
// would be one type, so wiring the wrong one in would compile and would
// write every version over the head.
//
// It EMBEDS rather than converts, so a head is not worse than a
// definition to hold, only more specific: Kind, Version, Definition and
// the rest come along. By value and not by pointer, because Published has
// no nil to carry and a head should not invent one.
type Head struct {
	Published
}

// NewHead marks a published definition as the current one.
func NewHead(published Published) (Head, error) {
	if published.IsZero() {
		return Head{}, ErrNoKind
	}

	return Head{Published: published}, nil
}

// Eq shadows the promoted comparison, which would otherwise take a
// Published and refuse the head beside it.
func (h Head) Eq(other Head) bool { return h.Published.Eq(other.Published) }

// HeadPath addresses the head record of one kind.
//
// It returns a path and not an id because resource.Id is built on top of
// this package and cannot be named from inside it. That turns out to be
// the right split anyway: filling a shape is what can fail, and naming a
// record once the path exists cannot.
func HeadPath(named kind.Kind) (path.Path, error) {
	return HeadShape.New(named.String())
}
