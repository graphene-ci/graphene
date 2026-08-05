package store

import "github.com/graphene-ci/graphene/internal/types/revision"

// Value is a value as the store hands it back: the value itself, and the
// two revisions the store keeps ABOUT it.
//
// The stamps are out here rather than inside the value on purpose, and
// the old code is the argument. Its resource envelope carried its own
// revision, so every write had to scrub it first:
//
//	stored.Revision = 0
//	stored.CreatedRevision = 0
//
// Two lines that had to be remembered at every place that wrote, and that
// went wrong by being forgotten rather than by being wrong. A value that
// cannot hold a revision cannot hold a stale one.
//
// One generic and not a stamped Resource plus a stamped Definition: what
// the store knows about a value is the same two numbers whatever the
// value is, and that is exactly what a type parameter is for.
//
// The fields are exported because there is nothing here to protect. Only
// the store fills one in, and it lives in another package, so private
// fields would need an exported constructor that anybody could call —
// hiding nothing at the price of three accessors.
type Value[T any] struct {
	// Value is what was stored. It knows nothing of the two numbers below,
	// which is the point.
	Value T

	// Revision is the store revision of the last write to this key. It is
	// the CAS token: it goes back out unchanged with the next write, and a
	// mismatch means somebody wrote in between.
	Revision revision.Revision

	// CreatedRevision is the revision the key was created at. It survives
	// every update and changes when a key is deleted and made again, which
	// is what makes it the way to tell a resource from the different one
	// that later took its path.
	CreatedRevision revision.Revision
}

// IsNew reports a value that was never written — the zero Value, as
// opposed to anything that came out of the store.
func (s Value[T]) IsNew() bool { return s.Revision.IsZero() }
