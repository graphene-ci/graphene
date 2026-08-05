package kv

import (
	"slices"

	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Entry is one stored record as the store knows it.
type Entry struct {
	Key   Key
	Value []byte
	// Revision is the store revision of the last write to this key: the
	// CAS token for the next one.
	Revision revision.Revision
	// CreatedRevision is the revision the key was created at. It survives
	// updates and changes on delete-and-recreate, which is what tells a
	// record from the different one that later took its key.
	CreatedRevision revision.Revision
}

// IsZero reports an entry that was never read out of a store. Every
// stored record has a revision, so a zero one is a value nobody wrote.
func (e Entry) IsZero() bool { return e.Revision.IsZero() }

// Clone copies an entry's bytes out of the store's own memory.
//
// The same reason Key.Clone exists, and it matters more here: a value is
// the larger of the two and the one a caller keeps. An implementation
// that hands back the page it read from is correct exactly until the
// transaction ends, which is to say correct in every test that reads it
// immediately.
func (e Entry) Clone() Entry {
	e.Key = e.Key.Clone()
	e.Value = slices.Clone(e.Value)

	return e
}

// String names the entry and its revision, without its value: a value is
// bytes of unknown size and an error message is not where it goes.
func (e Entry) String() string { return e.Key.String() + "@" + e.Revision.String() }
