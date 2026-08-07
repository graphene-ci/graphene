package resource

import "github.com/graphene-ci/graphene/internal/common/str"

// An author is who last wrote a record.
//
// The store already says WHEN — that is what a revision is — and said
// nothing about who, in a system whose whole subject is who may do what.
// "Who changed this" was unanswerable, and the only place it could have
// been answered from was a log nobody keeps forever.
//
// It is not a permission and grants nothing. Nothing is decided by
// reading it; it is what makes a decision somebody else made
// afterwards-explicable.
const maxAuthor = 128

//nolint:gochecknoglobals // a validated value cannot be a const; treated as one
var authorRules = []str.Rule{
	str.UTF8(),
	str.NFKC(),
	str.TrimSpace(),
	str.MaxBytes(maxAuthor),
	str.NoInvisible(),
}

// Author is who a write was made by.
type Author str.String

// NewAuthor names one.
//
// The EMPTY author is legal and means the kernel itself: a store being
// bootstrapped, a kernel writing down that it exists. Those writes have
// no caller, and inventing a name for one would be worse than saying
// there was none.
func NewAuthor(raw string) (Author, error) {
	if raw == "" {
		return NoAuthor, nil
	}

	return str.New[Author](raw, authorRules...)
}

// NoAuthor is the kernel itself.
const NoAuthor Author = ""

func (a Author) String() string { return string(a) }

// IsZero reports a write the kernel made on nobody's behalf.
func (a Author) IsZero() bool { return a == NoAuthor }
