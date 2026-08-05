package store

import "github.com/graphene-ci/graphene/internal/store/kv"

// EventKind is what happened to a record.
//
// An alias rather than a type of its own, so a value crosses between the
// two layers without a conversion and a switch written against one
// compiles against the other. Restated here for the same reason the
// errors beside it are: a file that reacts to a change should not have to
// import the package that knows about pages.
type EventKind = kv.EventKind

// The two things that can happen.
const (
	// EventPut — the record was written, whether it existed before or not.
	EventPut = kv.EventPut
	// EventDelete — the record was removed. The event still carries what
	// it last was, because whoever filters a stream has to be able to ask
	// what went away.
	EventDelete = kv.EventDelete
)

// Event is one change, decoded.
type Event[T any] struct {
	Kind EventKind
	// Value is what the record became, or — on a delete — what it last
	// was.
	Value Value[T]
}

// IsZero reports an event nobody filled in. A stream never delivers one.
func (e Event[T]) IsZero() bool { return e.Kind.IsZero() }
