// Package resource is what an instance of a kind IS, and the one door
// through which one is made.
//
// It holds two types, and the split between them is the whole design. An
// Intent is what an author asks for. A Resource is what the kernel made
// of that ask. Nothing turns the first into the second but Admit, which
// needs a definition — so a resource cannot exist without a kind having
// described it, in the same way a Path cannot exist without a TPath.
//
// The old code had one type for both, with twelve fields written by four
// different parties: the author, the controller, the kernel, and the
// store. Which of them a given write was allowed to touch lived in the
// service's checks rather than in the types, so "a client cannot set its
// own generation" was a rule someone had to remember rather than a thing
// that could not be expressed.
package resource

import (
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/gopherex/schemapb/go/schemapb"
)

// Intent is what an author asks for: which resource, what its spec should
// be, and the two pieces of bookkeeping an author owns.
//
// What is NOT here is the point. There is no generation to send, no
// definition version to pin, no deleting flag to set, no status to
// forge — not because those are ignored on input, but because this type
// does not have them. A client holding an Intent cannot express them.
type Intent struct {
	id         Id
	spec       *schemapb.StructValue
	finalizers []Finalizer
}

// IntentOption is one of the parts an author may set beyond the spec.
// An option and not a parameter because most writes set none.
type IntentOption func(*Intent)

// WithFinalizers records the claims that must be released before this
// resource may actually be removed.
func WithFinalizers(finalizers ...Finalizer) IntentOption {
	return func(in *Intent) { in.finalizers = slices.Clone(finalizers) }
}

// NewIntent states an ask.
//
// The spec is required even when it is empty, and an empty spec is
// written as an empty struct rather than as nothing: "a resource with no
// fields" and "a resource whose spec nobody sent" are different mistakes
// and only one of them is a mistake.
//
// The spec is COPIED in. A protobuf message is a pointer, and an intent
// that kept the caller's would be an immutable value with a mutable
// inside — the caller could go on editing what it had already submitted.
func NewIntent(id Id, spec *schemapb.StructValue, options ...IntentOption) (Intent, error) {
	switch {
	case id.IsZero():
		return Intent{}, ErrNoId
	case !id.IsExact():
		return Intent{}, fmt.Errorf("%w: %s names a subtree", ErrNotExact, id)
	case spec == nil:
		return Intent{}, fmt.Errorf("%w: %s", ErrNoSpec, id)
	}

	stated := Intent{id: id, spec: proto.CloneOf(spec)}
	for _, option := range options {
		option(&stated)
	}

	if err := checkFinalizers(stated.finalizers); err != nil {
		return Intent{}, fmt.Errorf("%s: %w", id, err)
	}

	return stated, nil
}

// checkFinalizers refuses an unnamed claim and the same claim twice.
//
// Twice matters more than it looks: removing a finalizer is how deletion
// proceeds, and a claim listed twice would be removed once and still be
// there, leaving a resource that can never finish being deleted.
func checkFinalizers(finalizers []Finalizer) error {
	for index, finalizer := range finalizers {
		if finalizer.IsZero() {
			return fmt.Errorf("%w: finalizer %d", ErrNoFinalizer, index)
		}

		if slices.Contains(finalizers[:index], finalizer) {
			return fmt.Errorf("%w: %s", ErrDuplicateFinalizer, finalizer)
		}
	}

	return nil
}

// Id is which resource is being asked for.
func (in Intent) Id() Id { return in.id }

// Spec is the asked-for state. The message is not copied on the way out —
// cloning on every read would cost an allocation on the hottest path
// there is — so it is to be read and not written.
func (in Intent) Spec() *schemapb.StructValue { return in.spec }

// Finalizers are the claims on this resource's deletion.
func (in Intent) Finalizers() []Finalizer { return slices.Clone(in.finalizers) }

// IsZero reports an intent that was never stated.
func (in Intent) IsZero() bool { return in.id.IsZero() }

func (in Intent) String() string { return in.id.String() }
