package resource

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/types/def"
)

// Admit turns what an author asked for into what the kernel will store.
//
// It is the only way a Resource comes into being from an intent, the way
// TPath.New is the only way a Path comes into being from strings. Passing
// through here is what a resource IS: something a kind described, whose
// spec that kind's schema accepted, whose generation somebody else
// counted and whose definition version somebody else pinned.
//
// previous is the resource being written over, or the zero Resource for a
// creation. It is a parameter and not a lookup because this function does
// not read anything — everything it needs is in front of it, so it is
// pure, and the same intent over the same previous always yields the same
// resource.
//
// The version is passed in rather than taken from the definition because
// a definition does not have one: it is a shape, and which version the
// store called that shape is the store's business.
func Admit(
	definition def.Definition,
	version def.Version,
	intent Intent,
	previous Resource,
) (Resource, error) {
	switch {
	case definition.IsZero():
		return Resource{}, ErrNoDefinition
	case version.IsZero():
		return Resource{}, fmt.Errorf("%w: %s", ErrNoVersion, definition.Kind())
	case intent.IsZero():
		return Resource{}, ErrNoIntent
	}

	if err := fits(definition, intent.id); err != nil {
		return Resource{}, err
	}

	if err := succeeds(previous, intent); err != nil {
		return Resource{}, err
	}

	if err := validate(SpecHalf, definition.Spec().Schema, intent.spec); err != nil {
		return Resource{}, fmt.Errorf("%s: %w", intent.id, err)
	}

	return Resource{
		intent:     intent,
		generation: nextGeneration(previous, intent),
		version:    version,
		// The status and the deletion mark are carried over untouched.
		// Neither is an author's to set, and an admission that reset them
		// would let any spec write erase a controller's report or revive
		// something already on its way out.
		status:   previous.status,
		deleting: previous.deleting,
	}, nil
}

// Report records what a controller found.
//
// Separate from Admit because it is a different party writing a different
// half under different permission, and because it must NOT move the
// generation: a status write that counted as intent would wake the
// controller that wrote it, forever.
func Report(definition def.Definition, current Resource, status *schemapb.StructValue) (Resource, error) {
	switch {
	case definition.IsZero():
		return Resource{}, ErrNoDefinition
	case current.IsZero():
		return Resource{}, ErrNoResource
	case status == nil:
		return Resource{}, ErrNoStatus
	}

	if err := fits(definition, current.Id()); err != nil {
		return Resource{}, err
	}

	if err := validate(StatusHalf, definition.Status().Schema, status); err != nil {
		return Resource{}, fmt.Errorf("%s: %w", current.Id(), err)
	}

	// current is a copy; assigning to it edits nothing the caller holds.
	current.status = proto.CloneOf(status)

	return current, nil
}

// MarkDeleting starts a deletion that has to wait.
//
// A resource with no finalizers is not marked, it is removed — so this
// refuses that case rather than leaving a tombstone nobody would ever
// clear. Marking one that is already marked changes nothing and is not an
// error: a second delete of the same thing is the same request.
func MarkDeleting(current Resource) (Resource, error) {
	if current.IsZero() {
		return Resource{}, ErrNoResource
	}

	if len(current.intent.finalizers) == 0 {
		return Resource{}, fmt.Errorf("%w: %s", ErrNoFinalizers, current.Id())
	}

	current.deleting = true

	return current, nil
}

// fits refuses an id that this definition does not describe.
//
// Both halves matter. The wrong kind would be validated by the wrong
// schema; the wrong shape would name a resource somewhere the kind never
// declared, and the key it encoded to would land in another kind's space.
func fits(definition def.Definition, id Id) error {
	if !id.Kind().Eq(definition.Kind()) {
		return fmt.Errorf("%w: %s against the definition of %s",
			ErrKindMismatch, id.Kind(), definition.Kind())
	}

	if !id.Path().Shape().Eq(definition.Shape()) {
		return fmt.Errorf("%w: %s is not %s", ErrShapeMismatch, id, definition.Shape())
	}

	return nil
}

// succeeds refuses an admission that does not follow from what is there.
//
// A creation follows from nothing and is always allowed; everything below
// is about the three ways an update can be a different resource wearing
// the same path.
func succeeds(previous Resource, intent Intent) error {
	if previous.IsZero() {
		return nil
	}

	if !previous.Id().Eq(intent.id) {
		return fmt.Errorf("%w: %s over %s", ErrIdChanged, intent.id, previous.Id())
	}

	if previous.deleting && !proto.Equal(previous.Spec(), intent.spec) {
		return fmt.Errorf("%w: %s", ErrDeleting, intent.id)
	}

	return nil
}

// nextGeneration counts intent and not writes: a creation starts at 1, a
// spec change moves it, and anything else — a finalizer removed, a status
// reported — carries the current value over.
//
// This is the whole of what lets a controller ignore the echo of its own
// status write: the revision moved, the generation did not, so there is
// nothing new to act on.
func nextGeneration(previous Resource, intent Intent) Generation {
	if previous.IsZero() {
		return NoGeneration.Next()
	}

	if proto.Equal(previous.Spec(), intent.spec) {
		return previous.generation
	}

	return previous.generation.Next()
}

// validate runs one half's values against that half's schema.
//
// Blocking and not Ok: schemapb grades its findings, and a warning is
// something to say rather than something to refuse over. The warnings are
// dropped here because there is nowhere yet to carry them back to the
// caller — when there is, this is where they leave from.
func validate(half Half, schema *schemapb.Schema, values *schemapb.StructValue) error {
	result, err := schema.Validate(values.ToGo())
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrSchemaBroken, half, err)
	}

	if !result.Blocking() {
		return nil
	}

	faults := make([]Fault, 0, len(result.GetErrors()))
	for _, found := range result.GetErrors() {
		faults = append(faults, Fault{Field: found.GetPath(), Code: found.GetCode().String()})
	}

	return InvalidError{Half: half, Faults: faults}
}
