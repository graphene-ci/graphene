package resource

import (
	"fmt"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/types/def"
)

// Snapshot is a resource laid flat, which is the shape it has to take to
// come back from bytes.
//
// It exists because Restore has to fill in fields no author may set, and
// eight positional arguments is a function nobody calls correctly. Every
// field is exported: a snapshot is a carrier on its way into Restore and
// has no life of its own.
type Snapshot struct {
	Id         Id
	Spec       *schemapb.StructValue
	Status     *schemapb.StructValue
	Finalizers []Finalizer
	Generation Generation
	Version    def.Version
	Deleting   bool
	Author     Author
}

// A canary, and the only unkeyed literal in the codebase.
//
// Everything that takes a resource apart and puts it back together —
// internal/convert today, whatever writes yaml tomorrow — is
// field-for-field with the struct above. Go has no way to make a
// converter fail when a field appears, so a field added and forgotten
// would go silently unwritten: a record that loses something on every
// round trip and says nothing about it.
//
// An unkeyed literal is the one thing that DOES stop compiling. When this
// line breaks, the fix is not here — it is in every converter, and this
// comment is the list of them.
//
// It is here rather than in the converter because vet refuses unkeyed
// literals of another package's struct, and it is right to: this is the
// one place the exception is earned.
var _ = Snapshot{Id{}, nil, nil, nil, 0, 0, false, ""}

// Restore rebuilds a resource that was admitted once already — read back
// from the store, or decoded off the wire.
//
// It is the second door into Resource and the honest name for it. Admit
// cannot serve here: it would recount the generation, and a resource that
// recounted its generation every time it was read would wake every
// controller watching it, every read.
//
// What it does NOT do is re-validate the spec, and that is deliberate.
// The resource pins the definition version it was admitted under; the
// definition may be two versions on by now, and checking a v1 resource
// against a v3 schema would refuse a value that was correct when it was
// written and is still what is stored. Validation belongs at the door
// values come IN through, which is Admit.
//
// What it does check is the structure — that the pieces make a resource
// at all — because a decoder handed corrupt bytes should say so here and
// not by producing a resource that quietly lies about itself.
func Restore(snapshot Snapshot) (Resource, error) {
	switch {
	case snapshot.Id.IsZero():
		return Resource{}, ErrNoId
	case !snapshot.Id.IsExact():
		return Resource{}, fmt.Errorf("%w: %s names a subtree", ErrNotExact, snapshot.Id)
	case snapshot.Spec == nil:
		return Resource{}, fmt.Errorf("%w: %s", ErrNoSpec, snapshot.Id)
	case snapshot.Generation.IsZero():
		// Every admission assigns at least 1, so a zero here is a resource
		// that never went through one.
		return Resource{}, fmt.Errorf("%w: %s has no generation", ErrNoResource, snapshot.Id)
	case snapshot.Version.IsZero():
		return Resource{}, fmt.Errorf("%w: %s", ErrNoVersion, snapshot.Id)
	}

	if err := checkFinalizers(snapshot.Finalizers); err != nil {
		return Resource{}, fmt.Errorf("%s: %w", snapshot.Id, err)
	}

	if snapshot.Deleting && len(snapshot.Finalizers) == 0 {
		// A marked resource with nothing to wait for should have been
		// removed instead. Stored, it is a tombstone no one will clear.
		return Resource{}, fmt.Errorf("%w: %s", ErrNoFinalizers, snapshot.Id)
	}

	return Resource{
		intent: Intent{
			id:   snapshot.Id,
			spec: snapshot.Spec,
		},
		finalizers: snapshot.Finalizers,
		status:     snapshot.Status,
		generation: snapshot.Generation,
		version:    snapshot.Version,
		deleting:   snapshot.Deleting,
		author:     snapshot.Author,
	}, nil
}

// Flatten is the inverse: what an encoder needs to write a resource down.
//
// It hands out the two protobuf messages as they are rather than cloning
// them, because an encoder reads them and is finished.
func (r Resource) Flatten() Snapshot {
	return Snapshot{
		Id:         r.intent.id,
		Spec:       r.intent.spec,
		Status:     r.status,
		Finalizers: r.Finalizers(),
		Generation: r.generation,
		Version:    r.version,
		Deleting:   r.deleting,
		Author:     r.author,
	}
}
