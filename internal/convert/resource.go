package convert

import (
	"fmt"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// A resource is written down through its Snapshot — the flat mirror that
// exists so a value with private fields can be taken apart and put back
// together without a second door into it.
//
// The two functions below are field-for-field with it, and a field added
// to Snapshot but not to them would go silently unconverted: a record
// that loses something on every write and says nothing about it. What
// prevents that is a canary beside Snapshot itself, which stops
// compiling when the struct changes and names this file.

// ResourceToPb writes a resource down.
func ResourceToPb(value resource.Resource) *graphenepbv1.Resource {
	if value.IsZero() {
		return nil
	}

	flat := value.Flatten()

	finalizers := make([]string, 0, len(flat.Finalizers))
	for _, finalizer := range flat.Finalizers {
		finalizers = append(finalizers, finalizer.String())
	}

	return &graphenepbv1.Resource{
		Id:                IdToPb(flat.Id),
		Spec:              flat.Spec,
		Status:            flat.Status,
		Finalizers:        finalizers,
		Generation:        uint64(flat.Generation),
		DefinitionVersion: uint32(flat.Version),
		Deleting:          flat.Deleting,
	}
}

// ResourceFromPb reads one back.
//
// It goes through resource.Restore, which re-checks that the pieces make
// a resource at all. What it does NOT do is re-validate the spec: the
// record pins the definition version it was admitted under, and checking
// a v1 value against a v3 schema would refuse what was correct when it
// was written and is still what is stored.
func ResourceFromPb(message *graphenepbv1.Resource) (resource.Resource, error) {
	if message == nil {
		return resource.Resource{}, nil
	}

	id, err := IdFromPb(message.GetId())
	if err != nil {
		return resource.Resource{}, err
	}

	finalizers := make([]resource.Finalizer, 0, len(message.GetFinalizers()))

	for _, raw := range message.GetFinalizers() {
		finalizer, err := resource.NewFinalizer(raw)
		if err != nil {
			return resource.Resource{}, fmt.Errorf("%s: finalizer: %w", id, err)
		}

		finalizers = append(finalizers, finalizer)
	}

	return resource.Restore(resource.Snapshot{
		Id:         id,
		Spec:       message.GetSpec(),
		Status:     message.GetStatus(),
		Finalizers: finalizers,
		Generation: resource.Generation(message.GetGeneration()),
		Version:    def.Version(message.GetDefinitionVersion()),
		Deleting:   message.GetDeleting(),
	})
}
