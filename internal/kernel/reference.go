package kernel

import (
	"context"
	"errors"
	"fmt"

	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// resolve turns the references a resource carries into ids.
//
// Extraction is pure and lives on the type; this is the half that needs
// lookups — a written reference is a path, and a path only becomes an id
// once the TARGET kind's shape is known. That shape is in the target's
// definition, which is why this cannot be a function of the value alone.
func (k Kernel) resolve(
	ctx context.Context,
	definition def.Definition,
	value resource.Resource,
) ([]resolved, error) {
	found, err := resource.References(definition, value)
	if err != nil {
		return nil, err
	}

	ids := make([]resolved, 0, len(found))

	for _, reference := range found {
		target, err := k.Definition(ctx, reference.Kind)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", reference, err)
		}

		at, err := target.Definition().Shape().Parse(reference.Raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", reference, err)
		}

		if !at.IsExact() {
			return nil, fmt.Errorf("%w: %s names a subtree", ErrRefNotExact, reference)
		}

		ids = append(ids, resolved{
			Reference: reference,
			id:        resource.NewId(reference.Kind, at),
		})
	}

	return ids, nil
}

// resolved is a reference with the id it names.
type resolved struct {
	resource.Reference

	id resource.Id
}

// checkReferences refuses a write whose references do not point at
// anything.
//
// Strong and owning references must resolve; weak ones are not looked up
// at all, which is what makes them weak — a reference that had to exist
// before it could be written would be a strong one with a softer name.
//
// The check is a read per reference and no scan: it asks "is this there",
// which is a key, rather than "who points here", which is not.
func (k Kernel) checkReferences(
	ctx context.Context,
	definition def.Definition,
	value resource.Resource,
) error {
	references, err := k.resolve(ctx, definition, value)
	if err != nil {
		return err
	}

	for _, reference := range references {
		if !reference.Strength.Requires() {
			continue
		}

		if _, err := k.resources.Get(ctx, reference.id); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return fmt.Errorf("%w: %s", ErrRefMissing, reference)
			}

			return err
		}
	}

	return nil
}

// referrers finds every resource pointing at one, and what its pointer
// means.
//
// There is no reverse index, so this is a scan — but not of the store.
// Only kinds whose DEFINITION declares a reference to this kind can
// possibly point at it, and those are found by walking the heads, of
// which there are as many as there are kinds. Then only those kinds'
// instances are read.
//
// This is where a reverse index will earn itself, and when it does the
// check above it stays the same code: what changes is where the candidate
// list comes from, not what is done with it.
func (k Kernel) referrers(ctx context.Context, id resource.Id) ([]referrer, error) {
	var found []referrer

	for head, err := range k.Kinds(ctx) {
		if err != nil {
			return nil, err
		}

		points := false

		for _, ref := range head.Definition().Refs() {
			if ref.Kind().Eq(id.Kind()) {
				points = true

				break
			}
		}

		if !points {
			continue
		}

		under, err := head.Definition().Shape().New()
		if err != nil {
			return nil, err
		}

		for stored, err := range k.resources.Scan(ctx, resource.NewId(head.Kind(), under)) {
			if err != nil {
				return nil, err
			}

			references, err := k.resolve(ctx, head.Definition(), stored.Value)
			if err != nil {
				return nil, err
			}

			for _, reference := range references {
				if reference.id.Eq(id) {
					found = append(found, referrer{
						id:       stored.Value.Id(),
						revision: stored.Revision,
						strength: reference.Strength,
					})
				}
			}
		}
	}

	return found, nil
}

// referrer is one resource pointing at the one being deleted.
type referrer struct {
	id       resource.Id
	revision revision.Revision
	strength def.Strength
}
