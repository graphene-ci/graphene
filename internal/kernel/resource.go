package kernel

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Get reads one resource, with the revision it is at — which is the token
// the next write of it has to hand back.
func (k Kernel) Get(ctx context.Context, id resource.Id) (store.Value[resource.Resource], error) {
	return k.resources.Get(ctx, id)
}

// Scan walks everything under an id, in key order. An id with fewer path
// values than its shape has positions is a subtree.
func (k Kernel) Scan(ctx context.Context, prefix resource.Id) iter.Seq2[store.Value[resource.Resource], error] {
	return k.resources.Scan(ctx, prefix)
}

// Watch follows changes under an id. It delivers no snapshot: take the
// revision first, scan second, watch third.
func (k Kernel) Watch(
	ctx context.Context,
	prefix resource.Id,
	after revision.Revision,
) (store.Stream[resource.Resource], error) {
	return k.resources.Watch(ctx, prefix, after)
}

// Put writes what an author asked for.
//
// expect is what the caller read: revision.Absent to create, otherwise
// the revision it saw. It is checked before any work is done rather than
// left to the store, so a caller working from a stale read is told that
// and not something further down about its spec.
func (k Kernel) Put(
	ctx context.Context,
	intent resource.Intent,
	expect revision.Revision,
) (revision.Revision, error) {
	if intent.IsZero() {
		return revision.None, resource.ErrNoIntent
	}

	head, err := k.Definition(ctx, intent.Id().Kind())
	if err != nil {
		return revision.None, err
	}

	previous, err := k.previous(ctx, intent.Id(), expect)
	if err != nil {
		return revision.None, err
	}

	admitted, err := resource.Admit(head.Definition(), head.Version(), intent, previous)
	if err != nil {
		return revision.None, err
	}

	if err := k.checkOwnerHeld(head.Definition(), admitted, previous); err != nil {
		return revision.None, err
	}

	if err := k.checkReferences(ctx, head.Definition(), admitted); err != nil {
		return revision.None, err
	}

	return k.resources.Put(ctx, admitted, expect)
}

// Claim places a claim on a resource's deletion.
//
// Its own call, like Report, because it is a different party writing a
// different part: whoever will do the cleaning is rarely whoever wrote
// the spec, and the permission to hold a resource open should not carry
// the permission to rewrite it.
func (k Kernel) Claim(
	ctx context.Context,
	id resource.Id,
	finalizer resource.Finalizer,
	expect revision.Revision,
) (revision.Revision, error) {
	current, err := k.held(ctx, id, expect)
	if err != nil {
		return revision.None, err
	}

	claimed, err := resource.Claim(current, finalizer)
	if err != nil {
		return revision.None, err
	}

	return k.resources.Put(ctx, claimed, expect)
}

// Release lets go of a claim, and removes the resource if that was the
// last thing a deletion was waiting for.
//
// The deletion finishes HERE, which is the only moment it can: a marked
// resource with no claims left is a tombstone nobody would ever clear,
// and this is exactly when that becomes true. The alternative is a
// sweeper hunting for them, which is a moving part for something already
// known at the instant it happens.
func (k Kernel) Release(
	ctx context.Context,
	id resource.Id,
	finalizer resource.Finalizer,
	expect revision.Revision,
) (revision.Revision, error) {
	current, err := k.held(ctx, id, expect)
	if err != nil {
		return revision.None, err
	}

	released, err := resource.Release(current, finalizer)
	if err != nil {
		return revision.None, err
	}

	if released.IsDeleting() && len(released.Finalizers()) == 0 {
		return k.resources.Delete(ctx, id, expect)
	}

	return k.resources.Put(ctx, released, expect)
}

// held reads the resource a claim is being placed on or taken off, and
// refuses one that is not there.
func (k Kernel) held(
	ctx context.Context,
	id resource.Id,
	expect revision.Revision,
) (resource.Resource, error) {
	current, err := k.previous(ctx, id, expect)
	if err != nil {
		return resource.Resource{}, err
	}

	if current.IsZero() {
		return resource.Resource{}, fmt.Errorf("%w: %s", store.ErrNotFound, id)
	}

	return current, nil
}

// Report records what a controller found.
//
// A separate call from Put because it is a different party writing a
// different half, and because it must not move the generation: a status
// write that counted as intent would wake the controller that wrote it,
// forever.
func (k Kernel) Report(
	ctx context.Context,
	id resource.Id,
	status *schemapb.StructValue,
	expect revision.Revision,
) (revision.Revision, error) {
	current, err := k.held(ctx, id, expect)
	if err != nil {
		return revision.None, err
	}

	// The version the resource was ADMITTED under, not the current one: a
	// v1 resource reports a v1 status, and checking it against a v3 schema
	// would refuse what was correct when it was written.
	published, err := k.DefinitionAt(ctx, id.Kind(), current.DefinitionVersion())
	if err != nil {
		return revision.None, err
	}

	reported, err := resource.Report(published.Definition(), current, status)
	if err != nil {
		return revision.None, err
	}

	return k.resources.Put(ctx, reported, expect)
}

// Delete removes a resource, or marks it and waits.
//
// Which of the two depends on the resource and not on the caller: with
// claims on it the record stays, marked, until whoever placed them
// releases them; with none there is nothing to wait for and it goes. A
// caller cannot choose, because the claims are the whole point of the
// protocol and an override would be a way around somebody else's cleanup.
func (k Kernel) Delete(ctx context.Context, id resource.Id, expect revision.Revision) (revision.Revision, error) {
	current, err := k.held(ctx, id, expect)
	if err != nil {
		return revision.None, err
	}

	if err := k.collect(ctx, id); err != nil {
		return revision.None, err
	}

	if len(current.Finalizers()) == 0 {
		return k.resources.Delete(ctx, id, expect)
	}

	marked, err := resource.MarkDeleting(current)
	if err != nil {
		return revision.None, err
	}

	return k.resources.Put(ctx, marked, expect)
}

// collect settles what happens to everything pointing at a resource
// before it goes.
//
// This is the garbage collector, running inline. Strong references refuse
// the delete; owning ones are followed and their holders removed first.
// When it has to become asynchronous it becomes a controller running this
// same walk behind a finalizer, and the walk does not change.
//
// CHILDREN BEFORE PARENTS, and that ordering is the whole of its crash
// safety. Interrupted, it leaves some children gone and the parent still
// there — which a retry finishes. The other order would leave orphans
// pointing at something that is no longer there to find them by.
func (k Kernel) collect(ctx context.Context, id resource.Id) error {
	holders, err := k.referrers(ctx, id)
	if err != nil {
		return err
	}

	for _, holder := range holders {
		if holder.strength.Holds() {
			return fmt.Errorf("%w: %s holds %s", ErrReferenced, holder.id, id)
		}
	}

	for _, holder := range holders {
		if !holder.strength.Owns() {
			continue
		}

		if _, err := k.Delete(ctx, holder.id, holder.revision); err != nil {
			return fmt.Errorf("collect %s: %w", holder.id, err)
		}
	}

	return nil
}

// checkOwnerHeld refuses a write that re-points an owning reference.
//
// Re-pointing one would quietly change who dies with whom, and the change
// would be invisible until something died that should not have. Strong
// and weak references may be re-pointed freely: they say what must exist,
// not what shares a lifetime.
func (k Kernel) checkOwnerHeld(
	definition def.Definition,
	admitted resource.Resource,
	previous resource.Resource,
) error {
	if previous.IsZero() {
		return nil
	}

	was, err := resource.References(definition, previous)
	if err != nil {
		return err
	}

	now, err := resource.References(definition, admitted)
	if err != nil {
		return err
	}

	if !owners(was).Equal(owners(now)) {
		return fmt.Errorf("%w: %s", ErrOwnerChanged, admitted.Id())
	}

	return nil
}

// owners is the set of owning references a resource carries, as text, so
// that two of them can be compared without caring about order.
func owners(references []resource.Reference) sortedSet {
	var found sortedSet

	for _, reference := range references {
		if reference.Strength.Owns() {
			found = append(found, reference.String())
		}
	}

	slices.Sort(found)

	return found
}

// sortedSet is a comparable list of strings.
type sortedSet []string

// Equal reports two sets naming the same things.
func (s sortedSet) Equal(other sortedSet) bool { return slices.Equal(s, other) }

// previous reads the resource a write is following, and checks it is the
// one the caller had.
//
// Reading it is not optional: an admission needs the resource it writes
// over to count the generation, to hold the owner still, and to refuse a
// spec change on something already going away. Checking the revision here
// rather than leaving it to the store means a stale caller is told that
// instead of being told something about its spec that it cannot act on.
func (k Kernel) previous(
	ctx context.Context,
	id resource.Id,
	expect revision.Revision,
) (resource.Resource, error) {
	stored, err := k.resources.Get(ctx, id)

	switch {
	case errors.Is(err, store.ErrNotFound):
		if !expect.IsZero() {
			return resource.Resource{}, fmt.Errorf("%w: %s is gone, expected it at %s",
				revision.ErrConflict, id, expect)
		}

		return resource.Resource{}, nil

	case err != nil:
		return resource.Resource{}, err

	case !stored.Revision.Eq(expect):
		return resource.Resource{}, fmt.Errorf("%w: %s is at %s, expected %s",
			revision.ErrConflict, id, stored.Revision, expect)
	}

	return stored.Value, nil
}
