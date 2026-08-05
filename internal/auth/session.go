package auth

import (
	"context"
	"fmt"
	"iter"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// The kernel's surface, with the question asked first. Every method below
// is the kernel's method and a check, and the check is one line because
// the verb IS the method — which is what all the splitting was for.

// Get reads one resource.
func (s Session) Get(ctx context.Context, id resource.Id) (store.Value[resource.Resource], error) {
	if err := s.allow(ctx, Get, id); err != nil {
		return store.Value[resource.Resource]{}, err
	}

	return s.guard.kernel.Get(ctx, id)
}

// Scan walks everything under an id that the caller may list.
//
// The permission is checked ONCE, against the prefix being scanned, and
// not per record. Filtering silently would hand back a list that is
// shorter than the truth with nothing saying so — and a caller cannot
// tell that from "there is nothing there", which is the difference
// between an empty answer and a wrong one.
func (s Session) Scan(
	ctx context.Context,
	prefix resource.Id,
) iter.Seq2[store.Value[resource.Resource], error] {
	return func(yield func(store.Value[resource.Resource], error) bool) {
		if err := s.allow(ctx, List, prefix); err != nil {
			yield(store.Value[resource.Resource]{}, err)

			return
		}

		for value, err := range s.guard.kernel.Scan(ctx, prefix) {
			if !yield(value, err) {
				return
			}
		}
	}
}

// Watch follows changes under an id the caller may watch.
func (s Session) Watch(
	ctx context.Context,
	prefix resource.Id,
	after revision.Revision,
) (store.Stream[resource.Resource], error) {
	if err := s.allow(ctx, Watch, prefix); err != nil {
		return store.Stream[resource.Resource]{}, err
	}

	return s.guard.kernel.Watch(ctx, prefix, after)
}

// Put writes intent.
//
// Writing a Role is the one write that is not only about its own
// permission: handing out grants is handing out authority, and handing
// out more than you hold is how a caller allowed to manage users becomes
// a caller allowed to do anything.
func (s Session) Put(
	ctx context.Context,
	intent resource.Intent,
	expect revision.Revision,
) (revision.Revision, error) {
	if err := s.allow(ctx, Put, intent.Id()); err != nil {
		return revision.None, err
	}

	if err := s.checkEscalation(ctx, intent); err != nil {
		return revision.None, err
	}

	return s.guard.kernel.Put(ctx, intent, expect)
}

// Report records what a controller found.
func (s Session) Report(
	ctx context.Context,
	id resource.Id,
	status *schemapb.StructValue,
	expect revision.Revision,
) (revision.Revision, error) {
	if err := s.allow(ctx, Report, id); err != nil {
		return revision.None, err
	}

	return s.guard.kernel.Report(ctx, id, status, expect)
}

// Claim places a claim on a resource's deletion.
func (s Session) Claim(
	ctx context.Context,
	id resource.Id,
	finalizer resource.Finalizer,
	expect revision.Revision,
) (revision.Revision, error) {
	if err := s.allow(ctx, Claim, id); err != nil {
		return revision.None, err
	}

	return s.guard.kernel.Claim(ctx, id, finalizer, expect)
}

// Release lets go of one.
func (s Session) Release(
	ctx context.Context,
	id resource.Id,
	finalizer resource.Finalizer,
	expect revision.Revision,
) (revision.Revision, error) {
	if err := s.allow(ctx, Release, id); err != nil {
		return revision.None, err
	}

	return s.guard.kernel.Release(ctx, id, finalizer, expect)
}

// Delete asks a resource to go away.
//
// Only the resource NAMED is checked, and not what the deletion cascades
// into. A caller permitted to delete an owner is permitted to delete what
// that owner owns, because that is what owning means — the alternative is
// a delete that half-succeeds and leaves a parent gone and its children
// behind, which is worse than either answer.
func (s Session) Delete(
	ctx context.Context,
	id resource.Id,
	expect revision.Revision,
) (revision.Revision, error) {
	if err := s.allow(ctx, Delete, id); err != nil {
		return revision.None, err
	}

	return s.guard.kernel.Delete(ctx, id, expect)
}

// Define publishes a shape for a kind.
func (s Session) Define(ctx context.Context, definition def.Definition) (def.Head, error) {
	if err := s.allowKind(ctx, Define, definition.Kind()); err != nil {
		return def.Head{}, err
	}

	return s.guard.kernel.Define(ctx, definition)
}

// Undefine removes a kind and every version of it.
func (s Session) Undefine(ctx context.Context, named kind.Kind) error {
	if err := s.allowKind(ctx, Undefine, named); err != nil {
		return err
	}

	return s.guard.kernel.Undefine(ctx, named)
}

// Definition is the current definition of a kind.
//
// Reading a kind's shape needs `get` on that kind: a caller who cannot
// read any instance of a kind has no business learning what its instances
// look like, and a schema is a map of what is worth asking for.
func (s Session) Definition(ctx context.Context, named kind.Kind) (def.Head, error) {
	if err := s.allowKind(ctx, Get, named); err != nil {
		return def.Head{}, err
	}

	return s.guard.kernel.Definition(ctx, named)
}

// Revision is the store-wide revision, which is a number and tells
// nobody anything about what is stored.
func (s Session) Revision(ctx context.Context) (revision.Revision, error) {
	return s.guard.kernel.Revision(ctx)
}

// checkEscalation refuses a role that hands out more than the caller has.
//
// This is what makes "may manage users" a lesser privilege than "may do
// anything", and without it there is no difference between the two: a
// caller who can write any Role can write themselves one that permits
// everything, and be root a moment later.
//
// Only writes to Role are checked, because a Role is the only place a
// grant lives. That was worth arranging for on its own, and this is the
// second reason it was.
func (s Session) checkEscalation(ctx context.Context, intent resource.Intent) error {
	if !intent.Id().Kind().Eq(RoleKind) {
		return nil
	}

	stated, err := grantsOf(intent.Spec(), s.shapeOf(ctx))
	if err != nil {
		return fmt.Errorf("%s: %w", intent.Id(), err)
	}

	held, err := s.grants(ctx)
	if err != nil {
		return err
	}

	if beyond, ok := covered(held, stated); !ok {
		return fmt.Errorf("%w: %s does not hold %s", ErrEscalation, s.who, beyond)
	}

	return nil
}
