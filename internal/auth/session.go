package auth

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/kernel"
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

// List walks everything under an id that the caller may list.
//
// The permission is checked ONCE, against the prefix being scanned, and
// not per record. Filtering silently would hand back a list that is
// shorter than the truth with nothing saying so — and a caller cannot
// tell that from "there is nothing there", which is the difference
// between an empty answer and a wrong one.
func (s Session) List(
	ctx context.Context,
	prefix resource.Id,
) iter.Seq2[store.Value[resource.Resource], error] {
	return func(yield func(store.Value[resource.Resource], error) bool) {
		if err := s.allow(ctx, List, prefix); err != nil {
			yield(store.Value[resource.Resource]{}, err)

			return
		}

		for value, err := range s.guard.kernel.List(ctx, prefix) {
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

	if err := s.checkEscalation(ctx, intent.Id(), intent.Spec(), def.SpecRoot); err != nil {
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

	// A status can hand out authority too. If a kind declares a reference
	// to an Identity or a Role in its status half, then whoever may
	// report on it names one — and a check that only ever read the spec
	// would make where the field sits decide whether the rule applies.
	if err := s.checkEscalation(ctx, id, status, def.StatusRoot); err != nil {
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

// May answers a kind-level question about something that is NOT a
// resource.
//
// Everywhere else in this package permission is the method, because every
// method is a thing the kernel does. The byte store is not the kernel: it
// has ids rather than paths, no revisions, and nothing to confine a
// prefix to. Wrapping it needs a way to ask, and this is that way — the
// kind name is a capability there rather than a shape.
//
// It stays deliberately narrow. There is no path, so nothing that HAS a
// path should come through here: a caller reaching for it to check a
// resource is a caller working around the methods above.
func (s Session) May(ctx context.Context, verb Verb, named kind.Kind) error {
	return s.allowKind(ctx, verb, named)
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

// checkEscalation refuses a write that hands out more than the caller
// has.
//
// This is what makes "may manage users" a lesser privilege than "may do
// anything", and without it there is no difference between the two: a
// caller who can write any Role can write themselves one that permits
// everything, and be root a moment later.
//
// A grant lives in one place, but it is HANDED OUT in three, and all
// three are the same act: stating grants in a Role, naming a Role from an
// Identity, and naming an Identity from anything that then acts as it — a
// Process, say, whose bytes belong to whoever wrote the record. The last
// one is why this is not a check about Role: a caller who could write
// `identity: admin` on a process they supply the bytes for would be admin
// as soon as it started.
//
// None of that is spelled kind by kind. A reference to a Role or an
// Identity IS the hand-out, the definition already says which fields are
// references, so what confers authority is read off the same declaration
// that says what points at what.
func (s Session) checkEscalation(
	ctx context.Context, id resource.Id, half *schemapb.StructValue, root string,
) error {
	stated, err := s.conferred(ctx, id, half, root)
	if err != nil {
		return err
	}

	if len(stated) == 0 {
		return nil
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

// conferred is everything the write would let somebody do.
//
// The references are read out of the DEFINITION and the grants out of the
// store, both unguarded: the question is what this write hands out, and a
// caller who may not read a role can still be refused for naming it.
func (s Session) conferred(
	ctx context.Context, id resource.Id, half *schemapb.StructValue, root string,
) ([]Grant, error) {
	var stated []Grant

	// A Role states grants in its SPEC and nowhere else, so a status
	// write cannot state one however the kind is declared.
	if id.Kind().Eq(RoleKind) && root == def.SpecRoot {
		granted, err := grantsOf(half, s.shapeOf(ctx))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", id, err)
		}

		stated = append(stated, granted...)
	}

	head, err := s.guard.kernel.Definition(ctx, id.Kind())
	if err != nil {
		return nil, fmt.Errorf("%s: %w", id, err)
	}

	refs, err := resource.ReferencesIn(head.Definition(), half, root)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", id, err)
	}

	for _, ref := range refs {
		granted, err := s.holds(ctx, ref)
		if err != nil {
			return nil, err
		}

		stated = append(stated, granted...)
	}

	return stated, nil
}

// holds is what naming one thing hands out: a role's own grants, or
// everything the identity named holds.
//
// Anything else is nothing. Most references are ordinary — one resource
// pointing at another — and only these two kinds carry authority.
func (s Session) holds(ctx context.Context, ref resource.Reference) ([]Grant, error) {
	switch {
	case ref.Kind.Eq(RoleKind):
		return s.role(ctx, ref.Raw)

	case ref.Kind.Eq(IdentityKind):
		return s.identity(ctx, ref.Raw)

	default:
		return nil, nil
	}
}

// identity is everything one identity may do, read for somebody else's
// sake.
//
// An identity that is not there hands out nothing, and that is not a hole
// worth closing here: a Process naming one is a strong reference, so the
// write is refused a moment later by integrity. Refusing it here as well
// would say "no such identity" to a caller who is entitled to be told the
// reference is broken.
func (s Session) identity(ctx context.Context, named string) ([]Grant, error) {
	who, err := NewPrincipal(named)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrEscalation, err)
	}

	id, err := IdentityId(who)
	if err != nil {
		return nil, err
	}

	stored, err := s.guard.kernel.Get(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}

		return nil, err
	}

	var held []Grant

	for _, role := range roleNames(stored.Value) {
		granted, err := s.role(ctx, role)
		if err != nil {
			return nil, err
		}

		held = append(held, granted...)
	}

	return held, nil
}

// DefinitionAt is one particular version of a kind's shape.
//
// A resource pins the version it was admitted under, so reading an older
// instance means reading the shape it was written against. Permitted by
// `get` on the kind, the same as the current shape: a caller who may see
// what a kind is may see what it was.
func (s Session) DefinitionAt(
	ctx context.Context,
	named kind.Kind,
	version def.Version,
) (def.Published, error) {
	if err := s.allowKind(ctx, Get, named); err != nil {
		return def.Published{}, err
	}

	return s.guard.kernel.DefinitionAt(ctx, named, version)
}

// Kinds walks every kind that has been defined.
//
// Permitted by `list` on Kind — the kind the head records live under —
// rather than by holding `get` on each. Filtering the walk down to what
// the caller may see would hand back a list shorter than the truth with
// nothing saying so, and a caller cannot tell that from "there is nothing
// there".
func (s Session) Kinds(ctx context.Context) iter.Seq2[def.Head, error] {
	return func(yield func(def.Head, error) bool) {
		if err := s.allowKind(ctx, List, def.HeadKind); err != nil {
			yield(def.Head{}, err)

			return
		}

		for head, err := range s.guard.kernel.Kinds(ctx) {
			if !yield(head, err) {
				return
			}
		}
	}
}

// WatchKind follows the current definition of one kind.
func (s Session) WatchKind(
	ctx context.Context,
	named kind.Kind,
	after revision.Revision,
) (store.Stream[def.Head], error) {
	if err := s.allowKind(ctx, Watch, named); err != nil {
		return store.Stream[def.Head]{}, err
	}

	return s.guard.kernel.WatchKind(ctx, named, after)
}

// WatchKinds follows every kind's current definition, under the same
// permission that lists them.
func (s Session) WatchKinds(
	ctx context.Context,
	after revision.Revision,
) (store.Stream[def.Head], error) {
	if err := s.allowKind(ctx, Watch, def.HeadKind); err != nil {
		return store.Stream[def.Head]{}, err
	}

	return s.guard.kernel.WatchKinds(ctx, after)
}

// Holders is what points at a resource, and what those pointers mean.
//
// It answers the question a refused delete raises — what is holding this
// — and it is the model's question rather than any client's: a reference
// carries a strength, and a strength is a statement about two lifetimes.
//
// Permitted by `get` on the resource being asked about. Naming what holds
// something does disclose ids the caller might not otherwise read, and
// that is the honest cost of answering at all; the alternative is a
// refusal that says a holder exists without saying which, which is worse
// to work with and discloses the same fact.
func (s Session) Holders(ctx context.Context, id resource.Id) ([]kernel.Holder, error) {
	if err := s.allow(ctx, Get, id); err != nil {
		return nil, err
	}

	return s.guard.kernel.Holders(ctx, id)
}
