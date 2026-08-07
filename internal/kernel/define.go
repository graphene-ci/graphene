package kernel

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Define publishes a shape for a kind and makes it the current one.
//
// It is IDEMPOTENT against the shape: declaring the same definition twice
// leaves one version, because Definition.Eq asks the only question that
// decides whether a kind needs a new one — do these describe the same
// shape. Without that, every apply of an unchanged manifest would churn a
// version, and every resource written afterwards would pin a number that
// meant nothing.
//
// The two writes go in one order and only one: the version first, the
// head second. Crash in between and there is an orphan version nobody
// looks at, which is harmless. The other order would leave the current
// definition of a kind half-written, and every admission against it would
// be validating instances with a shape that was never stored.
func (k Kernel) Define(ctx context.Context, definition def.Definition) (def.Head, error) {
	if definition.IsZero() {
		return def.Head{}, def.ErrNoKind
	}

	if err := unreserved(definition.Kind()); err != nil {
		return def.Head{}, err
	}

	var head def.Head

	// TWO RECORDS, ONE CHANGE. A version and the head that points at it
	// are not two writes that happen to follow each other: a head naming
	// a version nobody published is a kind whose instances cannot be
	// decoded, and the reverse is a version nothing reaches. The order
	// used to be the whole mitigation; now there is no in-between to
	// order.
	err := k.change(ctx, func(inside Kernel) error {
		// A kind nobody has defined yet is not a failure here — it is
		// the first version. head reports that as ErrNoSuchKind rather
		// than passing the store's own ErrNotFound through, because
		// everywhere else the difference matters.
		current, err := inside.head(ctx, definition.Kind())
		if err != nil && !errors.Is(err, ErrNoSuchKind) {
			return err
		}

		if !current.Value.IsZero() && current.Value.Definition().Eq(definition) {
			head = current.Value

			return nil
		}

		published, err := def.Publish(definition, current.Value.Version().Next())
		if err != nil {
			return err
		}

		if _, err := inside.published.Put(ctx, published, revision.Absent); err != nil {
			return fmt.Errorf("publish %s: %w", published, err)
		}

		head, err = def.NewHead(published)
		if err != nil {
			return err
		}

		if _, err := inside.heads.Put(ctx, head, current.Revision); err != nil {
			return fmt.Errorf("make %s current: %w", published, err)
		}

		return nil
	})
	if err != nil {
		return def.Head{}, err
	}

	return head, nil
}

// unreserved refuses the two names the kernel addresses its own records
// under.
//
// A kind named "Kind" would put its instances in the very key space the
// heads live in — a resource of it at path /web would encode to exactly
// the key the head of a kind named "web" does, and one would silently
// overwrite the other. Same for "Definition" and the published shapes.
//
// It is a small check for a failure that would look like a store quietly
// losing kinds.
func unreserved(named kind.Kind) error {
	for _, reserved := range []kind.Kind{def.HeadKind, def.PublishedKind} {
		if named.Eq(reserved) {
			return fmt.Errorf("%w: %s", ErrReservedKind, named)
		}
	}

	return nil
}

// WatchKind follows the current definition of one kind.
//
// The head record IS the current definition, so a change to it is exactly
// the event a controller wants: a new version published, or the kind
// undefined. Publishing a version that does not become current produces
// nothing here, which is right — nothing about the kind changed.
//
// A delete carries the definition that was current, so a controller
// learns which shape it is losing rather than only that it lost one.
func (k Kernel) WatchKind(
	ctx context.Context,
	named kind.Kind,
	after revision.Revision,
) (store.Stream[def.Head], error) {
	at, err := def.HeadPath(named)
	if err != nil {
		return store.Stream[def.Head]{}, err
	}

	return k.heads.Watch(ctx, resource.NewId(def.HeadKind, at), after)
}

// WatchKinds follows every kind's current definition at once — what a
// mirror of the whole registry follows, rather than one controller.
func (k Kernel) WatchKinds(ctx context.Context, after revision.Revision) (store.Stream[def.Head], error) {
	everything, err := def.HeadShape.New()
	if err != nil {
		return store.Stream[def.Head]{}, err
	}

	return k.heads.Watch(ctx, resource.NewId(def.HeadKind, everything), after)
}

// Definition is the current definition of a kind — one Get, on an exact
// key, which is what the head record exists to make possible.
func (k Kernel) Definition(ctx context.Context, named kind.Kind) (def.Head, error) {
	stored, err := k.head(ctx, named)
	if err != nil {
		return def.Head{}, err
	}

	return stored.Value, nil
}

// DefinitionAt is one particular version, which is what a resource pins
// and what anything reading an older instance has to validate against.
func (k Kernel) DefinitionAt(ctx context.Context, named kind.Kind, version def.Version) (def.Published, error) {
	at, err := def.PublishedPath(named, version)
	if err != nil {
		return def.Published{}, err
	}

	stored, err := k.published.Get(ctx, resource.NewId(def.PublishedKind, at))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return def.Published{}, fmt.Errorf("%w: %s %s", ErrNoSuchVersion, named, version)
		}

		return def.Published{}, err
	}

	return stored.Value, nil
}

// Kinds walks every kind that has been defined, in key order.
func (k Kernel) Kinds(ctx context.Context) iter.Seq2[def.Head, error] {
	return func(yield func(def.Head, error) bool) {
		everything, err := def.HeadShape.New()
		if err != nil {
			yield(def.Head{}, err)

			return
		}

		for stored, err := range k.heads.Scan(ctx, resource.NewId(def.HeadKind, everything)) {
			if !yield(stored.Value, err) {
				return
			}
		}
	}
}

// Undefine removes a kind and every version of it.
//
// It refuses while any instance is left, and that refusal is synchronous
// rather than a finalizer: finalizers exist so that somebody ELSE can
// clean up asynchronously, and here there is nobody else — the instances
// are in this store and the answer is one scan that stops at the first
// one it finds.
//
// The head goes first, which is the reverse of Define and for the same
// reason. Once the head is gone the kind admits nothing, so a crash
// during the sweep leaves versions nobody can reach rather than a kind
// whose current shape has disappeared from under it.
func (k Kernel) Undefine(ctx context.Context, named kind.Kind) error {
	// The count of instances, the head and every version, as one change.
	// Checked separately, "no instances" is true when it is read and can
	// be false by the time the head goes.
	return k.change(ctx, func(inside Kernel) error {
		return inside.undefine(ctx, named)
	})
}

// undefine is Undefine with the transaction already open.
func (k Kernel) undefine(ctx context.Context, named kind.Kind) error {
	stored, err := k.head(ctx, named)
	defined := err == nil

	switch {
	case defined:
		used, useErr := k.inUse(ctx, stored.Value)
		if useErr != nil {
			return useErr
		}

		if used {
			return fmt.Errorf("%w: %s", ErrKindInUse, named)
		}

		at, pathErr := def.HeadPath(named)
		if pathErr != nil {
			return pathErr
		}

		if _, delErr := k.heads.Delete(ctx, resource.NewId(def.HeadKind, at), stored.Revision); delErr != nil {
			return fmt.Errorf("undefine %s: %w", named, delErr)
		}

	case errors.Is(err, ErrNoSuchKind):
		// No head, which is either a kind that never existed or one whose
		// removal was interrupted after the head went and before the
		// versions did. The two are told apart by whether anything is left
		// to sweep, and only the sweep can say.

	default:
		return err
	}

	swept, err := k.sweep(ctx, named)
	if err != nil {
		return err
	}

	// Nothing there and nothing left: the kind never existed. Anything
	// else — a head just removed, or versions an interrupted removal left
	// behind — is a removal that finished, whichever run finished it.
	if !defined && swept == 0 {
		return fmt.Errorf("%w: %s", ErrNoSuchKind, named)
	}

	return nil
}

// sweep removes every published version of a kind, and says how many it
// found.
//
// It walks the prefix rather than counting down from what the head said,
// and that is what makes Undefine resumable. Counting down needs the
// head, and the head is the first thing Undefine removes — so an
// interrupted removal could never be finished, and the versions it left
// would be unreachable forever: not listed, because there is no head, and
// only readable by guessing a number.
//
// The walk is a snapshot taken before anything is deleted, because
// deleting from underneath a running scan is asking a store to describe
// what it is in the middle of changing.
func (k Kernel) sweep(ctx context.Context, named kind.Kind) (int, error) {
	under, err := def.PublishedShape.New(named.String())
	if err != nil {
		return 0, err
	}

	type doomed struct {
		id resource.Id
		at revision.Revision
	}

	var found []doomed

	for stored, err := range k.published.Scan(ctx, resource.NewId(def.PublishedKind, under)) {
		if err != nil {
			return 0, err
		}

		id, err := k.publishedId(stored.Value)
		if err != nil {
			return 0, err
		}

		found = append(found, doomed{id: id, at: stored.Revision})
	}

	for _, one := range found {
		if _, err := k.published.Delete(ctx, one.id, one.at); err != nil {
			return 0, fmt.Errorf("remove %s: %w", one.id, err)
		}
	}

	return len(found), nil
}

// publishedId is where one published shape lives.
func (k Kernel) publishedId(published def.Published) (resource.Id, error) {
	at, err := def.PublishedPath(published.Kind(), published.Version())
	if err != nil {
		return resource.Id{}, err
	}

	return resource.NewId(def.PublishedKind, at), nil
}

// inUse reports whether a kind has any instance left, stopping at the
// first one rather than counting them.
func (k Kernel) inUse(ctx context.Context, head def.Head) (bool, error) {
	everything, err := head.Definition().Shape().New()
	if err != nil {
		return false, err
	}

	for _, err := range k.resources.Scan(ctx, resource.NewId(head.Kind(), everything)) {
		if err != nil {
			return false, err
		}

		return true, nil
	}

	return false, nil
}

// head reads the head record with the revision it is at, which is what
// the next write of it has to compare against.
func (k Kernel) head(ctx context.Context, named kind.Kind) (store.Value[def.Head], error) {
	at, err := def.HeadPath(named)
	if err != nil {
		return store.Value[def.Head]{}, err
	}

	stored, err := k.heads.Get(ctx, resource.NewId(def.HeadKind, at))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return store.Value[def.Head]{}, fmt.Errorf("%w: %s", ErrNoSuchKind, named)
		}

		return store.Value[def.Head]{}, err
	}

	return stored, nil
}
