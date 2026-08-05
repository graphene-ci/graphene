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

	// A kind nobody has defined yet is not a failure here — it is the
	// first version. head reports that as ErrNoSuchKind rather than
	// passing the store's own ErrNotFound through, because everywhere
	// else the difference matters.
	current, err := k.head(ctx, definition.Kind())
	if err != nil && !errors.Is(err, ErrNoSuchKind) {
		return def.Head{}, err
	}

	if !current.Value.IsZero() && current.Value.Definition().Eq(definition) {
		return current.Value, nil
	}

	published, err := def.Publish(definition, current.Value.Version().Next())
	if err != nil {
		return def.Head{}, err
	}

	if _, err := k.published.Put(ctx, published, revision.Absent); err != nil {
		return def.Head{}, fmt.Errorf("publish %s: %w", published, err)
	}

	head, err := def.NewHead(published)
	if err != nil {
		return def.Head{}, err
	}

	if _, err := k.heads.Put(ctx, head, current.Revision); err != nil {
		return def.Head{}, fmt.Errorf("make %s current: %w", published, err)
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
	stored, err := k.head(ctx, named)
	if err != nil {
		return err
	}

	used, err := k.inUse(ctx, stored.Value)
	if err != nil {
		return err
	}

	if used {
		return fmt.Errorf("%w: %s", ErrKindInUse, named)
	}

	at, err := def.HeadPath(named)
	if err != nil {
		return err
	}

	if _, err := k.heads.Delete(ctx, resource.NewId(def.HeadKind, at), stored.Revision); err != nil {
		return fmt.Errorf("undefine %s: %w", named, err)
	}

	return k.sweep(ctx, named, stored.Value.Version())
}

// sweep removes every published version of a kind, newest first.
//
// Newest first so that an interrupted sweep leaves the OLDEST versions
// behind: those are the ones an old resource might still pin, and a
// resource whose pinned version is gone can no longer be read as what it
// was.
func (k Kernel) sweep(ctx context.Context, named kind.Kind, from def.Version) error {
	for version := from; !version.IsZero(); version-- {
		at, err := def.PublishedPath(named, version)
		if err != nil {
			return err
		}

		id := resource.NewId(def.PublishedKind, at)

		stored, err := k.published.Get(ctx, id)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				continue
			}

			return err
		}

		if _, err := k.published.Delete(ctx, id, stored.Revision); err != nil {
			return fmt.Errorf("remove %s %s: %w", named, version, err)
		}
	}

	return nil
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
