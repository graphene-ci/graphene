package kernel_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// bundleShape is what a Bundle is addressed by: one name, flat.
var bundleShape = path.MustNewTPath("name")

// bundle is the kind that gets pointed AT.
func bundle(t *testing.T) def.Definition {
	t.Helper()

	built, err := def.New(
		kind.MustNew("Bundle"),
		bundleShape,
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "bundle-spec"}).
			Fields(schemapb.Str("digest")).
			MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "bundle-status"}).MustBuild()),
	)
	if err != nil {
		t.Fatalf("bundle: %v", err)
	}

	return built
}

// pointer is a kind with one reference of the given strength at Bundle.
func pointer(t *testing.T, strength def.Strength) def.Definition {
	t.Helper()

	ref, err := def.ParseRef("spec.bundle", "Bundle", strength)
	if err != nil {
		t.Fatalf("ref: %v", err)
	}

	built, err := def.New(
		kind.MustNew("Process"),
		path.MustNewTPath("kernel", "name"),
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-spec"}).
			Fields(schemapb.Str("bundle")).
			MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-status"}).MustBuild()),
		ref,
	)
	if err != nil {
		t.Fatalf("pointer: %v", err)
	}

	return built
}

func bundleId(t *testing.T, name string) resource.Id {
	t.Helper()

	at, err := bundleShape.New(name)
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	return resource.NewId(kind.MustNew("Bundle"), at)
}

// pointing is an intent for a Process whose reference names the given
// bundle, or nothing at all when the name is empty.
func pointing(t *testing.T, at string) resource.Intent {
	t.Helper()

	spec := map[string]any{}
	if at != "" {
		spec["bundle"] = at
	}

	stated, err := resource.NewIntent(id(t, "local", "web"), schemapb.MustStructFromGo(spec))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	return stated
}

// putBundle creates one and hands back the revision it landed at.
func putBundle(t *testing.T, k kernel.Kernel, name string) revision.Revision {
	t.Helper()

	stated, err := resource.NewIntent(bundleId(t, name),
		schemapb.MustStructFromGo(map[string]any{"digest": "sha256:" + name}))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	at, err := k.Put(context.Background(), stated, revision.Absent)
	if err != nil {
		t.Fatalf("put bundle: %v", err)
	}

	return at
}

// A strong reference must resolve. That is what referential integrity
// means here, and it is checked at the door rather than discovered later
// by a controller failing into a status nobody reads.
func TestAStrongReferenceMustResolve(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		define(t, k, bundle(t))
		define(t, k, pointer(t, def.Strong))

		if _, err := k.Put(ctx, pointing(t, "missing"), revision.Absent); !errors.Is(err, kernel.ErrRefMissing) {
			t.Fatalf("want ErrRefMissing, got %v", err)
		}

		putBundle(t, k, "b1")

		if _, err := k.Put(ctx, pointing(t, "b1"), revision.Absent); err != nil {
			t.Fatalf("pointing at something that exists: %v", err)
		}
	})
}

// A weak reference is not looked up at all. A reference that had to exist
// before it could be written would be a strong one with a softer name.
func TestAWeakReferenceIsNotCheckedAtAll(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		define(t, k, bundle(t))
		define(t, k, pointer(t, def.Weak))

		if _, err := k.Put(ctx, pointing(t, "missing"), revision.Absent); err != nil {
			t.Fatalf("a weak reference at nothing was refused: %v", err)
		}
	})
}

// A field the definition declares and the value has not filled is not a
// reference: an optional reference is a real thing, and requiring one is
// the schema's business.
func TestAnUnfilledReferenceFieldIsNotAReference(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		define(t, k, bundle(t))
		define(t, k, pointer(t, def.Strong))

		if _, err := k.Put(ctx, pointing(t, ""), revision.Absent); err != nil {
			t.Fatalf("an unfilled reference field was treated as one: %v", err)
		}
	})
}

// A strong reference is the promise that the target outlives the pointer,
// so the target cannot be deleted while it is held.
func TestAStronglyHeldResourceCannotBeDeleted(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		define(t, k, bundle(t))
		define(t, k, pointer(t, def.Strong))

		at := putBundle(t, k, "b1")

		holder, err := k.Put(ctx, pointing(t, "b1"), revision.Absent)
		if err != nil {
			t.Fatalf("put: %v", err)
		}

		if _, err := k.Delete(ctx, bundleId(t, "b1"), at); !errors.Is(err, kernel.ErrReferenced) {
			t.Fatalf("want ErrReferenced, got %v", err)
		}

		// Let go of it, and the target goes.
		if _, err := k.Delete(ctx, id(t, "local", "web"), holder); err != nil {
			t.Fatalf("delete holder: %v", err)
		}

		if _, err := k.Delete(ctx, bundleId(t, "b1"), at); err != nil {
			t.Fatalf("delete target: %v", err)
		}
	})
}

// An owning reference points at what will take it down. Deleting the
// target deletes its holders, children before parents.
func TestDeletingAnOwnerTakesItsChildrenWithIt(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		define(t, k, bundle(t))
		define(t, k, pointer(t, def.Owner))

		at := putBundle(t, k, "b1")

		if _, err := k.Put(ctx, pointing(t, "b1"), revision.Absent); err != nil {
			t.Fatalf("put: %v", err)
		}

		if _, err := k.Delete(ctx, bundleId(t, "b1"), at); err != nil {
			t.Fatalf("delete owner: %v", err)
		}

		if _, err := k.Get(ctx, id(t, "local", "web")); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("the child outlived its owner: %v", err)
		}

		if _, err := k.Get(ctx, bundleId(t, "b1")); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("the owner survived its own deletion: %v", err)
		}
	})
}

// Re-pointing an owning reference would quietly change who dies with
// whom, and the change would be invisible until something died that
// should not have.
func TestAnOwningReferenceCannotBeRePointed(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		define(t, k, bundle(t))
		define(t, k, pointer(t, def.Owner))

		putBundle(t, k, "b1")
		putBundle(t, k, "b2")

		at, err := k.Put(ctx, pointing(t, "b1"), revision.Absent)
		if err != nil {
			t.Fatalf("put: %v", err)
		}

		if _, err := k.Put(ctx, pointing(t, "b2"), at); !errors.Is(err, kernel.ErrOwnerChanged) {
			t.Fatalf("want ErrOwnerChanged, got %v", err)
		}

		// Writing it again unchanged is not a re-pointing.
		if _, err := k.Put(ctx, pointing(t, "b1"), at); err != nil {
			t.Fatalf("rewriting the same owner: %v", err)
		}
	})
}

// A strong reference cannot be re-pointed into nothing either: the check
// is on the value being written, not on the one it replaces.
func TestARePointedStrongReferenceIsCheckedToo(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		define(t, k, bundle(t))
		define(t, k, pointer(t, def.Strong))

		putBundle(t, k, "b1")

		at, err := k.Put(ctx, pointing(t, "b1"), revision.Absent)
		if err != nil {
			t.Fatalf("put: %v", err)
		}

		if _, err := k.Put(ctx, pointing(t, "gone"), at); !errors.Is(err, kernel.ErrRefMissing) {
			t.Fatalf("want ErrRefMissing, got %v", err)
		}
	})
}
