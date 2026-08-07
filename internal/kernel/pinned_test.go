package kernel_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store/kv/memory"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// A resource is read through the version it was ADMITTED under. These are
// the two places that used to read the current one instead.

// A kind that stops pointing at something does not release what its
// existing resources are holding: they were admitted under a version that
// points, and they still do.
func TestAnOldVersionStillHolds(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bytes := memory.New()

	t.Cleanup(func() { _ = bytes.Close() })

	k := kernel.New(bytes)

	if _, err := k.Define(ctx, targetKind()); err != nil {
		t.Fatalf("define target: %v", err)
	}

	if _, err := k.Define(ctx, holderKind(t)); err != nil {
		t.Fatalf("define holder v1: %v", err)
	}

	target := writeTarget(t, ctx, k, "held")

	if _, err := k.Put(ctx, holderIntent(t, "holder", "held"), revision.Absent); err != nil {
		t.Fatalf("put holder: %v", err)
	}

	// v2 of the same kind, with the reference dropped. The shape does not
	// move — only what the kind says its fields mean.
	if _, err := k.Define(ctx, forgetfulHolderKind()); err != nil {
		t.Fatalf("define holder v2: %v", err)
	}

	// The holder is still there and was still admitted under v1, so the
	// target is still held.
	holders, err := k.Holders(ctx, targetId(t, "held"))
	if err != nil {
		t.Fatalf("holders: %v", err)
	}

	if len(holders) != 1 {
		t.Fatalf("a v1 holder became invisible when v2 dropped the reference: %v", holders)
	}

	if _, err := k.Delete(ctx, targetId(t, "held"), target); !errors.Is(err, kernel.ErrReferenced) {
		t.Fatalf("removing what a v1 resource holds: want ErrReferenced, got %v", err)
	}
}

// A kind cannot change how its instances are addressed. It would not be a
// new version of that kind — it would be a different kind under a taken
// name, and every reference to it, stored as a written path, would come
// to mean something else at once.
func TestAKindCannotChangeItsShape(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bytes := memory.New()

	t.Cleanup(func() { _ = bytes.Close() })

	k := kernel.New(bytes)

	if _, err := k.Define(ctx, targetKind()); err != nil {
		t.Fatalf("define: %v", err)
	}

	reshaped := def.MustNew(
		kind.MustNew("Target"), path.MustNewTPath("tenant", "name"),
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "target-spec"}).MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "target-status"}).MustBuild()),
	)

	if _, err := k.Define(ctx, reshaped); !errors.Is(err, kernel.ErrShapeChanged) {
		t.Fatalf("want ErrShapeChanged, got %v", err)
	}

	// And the kind is untouched: a refused definition publishes nothing.
	head, err := k.Definition(ctx, kind.MustNew("Target"))
	if err != nil {
		t.Fatalf("definition: %v", err)
	}

	if !head.Version().Eq(1) {
		t.Fatalf("a refused definition moved the version to %s", head.Version())
	}
}

// A schema may still grow. What a resource IS may change; what addresses
// it may not.
func TestAKindMayStillChangeItsSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bytes := memory.New()

	t.Cleanup(func() { _ = bytes.Close() })

	k := kernel.New(bytes)

	if _, err := k.Define(ctx, targetKind()); err != nil {
		t.Fatalf("define: %v", err)
	}

	wider := def.MustNew(
		kind.MustNew("Target"), path.MustNewTPath("name"),
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "target-spec"}).
			Fields(schemapb.Str("note")).MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "target-status"}).MustBuild()),
	)

	head, err := k.Define(ctx, wider)
	if err != nil {
		t.Fatalf("widening a schema: %v", err)
	}

	if !head.Version().Eq(2) {
		t.Fatalf("a new schema landed at version %s", head.Version())
	}
}

// forgetfulHolderKind is holderKind with the reference dropped and the
// field left in place: v2 says the string is just a string.
func forgetfulHolderKind() def.Definition {
	return def.MustNew(
		kind.MustNew("Holder"), path.MustNewTPath("name"),
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "holder-spec"}).
			Fields(schemapb.Str("points")).MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "holder-status"}).MustBuild()),
	)
}
