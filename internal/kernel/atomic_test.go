package kernel_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/store/kv/memory"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// A check followed by a write is not a guarantee unless nothing can land
// in between. These are about the in-between.

// The race the transaction exists for: one caller points at a target
// while another removes it. Whichever wins, the reference must not
// survive the target.
//
// Run many times because a race that is usually won proves nothing; what
// is being tested is that there is no ordering in which both succeed.
func TestAReferenceNeverOutlivesItsTarget(t *testing.T) {
	t.Parallel()

	const attempts = 40

	for attempt := range attempts {
		ctx := context.Background()
		k := standing(t, ctx)

		target := writeTarget(t, ctx, k, "shared")

		var wait sync.WaitGroup

		wait.Add(2)

		// One points at it.
		go func() {
			defer wait.Done()

			_, _ = k.Put(ctx, holderIntent(t, "holder", "shared"), revision.Absent)
		}()

		// The other takes it away.
		go func() {
			defer wait.Done()

			_, _ = k.Delete(ctx, targetId(t, "shared"), target)
		}()

		wait.Wait()

		_, holding := k.Get(ctx, holderId(t, "holder"))
		_, pointed := k.Get(ctx, targetId(t, "shared"))

		if holding == nil && errors.Is(pointed, store.ErrNotFound) {
			t.Fatalf("attempt %d: the holder survived its target", attempt)
		}
	}
}

// A refused write leaves nothing behind. The integrity check runs after
// the resource has been admitted, so a store that wrote first and checked
// second would leave the record and report the refusal.
func TestARefusedWriteLeavesNothing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	k := standing(t, ctx)

	_, err := k.Put(ctx, holderIntent(t, "hopeful", "nobody"), revision.Absent)
	if err == nil {
		t.Fatal("a reference to nothing was accepted")
	}

	if _, err := k.Get(ctx, holderId(t, "hopeful")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the refused write is there anyway: %v", err)
	}
}

// standing is a kernel with a target kind and a kind that points at it.
func standing(t *testing.T, ctx context.Context) kernel.Kernel {
	t.Helper()

	bytes := memory.New()

	t.Cleanup(func() { _ = bytes.Close() })

	k := kernel.New(bytes)

	if _, err := k.Define(ctx, targetKind()); err != nil {
		t.Fatalf("define target: %v", err)
	}

	if _, err := k.Define(ctx, holderKind(t)); err != nil {
		t.Fatalf("define holder: %v", err)
	}

	return k
}

func targetKind() def.Definition {
	return def.MustNew(
		kind.MustNew("Target"), path.MustNewTPath("name"),
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "target-spec"}).MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "target-status"}).MustBuild()),
	)
}

func holderKind(t *testing.T) def.Definition {
	t.Helper()

	field, err := path.NewFieldPath(def.SpecRoot, "points")
	if err != nil {
		t.Fatalf("field path: %v", err)
	}

	points, err := def.NewRef(field, kind.MustNew("Target"), def.Strong)
	if err != nil {
		t.Fatalf("ref: %v", err)
	}

	return def.MustNew(
		kind.MustNew("Holder"), path.MustNewTPath("name"),
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "holder-spec"}).
			Fields(schemapb.Str("points")).MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "holder-status"}).MustBuild()),
		def.Reference(points),
	)
}

func targetId(t *testing.T, name string) resource.Id {
	t.Helper()

	at, err := path.MustNewTPath("name").New(name)
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	return resource.NewId(kind.MustNew("Target"), at)
}

func holderId(t *testing.T, name string) resource.Id {
	t.Helper()

	at, err := path.MustNewTPath("name").New(name)
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	return resource.NewId(kind.MustNew("Holder"), at)
}

func writeTarget(t *testing.T, ctx context.Context, k kernel.Kernel, name string) revision.Revision {
	t.Helper()

	intent, err := resource.NewIntent(targetId(t, name), schemapb.MustStructFromGo(map[string]any{}))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	at, err := k.Put(ctx, intent, revision.Absent)
	if err != nil {
		t.Fatalf("put target: %v", err)
	}

	return at
}

func holderIntent(t *testing.T, name, points string) resource.Intent {
	t.Helper()

	intent, err := resource.NewIntent(holderId(t, name),
		schemapb.MustStructFromGo(map[string]any{"points": "/" + points}))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	return intent
}
