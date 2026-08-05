package kernel_test

import (
	"context"
	"errors"
	"testing"

	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
)

// The head IS the current definition, so a change to it is exactly the
// event a controller wants — and publishing a version that does NOT
// become current produces nothing, which is right: nothing about the kind
// changed.
func TestWatchingAKindSeesItsCurrentDefinitionChange(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		define(t, k, process(t))

		at, err := k.Revision(ctx)
		if err != nil {
			t.Fatalf("revision: %v", err)
		}

		stream, err := k.WatchKind(ctx, kind.MustNew("Process"), at)
		if err != nil {
			t.Fatalf("watch: %v", err)
		}

		defer func() { _ = stream.Close() }()

		// Nothing has happened since the cursor.
		deadline, cancel := context.WithTimeout(ctx, patience)
		if _, err := stream.Next(deadline); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("a quiet watch handed back %v", err)
		}

		cancel()

		// Redefining the same shape changes nothing, so it says nothing.
		define(t, k, process(t))

		deadline, cancel = context.WithTimeout(ctx, patience)
		if _, err := stream.Next(deadline); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("redefining an unchanged shape was announced: %v", err)
		}

		cancel()

		// A new shape is a new current definition, and that is an event.
		define(t, k, process(t, "identity"))

		deadline, cancel = context.WithTimeout(ctx, patience)
		defer cancel()

		event, err := stream.Next(deadline)
		if err != nil {
			t.Fatalf("next: %v", err)
		}

		if event.Kind != store.EventPut {
			t.Fatalf("delivered %v", event.Kind)
		}

		if !event.Value.Value.Version().Eq(2) {
			t.Fatalf("announced version %s", event.Value.Value.Version())
		}

		if len(event.Value.Value.Definition().Spec().GetFields()) != 2 {
			t.Fatal("the event did not carry the new shape")
		}
	})
}

// Undefining a kind is a delete, and the event carries the definition
// that was current — so a controller learns which shape it is losing
// rather than only that it lost one.
func TestUndefiningAKindIsAnnouncedWithWhatItWas(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		head := define(t, k, process(t))

		at, err := k.Revision(ctx)
		if err != nil {
			t.Fatalf("revision: %v", err)
		}

		stream, err := k.WatchKinds(ctx, at)
		if err != nil {
			t.Fatalf("watch: %v", err)
		}

		defer func() { _ = stream.Close() }()

		if err := k.Undefine(ctx, head.Kind()); err != nil {
			t.Fatalf("undefine: %v", err)
		}

		deadline, cancel := context.WithTimeout(ctx, patience)
		defer cancel()

		event, err := stream.Next(deadline)
		if err != nil {
			t.Fatalf("next: %v", err)
		}

		if event.Kind != store.EventDelete {
			t.Fatalf("delivered %v", event.Kind)
		}

		if !event.Value.Value.Kind().Eq(head.Kind()) || !event.Value.Value.Version().Eq(1) {
			t.Fatalf("the delete carried %s", event.Value.Value)
		}
	})
}

// A kind named the way the kernel names its own records would put its
// instances in the key space the heads live in — a resource of it at /web
// encodes to exactly the key the head of a kind named "web" does, and one
// would overwrite the other with nothing noticing.
func TestTheKernelsOwnKindNamesAreReserved(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		for _, reserved := range []kind.Kind{def.HeadKind, def.PublishedKind} {
			shaped, err := def.New(reserved, def.HeadShape,
				process(t).Spec(), process(t).Status())
			if err != nil {
				t.Fatalf("definition: %v", err)
			}

			if _, err := k.Define(ctx, shaped); !errors.Is(err, kernel.ErrReservedKind) {
				t.Fatalf("%s: want ErrReservedKind, got %v", reserved, err)
			}
		}
	})
}
