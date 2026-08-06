package kernel_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/infrastructure/kv/bbolt"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/store/kv/memory"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// patience bounds a Next that is expected to have something waiting.
// Delivery is synchronous with the write, so an event that is coming is
// already queued before Next is called; this only stops a bug from
// hanging the suite.
const patience = 50 * time.Millisecond

// stores is every byte layer the kernel is expected to work on.
//
// The kernel is written against the port and never against a store, so
// running its whole behaviour on both is how that claim stays honest. It
// is also the only place the two are compared doing real work rather than
// passing the same conformance suite separately — and a store can pass
// that suite and still be wrong about something only a caller stringing
// several calls together would notice.
var stores = map[string]func(t *testing.T) kv.Store{
	"memory": func(*testing.T) kv.Store {
		return memory.New()
	},
	"bbolt": func(t *testing.T) kv.Store {
		t.Helper()

		opened, err := bbolt.Open(filepath.Join(t.TempDir(), "store.db"))
		if err != nil {
			t.Fatalf("open: %v", err)
		}

		return opened
	},
}

// each runs one behaviour on every byte layer.
func each(t *testing.T, body func(t *testing.T, k kernel.Kernel)) {
	t.Helper()

	for name, open := range stores {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			bytes := open(t)
			t.Cleanup(func() { _ = bytes.Close() })

			body(t, kernel.New(bytes))
		})
	}
}

// process is the kind these tests define. bundle is required, so a spec
// missing it is refused by the schema and not by anything here.
func process(t *testing.T, extra ...string) def.Definition {
	t.Helper()

	fields := []schemapb.FieldDef{schemapb.Str("bundle").Required()}
	for _, name := range extra {
		fields = append(fields, schemapb.Str(schemapb.FieldName(name)))
	}

	spec := def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-spec"}).
		Fields(fields...).
		MustBuild())

	status := def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-status"}).
		Fields(schemapb.Str("phase")).
		MustBuild())

	built, err := def.New(kind.MustNew("Process"), path.MustNewTPath("kernel", "name"), spec, status)
	if err != nil {
		t.Fatalf("definition: %v", err)
	}

	return built
}

func id(t *testing.T, values ...string) resource.Id {
	t.Helper()

	at, err := path.MustNewTPath("kernel", "name").New(values...)
	if err != nil {
		t.Fatalf("path %v: %v", values, err)
	}

	return resource.NewId(kind.MustNew("Process"), at)
}

func intent(t *testing.T, bundle string) resource.Intent {
	t.Helper()

	stated, err := resource.NewIntent(id(t, "local", "web"),
		schemapb.MustStructFromGo(map[string]any{"bundle": bundle}))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	return stated
}

func other(t *testing.T, name string) resource.Finalizer {
	t.Helper()

	built, err := resource.NewFinalizer(name)
	if err != nil {
		t.Fatalf("finalizer: %v", err)
	}

	return built
}

func define(t *testing.T, k kernel.Kernel, definition def.Definition) def.Head {
	t.Helper()

	head, err := k.Define(context.Background(), definition)
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	return head
}

// Declaring the same shape twice leaves one version. Without this, every
// apply of an unchanged manifest would churn a version and every resource
// written afterwards would pin a number that meant nothing.
func TestDefiningTheSameShapeTwiceChangesNothing(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		first := define(t, k, process(t))
		if !first.Version().Eq(1) {
			t.Fatalf("first definition is %s", first.Version())
		}

		again := define(t, k, process(t))
		if !again.Version().Eq(1) {
			t.Fatalf("redefining the same shape moved to %s", again.Version())
		}

		changed := define(t, k, process(t, "identity"))
		if !changed.Version().Eq(2) {
			t.Fatalf("a changed shape moved to %s", changed.Version())
		}

		// The old one is still readable, because instances pin it.
		if _, err := k.DefinitionAt(context.Background(), first.Kind(), 1); err != nil {
			t.Fatalf("reading the version a resource might pin: %v", err)
		}
	})
}

func TestAnUndefinedKindAdmitsNothing(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		if _, err := k.Put(ctx, intent(t, "b1"), revision.Absent); !errors.Is(err, kernel.ErrNoSuchKind) {
			t.Fatalf("want ErrNoSuchKind, got %v", err)
		}

		if _, err := k.Definition(ctx, kind.MustNew("Process")); !errors.Is(err, kernel.ErrNoSuchKind) {
			t.Fatalf("want ErrNoSuchKind, got %v", err)
		}
	})
}

// A resource pins the version it was admitted under, and keeps it while
// the kind moves on underneath.
func TestAResourcePinsTheVersionItWasAdmittedUnder(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		define(t, k, process(t))

		at, err := k.Put(ctx, intent(t, "b1"), revision.Absent)
		if err != nil {
			t.Fatalf("put: %v", err)
		}

		define(t, k, process(t, "identity"))

		stored, err := k.Get(ctx, id(t, "local", "web"))
		if err != nil {
			t.Fatalf("get: %v", err)
		}

		if !stored.Value.DefinitionVersion().Eq(1) {
			t.Fatalf("pinned %s after the kind moved to v2", stored.Value.DefinitionVersion())
		}

		if !stored.Revision.Eq(at) {
			t.Fatalf("stored at %s, written at %s", stored.Revision, at)
		}
	})
}

// A caller writing from a read somebody else has overtaken is told that,
// and told it before anything about its spec.
func TestAStaleWriteIsRefused(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		define(t, k, process(t))

		first, err := k.Put(ctx, intent(t, "b1"), revision.Absent)
		if err != nil {
			t.Fatalf("put: %v", err)
		}

		if _, err := k.Put(ctx, intent(t, "b2"), first); err != nil {
			t.Fatalf("second put: %v", err)
		}

		if _, err := k.Put(ctx, intent(t, "b3"), first); !errors.Is(err, revision.ErrConflict) {
			t.Fatalf("want ErrConflict, got %v", err)
		}

		// Creating over something that exists is the same mistake wearing
		// a different expectation.
		if _, err := k.Put(ctx, intent(t, "b4"), revision.Absent); !errors.Is(err, revision.ErrConflict) {
			t.Fatalf("creating twice: want ErrConflict, got %v", err)
		}
	})
}

// Generation counts intent. A status report moves the revision and must
// not move the generation, or the controller that wrote it wakes itself
// forever.
func TestReportingStatusDoesNotMoveTheGeneration(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		define(t, k, process(t))

		at, err := k.Put(ctx, intent(t, "b1"), revision.Absent)
		if err != nil {
			t.Fatalf("put: %v", err)
		}

		reported, err := k.Report(ctx, id(t, "local", "web"),
			schemapb.MustStructFromGo(map[string]any{"phase": "running"}), at)
		if err != nil {
			t.Fatalf("report: %v", err)
		}

		if !reported.After(at) {
			t.Fatalf("the report is at %s, the write at %s", reported, at)
		}

		stored, err := k.Get(ctx, id(t, "local", "web"))
		if err != nil {
			t.Fatalf("get: %v", err)
		}

		if !stored.Value.Generation().Eq(1) {
			t.Fatalf("the generation moved to %s", stored.Value.Generation())
		}

		if stored.Value.Status().ToGo()["phase"] != "running" {
			t.Fatalf("status came back as %v", stored.Value.Status().ToGo())
		}
	})
}

// A resource nobody has a claim on goes at once; one with a claim stays,
// marked, until the claim is released — and releasing the last one is
// what finally removes it.
func TestDeletionWaitsForFinalizers(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		define(t, k, process(t))

		claim, err := resource.NewFinalizer("graphene.io/gc")
		if err != nil {
			t.Fatalf("finalizer: %v", err)
		}

		at, err := k.Put(ctx, intent(t, "b1"), revision.Absent)
		if err != nil {
			t.Fatalf("put: %v", err)
		}

		// A claim is placed by whoever will do the cleaning, which is its
		// own act — not part of writing the spec.
		at, err = k.Claim(ctx, id(t, "local", "web"), claim, at)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}

		marked, err := k.Delete(ctx, id(t, "local", "web"), at)
		if err != nil {
			t.Fatalf("delete: %v", err)
		}

		stored, err := k.Get(ctx, id(t, "local", "web"))
		if err != nil {
			t.Fatalf("a claimed resource left before its claim was released: %v", err)
		}

		if !stored.Value.IsDeleting() {
			t.Fatal("the resource was not marked")
		}

		// Its spec cannot change while it is going away: the claim is
		// being worked against the spec it had.
		if _, err := k.Put(ctx, intent(t, "b2"), marked); !errors.Is(err, resource.ErrDeleting) {
			t.Fatalf("want ErrDeleting, got %v", err)
		}

		// And nobody may claim it now either — a claim after the mark
		// would hold the deletion open forever.
		_, err = k.Claim(ctx, id(t, "local", "web"), other(t, "graphene.io/late"), marked)
		if !errors.Is(err, resource.ErrClaimWhileDeleting) {
			t.Fatalf("want ErrClaimWhileDeleting, got %v", err)
		}

		// Releasing the last claim is what finally removes the record.
		if _, err := k.Release(ctx, id(t, "local", "web"), claim, marked); err != nil {
			t.Fatalf("releasing the claim: %v", err)
		}

		if _, err := k.Get(ctx, id(t, "local", "web")); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})
}

// A kind cannot be removed while instances of it are left: removing it
// would leave records nothing can validate, read back or address.
func TestAKindWithInstancesCannotBeRemoved(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		head := define(t, k, process(t))
		define(t, k, process(t, "identity"))

		at, err := k.Put(ctx, intent(t, "b1"), revision.Absent)
		if err != nil {
			t.Fatalf("put: %v", err)
		}

		if err := k.Undefine(ctx, head.Kind()); !errors.Is(err, kernel.ErrKindInUse) {
			t.Fatalf("want ErrKindInUse, got %v", err)
		}

		if _, err := k.Delete(ctx, id(t, "local", "web"), at); err != nil {
			t.Fatalf("delete: %v", err)
		}

		if err := k.Undefine(ctx, head.Kind()); err != nil {
			t.Fatalf("undefine: %v", err)
		}

		// Both the head and every version it had are gone.
		if _, err := k.Definition(ctx, head.Kind()); !errors.Is(err, kernel.ErrNoSuchKind) {
			t.Fatalf("want ErrNoSuchKind, got %v", err)
		}

		if _, err := k.DefinitionAt(ctx, head.Kind(), 1); !errors.Is(err, kernel.ErrNoSuchVersion) {
			t.Fatalf("want ErrNoSuchVersion, got %v", err)
		}
	})
}

// Kinds are listed off the head records, which is one scan of one prefix
// rather than a walk of every version of everything.
func TestKindsAreListedFromTheirHeads(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		define(t, k, process(t))
		define(t, k, process(t, "identity"))

		seen := map[string]def.Version{}

		for head, err := range k.Kinds(ctx) {
			if err != nil {
				t.Fatalf("kinds: %v", err)
			}

			seen[head.Kind().String()] = head.Version()
		}

		if len(seen) != 1 || seen["Process"] != 2 {
			t.Fatalf("listed %v", seen)
		}
	})
}

// A watch on the kernel delivers ADMITTED resources: decoded, with the
// generation the kernel counted. That is the whole difference between
// watching the kernel and watching the bytes.
func TestAWatchDeliversAdmittedResources(t *testing.T) {
	t.Parallel()

	each(t, func(t *testing.T, k kernel.Kernel) {
		ctx := context.Background()

		define(t, k, process(t))

		everything, err := path.MustNewTPath("kernel", "name").New()
		if err != nil {
			t.Fatalf("prefix: %v", err)
		}

		stream, err := k.Watch(ctx,
			resource.NewId(kind.MustNew("Process"), everything), revision.Beginning)
		if err != nil {
			t.Fatalf("watch: %v", err)
		}

		defer func() { _ = stream.Close() }()

		if _, err := k.Put(ctx, intent(t, "b1"), revision.Absent); err != nil {
			t.Fatalf("put: %v", err)
		}

		// Delivery is synchronous with the write, so the event is already
		// waiting: no sleeping, no goroutine, no flake.
		deadline, cancel := context.WithTimeout(ctx, patience)
		defer cancel()

		event, err := stream.Next(deadline)
		if err != nil {
			t.Fatalf("next: %v", err)
		}

		if event.Kind != store.EventPut {
			t.Fatalf("delivered %v", event.Kind)
		}

		if event.Value.Value.Spec().ToGo()["bundle"] != "b1" {
			t.Fatalf("the event carried %v", event.Value.Value.Spec().ToGo())
		}

		if !event.Value.Value.Generation().Eq(1) {
			t.Fatalf("the event carried generation %s", event.Value.Value.Generation())
		}
	})
}

// A removal interrupted after the head went and before the versions did
// can be finished by running it again.
//
// It could not before, and the failure was silent and permanent: the
// sweep counted down from what the head said, and the head is the first
// thing removed — so a second run found no head, refused, and left
// versions nobody could ever list, reach or remove. Walking the prefix
// instead needs no head, which is what makes the operation resumable.
//
// The kernel is built by hand here rather than through the usual helper,
// because reaching the state a crash leaves means reaching PAST the
// kernel: the head is a record only its own codec can read, so removing
// it the way an interrupted run did means removing the key.
func TestAnInterruptedRemovalCanBeFinished(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	bytes := memory.New()
	t.Cleanup(func() { _ = bytes.Close() })

	k := kernel.New(bytes)

	head := define(t, k, process(t))
	define(t, k, process(t, "identity"))

	at, err := def.HeadPath(head.Kind())
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	key := store.KeyOf(resource.NewId(def.HeadKind, at))

	entry, err := bytes.Get(ctx, key)
	if err != nil {
		t.Fatalf("the head should be there: %v", err)
	}

	if _, err := bytes.Delete(ctx, key, entry.Revision); err != nil {
		t.Fatalf("delete head: %v", err)
	}

	// Exactly what an interrupted removal leaves.
	if _, err := k.Definition(ctx, head.Kind()); !errors.Is(err, kernel.ErrNoSuchKind) {
		t.Fatalf("the head should be gone: %v", err)
	}

	if _, err := k.DefinitionAt(ctx, head.Kind(), 1); err != nil {
		t.Fatalf("the versions should still be there: %v", err)
	}

	// Running it again finishes what was left.
	if err := k.Undefine(ctx, head.Kind()); err != nil {
		t.Fatalf("finishing an interrupted removal: %v", err)
	}

	for _, version := range []def.Version{1, 2} {
		if _, err := k.DefinitionAt(ctx, head.Kind(), version); !errors.Is(err, kernel.ErrNoSuchVersion) {
			t.Fatalf("version %s survived: %v", version, err)
		}
	}

	// And a kind that never existed is still told apart from one that was
	// removed: nothing there, and nothing left to sweep.
	if err := k.Undefine(ctx, head.Kind()); !errors.Is(err, kernel.ErrNoSuchKind) {
		t.Fatalf("want ErrNoSuchKind, got %v", err)
	}
}
