package presence_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/auth"
	"github.com/graphene-ci/graphene/internal/core/builtin"
	"github.com/graphene-ci/graphene/internal/core/controller"
	"github.com/graphene-ci/graphene/internal/core/key"
	"github.com/graphene-ci/graphene/internal/core/presence"
	"github.com/graphene-ci/graphene/internal/core/registry"
	"github.com/graphene-ci/graphene/internal/core/service"
	"github.com/graphene-ci/graphene/internal/infrastructure/store/bbolt"
)

// newWriter builds a writer over a real store and service: presence is
// worth testing against the same validation everything else goes through.
func newWriter(t *testing.T) (controller.Writer, context.Context) {
	t.Helper()

	st, err := bbolt.Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	t.Cleanup(func() { _ = st.Close() })

	ctx := auth.WithCredentials(context.Background(), auth.FullAccess(auth.PrincipalSystem, "test"))

	reg := registry.New(st)
	if err := builtin.Ensure(ctx, reg); err != nil {
		t.Fatalf("ensure builtins: %v", err)
	}

	return controller.OverService(service.NewResources(st, reg)), ctx
}

func kernelResource(arch string) *graphenepbv1.Resource {
	return &graphenepbv1.Resource{
		Key:  &graphenepbv1.Key{Kind: builtin.KindKernel, Path: []string{"k1"}},
		Spec: schemapb.MustStructFromGo(map[string]any{"os": "linux", "arch": arch}),
	}
}

func read(ctx context.Context, t *testing.T, writer controller.Writer) *graphenepbv1.Resource {
	t.Helper()

	got, err := writer.Get(ctx, kernelKey())
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	return got
}

func kernelKey() key.Key { return key.New(builtin.KindKernel, "k1") }

func leaseKey() key.Key { return key.New(builtin.KindKernelLease, "k1") }

// Ensure creates what is absent, updates what drifted, and — the part that
// matters — writes NOTHING when the resource already says what it should.
// A revision bump is an event for every watcher in the system.
func TestEnsureIsQuietWhenNothingChanged(t *testing.T) {
	t.Parallel()

	writer, ctx := newWriter(t)

	if err := presence.Ensure(ctx, writer, kernelResource("amd64")); err != nil {
		t.Fatalf("create: %v", err)
	}

	created := read(ctx, t, writer).GetRevision()

	if err := presence.Ensure(ctx, writer, kernelResource("amd64")); err != nil {
		t.Fatalf("re-ensure: %v", err)
	}

	if got := read(ctx, t, writer).GetRevision(); got != created {
		t.Fatalf("an unchanged resource was rewritten: revision %d → %d", created, got)
	}

	// A drifted spec IS written: the machine was re-provisioned.
	if err := presence.Ensure(ctx, writer, kernelResource("arm64")); err != nil {
		t.Fatalf("update: %v", err)
	}

	updated := read(ctx, t, writer)
	if updated.GetRevision() == created {
		t.Fatal("a changed spec was not written")
	}

	if updated.GetSpec().ToGo()["arch"] != "arm64" {
		t.Fatalf("spec: %v", updated.GetSpec().ToGo())
	}
}

// Registration must not clobber the status a controller wrote: this kernel
// owns what it IS, not what it has been judged to be.
func TestEnsureKeepsTheStatus(t *testing.T) {
	t.Parallel()

	writer, ctx := newWriter(t)

	if err := presence.Ensure(ctx, writer, kernelResource("amd64")); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Something else marks it online, the way the lease controller does.
	judged := read(ctx, t, writer)
	judged.Status = schemapb.MustStructFromGo(map[string]any{"online": true})

	if err := writer.Put(ctx, judged, judged.GetRevision()); err != nil {
		t.Fatalf("write status: %v", err)
	}

	// The kernel re-registers with a changed spec.
	if err := presence.Ensure(ctx, writer, kernelResource("arm64")); err != nil {
		t.Fatalf("re-register: %v", err)
	}

	got := read(ctx, t, writer)
	if online, _ := got.GetStatus().ToGo()["online"].(bool); !online {
		t.Fatalf("re-registration erased the status: %v", got.GetStatus().ToGo())
	}
}

// Renew writes every time, changed or not — the revision bump IS the
// heartbeat, so a lease that stopped bumping is a kernel that stopped.
func TestRenewAlwaysWrites(t *testing.T) {
	t.Parallel()

	writer, ctx := newWriter(t)

	lease := func() *graphenepbv1.Resource {
		return &graphenepbv1.Resource{
			Key: &graphenepbv1.Key{Kind: builtin.KindKernelLease, Path: []string{"k1"}},
			Spec: schemapb.MustStructFromGo(map[string]any{
				"kernel": "k1", "ttl_seconds": int64(30),
			}),
		}
	}

	revisions := make([]uint64, 0, 3)

	for range 3 {
		if err := presence.Renew(ctx, writer, lease()); err != nil {
			t.Fatalf("renew: %v", err)
		}

		got, err := writer.Get(ctx, leaseKey())
		if err != nil {
			t.Fatalf("read lease: %v", err)
		}

		revisions = append(revisions, got.GetRevision())
	}

	if revisions[0] >= revisions[1] || revisions[1] >= revisions[2] {
		t.Fatalf("renewals did not bump the revision: %v", revisions)
	}
}

// An absent resource reads the same whether the truth is in-process or a
// link away, so Ensure and Renew never have to ask which one they have.
func TestAbsentIsUniform(t *testing.T) {
	t.Parallel()

	writer, ctx := newWriter(t)

	if _, err := writer.Get(ctx, kernelKey()); !errors.Is(err, controller.ErrAbsent) {
		t.Fatalf("missing resource: got %v, want ErrAbsent", err)
	}
}
