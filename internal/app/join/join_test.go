package join_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/app/join"
	"github.com/graphene-ci/graphene/internal/app/report"
	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/blob"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/process"
	"github.com/graphene-ci/graphene/internal/store/kv/memory"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// A kernel that joined can do what a kernel does, and can be checked
// doing it: these tests go through the GUARD, because a grant that is
// only inspected is a grant nobody ran.
func TestAJoinedKernelMayDoWhatAKernelDoes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	k, guard := standing(t, ctx)

	token, err := join.Join(ctx, k, "edge")
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	who := principal(t, token)
	session := guard.As(who)

	// Its own record: it creates it, and reports what it is running.
	own, err := report.Id("edge")
	if err != nil {
		t.Fatalf("id: %v", err)
	}

	intent, err := resource.NewIntent(own, schemapb.MustStructFromGo(map[string]any{}))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	at, err := session.Put(ctx, intent, revision.Absent)
	if err != nil {
		t.Fatalf("its own record: %v", err)
	}

	if _, err := session.Report(ctx, own,
		schemapb.MustStructFromGo(map[string]any{"listen": "127.0.0.1:1"}), at); err != nil {
		t.Fatalf("reporting what it is: %v", err)
	}

	// Its own processes: it reads them and says what became of them.
	mine, err := process.Id("edge", "one")
	if err != nil {
		t.Fatalf("process id: %v", err)
	}

	writeProcess(t, ctx, k, mine)

	if _, err := session.Get(ctx, mine); err != nil {
		t.Fatalf("reading its own process: %v", err)
	}

	stored, err := session.Get(ctx, mine)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if _, err := session.Report(ctx, mine,
		schemapb.MustStructFromGo(map[string]any{"phase": process.PhaseRunning}),
		stored.Revision); err != nil {
		t.Fatalf("reporting what became of it: %v", err)
	}

	// And bytes, which is how it gets what it was told to run.
	if err := session.May(ctx, auth.Get, blob.Kind); err != nil {
		t.Fatalf("fetching bytes: %v", err)
	}
}

// And nothing else. Each of these is a thing somebody would reach for
// when the narrow grant got in their way, and each is the reason it is
// narrow.
func TestAJoinedKernelMayNotDoAnythingElse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	k, guard := standing(t, ctx)

	token, err := join.Join(ctx, k, "edge")
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	session := guard.As(principal(t, token))

	// Another kernel's record. A kernel that could write one could keep a
	// dead machine looking alive.
	elsewhere, err := report.Id("other")
	if err != nil {
		t.Fatalf("id: %v", err)
	}

	other, err := resource.NewIntent(elsewhere, schemapb.MustStructFromGo(map[string]any{}))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	if _, err := session.Put(ctx, other, revision.Absent); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("another kernel's record: want ErrForbidden, got %v", err)
	}

	// Another kernel's processes.
	theirs, err := process.Id("other", "one")
	if err != nil {
		t.Fatalf("process id: %v", err)
	}

	writeProcess(t, ctx, k, theirs)

	if _, err := session.Get(ctx, theirs); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("another kernel's process: want ErrForbidden, got %v", err)
	}

	// Writing what to run — on its OWN kernel. An agent that could do
	// this would be arguing with its orders rather than carrying them.
	mine, err := process.Id("edge", "invented")
	if err != nil {
		t.Fatalf("process id: %v", err)
	}

	own, err := resource.NewIntent(mine, schemapb.MustStructFromGo(map[string]any{
		"blob":   "0123456789abcdef0123456789abcdef",
		"format": process.RawExec,
	}))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	if _, err := session.Put(ctx, own, revision.Absent); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("writing its own orders: want ErrForbidden, got %v", err)
	}

	// Identities. The one that would make all the others moot.
	root, err := auth.IdentityId(auth.First)
	if err != nil {
		t.Fatalf("identity id: %v", err)
	}

	if _, err := session.Get(ctx, root); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("reading an identity: want ErrForbidden, got %v", err)
	}

	// And uploading bytes: a kernel fetches what it was told to run and
	// does not decide what anybody runs.
	if err := session.May(ctx, auth.Put, blob.Kind); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("uploading bytes: want ErrForbidden, got %v", err)
	}
}

// Joining twice is refused rather than quietly minting a second
// credential: the second one would silently be the only one that works,
// and the machine using the first would drop off the network.
func TestJoiningTwiceIsRefused(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	k, _ := standing(t, ctx)

	if _, err := join.Join(ctx, k, "edge"); err != nil {
		t.Fatalf("join: %v", err)
	}

	if _, err := join.Join(ctx, k, "edge"); !errors.Is(err, join.ErrExists) {
		t.Fatalf("want ErrExists, got %v", err)
	}
}

// standing is a kernel with everything published that a kernel's grants
// name.
func standing(t *testing.T, ctx context.Context) (kernel.Kernel, auth.Guard) {
	t.Helper()

	bytes := memory.New()

	t.Cleanup(func() { _ = bytes.Close() })

	k := kernel.New(bytes)

	if err := auth.Bootstrap(ctx, k); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	if err := report.Publish(ctx, k); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if _, err := k.Define(ctx, process.Definition()); err != nil {
		t.Fatalf("define process: %v", err)
	}

	return k, auth.New(k)
}

func principal(t *testing.T, token auth.Token) auth.Principal {
	t.Helper()

	name, _, err := auth.Split(token.String())
	if err != nil {
		t.Fatalf("token: %v", err)
	}

	who, err := auth.NewPrincipal(name)
	if err != nil {
		t.Fatalf("principal: %v", err)
	}

	return who
}

// writeProcess puts orders on a kernel, the way whoever placed them does
// — unguarded, because who may place them is a different test.
func writeProcess(t *testing.T, ctx context.Context, k kernel.Kernel, id resource.Id) {
	t.Helper()

	intent, err := resource.NewIntent(id, schemapb.MustStructFromGo(map[string]any{
		"blob":   "0123456789abcdef0123456789abcdef",
		"format": process.RawExec,
	}))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	if _, err := k.Put(ctx, intent, revision.Absent); err != nil {
		t.Fatalf("put %s: %v", id, err)
	}
}
