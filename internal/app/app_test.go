package app_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/graphene-ci/graphene/internal/app"
	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// patience bounds a wait that is expected to end at once. Nothing here
// sleeps for it: the config is applied by the loop, and this only stops a
// bug from hanging the suite.
const patience = 2 * time.Second

func open(t *testing.T, at string) *app.App {
	t.Helper()

	opened, err := app.Open(context.Background(), app.Bootstrap{
		Store:   at,
		Name:    "local",
		Version: "test",
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	t.Cleanup(func() { _ = opened.Close() })

	return opened
}

// Everything a kernel publishes goes through Define, which is idempotent
// against the shape. So starting is safe to repeat, and there is no "have
// I been here before" flag to get wrong.
func TestStartingIsSafeToRepeat(t *testing.T) {
	t.Parallel()

	at := filepath.Join(t.TempDir(), "kernel.db")
	ctx := context.Background()

	first := open(t, at)

	head, err := first.Kernel().Definition(ctx, auth.RoleKind)
	if err != nil {
		t.Fatalf("definition: %v", err)
	}

	if !head.Version().Eq(1) {
		t.Fatalf("first start published version %s", head.Version())
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	second := open(t, at)

	head, err = second.Kernel().Definition(ctx, auth.RoleKind)
	if err != nil {
		t.Fatalf("definition: %v", err)
	}

	if !head.Version().Eq(1) {
		t.Fatalf("starting again published version %s", head.Version())
	}
}

// A kernel writes its own record once, with defaults, and reports what
// this build is every time. The spec is an administrator's afterwards: a
// kernel that rewrote it at every start would undo their edits on every
// restart.
func TestAKernelDescribesItselfWithoutOverwritingItsConfig(t *testing.T) {
	t.Parallel()

	at := filepath.Join(t.TempDir(), "kernel.db")
	ctx := context.Background()

	first := open(t, at)

	if got := first.Config().Listen(); got != "127.0.0.1:7373" {
		t.Fatalf("a fresh kernel is configured to listen on %q", got)
	}

	id, err := app.KernelId("local")
	if err != nil {
		t.Fatalf("id: %v", err)
	}

	// The status says what is running, which is not anybody's to choose.
	stored, err := first.Kernel().Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if stored.Value.Status().ToGo()["version"] != "test" {
		t.Fatalf("reported %v", stored.Value.Status().ToGo())
	}

	// An administrator edits the spec.
	edited, err := resource.NewIntent(id, app.NewConfig("0.0.0.0:9999", 16).Spec())
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	if _, err := first.Kernel().Put(ctx, edited, stored.Revision); err != nil {
		t.Fatalf("put: %v", err)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// And it survives the restart.
	second := open(t, at)

	if got := second.Config().Listen(); got != "0.0.0.0:9999" {
		t.Fatalf("restarting reset the configuration to %q", got)
	}

	if got := second.Config().Cache(); got != 16 {
		t.Fatalf("restarting reset the cache to %d", got)
	}
}

// A kernel watching its own store is the first real consumer of the
// watch, and this is the whole of what it buys: an edit reaches a running
// kernel without a restart.
func TestAnEditReachesARunningKernel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	running := open(t, filepath.Join(t.TempDir(), "kernel.db"))

	// The one goroutine, started where the test assembles things — which
	// is the same rule main follows.
	followed := make(chan error, 1)

	go func() { followed <- running.Follow(ctx) }()

	id, err := app.KernelId("local")
	if err != nil {
		t.Fatalf("id: %v", err)
	}

	stored, err := running.Kernel().Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	edited, err := resource.NewIntent(id, app.NewConfig("0.0.0.0:9999", 0).Spec())
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	if _, err := running.Kernel().Put(ctx, edited, stored.Revision); err != nil {
		t.Fatalf("put: %v", err)
	}

	until := time.After(patience)

	for running.Config().Listen() != "0.0.0.0:9999" {
		select {
		case <-until:
			t.Fatalf("the edit never reached the kernel; it still has %q",
				running.Config().Listen())
		case err := <-followed:
			t.Fatalf("the watch stopped: %v", err)
		default:
		}
	}

	cancel()
	<-followed
}

// A configuration written down and read back is the same configuration.
//
// This looks too small to be worth a test, and it is the one that caught
// a real bug: the schema said uint32 while the writer produced a uint64,
// so the reader found nothing where the number was — and nothing is
// indistinguishable from unset, which reads as a default rather than as a
// mistake. Every field, non-zero, or it proves nothing.
func TestAConfigSurvivesBeingWrittenDown(t *testing.T) {
	t.Parallel()

	original := app.NewConfig("0.0.0.0:9999", 16)

	back := app.ConfigFrom(original.Spec())
	if !back.Eq(original) {
		t.Fatalf("%s came back as %s", original, back)
	}

	// An empty record is every default, not every zero.
	empty := app.ConfigFrom(app.NewConfig("", 0).Spec())
	if empty.Listen() == "" || empty.Cache() == 0 {
		t.Fatalf("defaults came back as %s", empty)
	}
}
