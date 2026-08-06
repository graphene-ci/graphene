package report_test

import (
	"context"
	"testing"
	"time"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/app/config"
	"github.com/graphene-ci/graphene/internal/app/report"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store/kv/memory"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// A kernel writing its record says it is there, and a reader can tell.
func TestAKernelThatWroteItselfDownIsUp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	k := standing(t, ctx)

	if err := report.Write(ctx, k, running(t), "test"); err != nil {
		t.Fatalf("write: %v", err)
	}

	state, since := report.Alive(read(t, ctx, k), time.Now())
	if state != report.Up {
		t.Fatalf("a kernel that just wrote itself down is %q", state)
	}

	if since.IsZero() {
		t.Fatal("it is up and was never seen")
	}
}

// Silence past the grace is gone. This is the whole judgement, and it is
// the reader's: nothing was stored saying so.
func TestSilencePastTheGraceIsGone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	k := standing(t, ctx)

	if err := report.Write(ctx, k, running(t), "test"); err != nil {
		t.Fatalf("write: %v", err)
	}

	stored := read(t, ctx, k)

	// One second inside the grace, and one second past it. The boundary
	// is the only part of this worth testing, which is why Alive takes
	// the time rather than reading the clock.
	if state, _ := report.Alive(stored, time.Now().Add(report.Grace-time.Second)); state != report.Up {
		t.Fatalf("inside the grace: %q", state)
	}

	if state, _ := report.Alive(stored, time.Now().Add(report.Grace+time.Second)); state != report.Gone {
		t.Fatalf("past the grace: %q", state)
	}
}

// A record that never said is not "gone" — it is a kernel nobody has
// heard from, which is a different thing to act on and reads differently.
func TestARecordWithNoBeatIsSilent(t *testing.T) {
	t.Parallel()

	id, err := report.Id("quiet")
	if err != nil {
		t.Fatalf("id: %v", err)
	}

	ctx := context.Background()
	k := standing(t, ctx)

	intent, err := resource.NewIntent(id, schemapb.MustStructFromGo(map[string]any{}))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	if _, err := k.Put(ctx, intent, 0); err != nil {
		t.Fatalf("put: %v", err)
	}

	stored, err := k.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if state, _ := report.Alive(stored.Value, time.Now()); state != report.Silent {
		t.Fatalf("a record with no beat is %q", state)
	}
}

// The grace comes from the RECORD, so a kernel that beats more slowly is
// not called dead by a reader from a different build.
func TestTheGraceComesFromWhatTheKernelSaid(t *testing.T) {
	t.Parallel()

	slow := resourceWith(t, map[string]any{
		"heartbeat":    time.Now().UTC().Add(-report.Grace - time.Minute).Format(time.RFC3339),
		"beat_seconds": uint64((report.Grace + 10*time.Minute) / time.Second),
	})

	if state, _ := report.Alive(slow, time.Now()); state != report.Up {
		t.Fatalf("a kernel that said it beats slowly is %q", state)
	}
}

// A heartbeat nobody can read says nothing, and saying nothing is not
// saying "dead": that would page somebody about a kernel that is fine.
func TestAnUnreadableBeatSaysNothing(t *testing.T) {
	t.Parallel()

	broken := resourceWith(t, map[string]any{"heartbeat": "yesterday afternoon"})

	if state, _ := report.Alive(broken, time.Now()); state != report.Silent {
		t.Fatalf("an unreadable beat is %q", state)
	}
}

// standing is a kernel with the Kernel kind published.
func standing(t *testing.T, ctx context.Context) kernel.Kernel {
	t.Helper()

	bytes := memory.New()

	t.Cleanup(func() { _ = bytes.Close() })

	k := kernel.New(bytes)
	if err := report.Publish(ctx, k); err != nil {
		t.Fatalf("publish: %v", err)
	}

	return k
}

func running(t *testing.T) config.Config {
	t.Helper()

	return config.NewLocal("one", "127.0.0.1:0", t.TempDir()+"/kernel.db", 8, "")
}

func read(t *testing.T, ctx context.Context, k kernel.Kernel) resource.Resource {
	t.Helper()

	id, err := report.Id("one")
	if err != nil {
		t.Fatalf("id: %v", err)
	}

	stored, err := k.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	return stored.Value
}

// resourceWith builds a record with a status somebody wrote, without a
// store: what is being tested is a reading of a status, so the status is
// the input.
func resourceWith(t *testing.T, status map[string]any) resource.Resource {
	t.Helper()

	ctx := context.Background()
	k := standing(t, ctx)

	id, err := report.Id("one")
	if err != nil {
		t.Fatalf("id: %v", err)
	}

	intent, err := resource.NewIntent(id, schemapb.MustStructFromGo(map[string]any{}))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	at, err := k.Put(ctx, intent, 0)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	if _, err := k.Report(ctx, id, schemapb.MustStructFromGo(status), at); err != nil {
		t.Fatalf("report: %v", err)
	}

	return read(t, ctx, k)
}
