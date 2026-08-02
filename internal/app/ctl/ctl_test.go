package ctl_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/app/config"
	appctl "github.com/graphene-ci/graphene/internal/app/ctl"
	appkernel "github.com/graphene-ci/graphene/internal/app/kernel"
	"github.com/graphene-ci/graphene/internal/core/builtin"
)

const bootstrapToken = "bootstrap-secret"

// startKernel runs a kernel serving a unix socket and returns a connected
// ctl client — the same client the command layer uses.
func startKernel(ctx context.Context, t *testing.T) *appctl.Client {
	t.Helper()

	dir := t.TempDir()
	socket := filepath.Join(dir, "graphene.sock")
	body := fmt.Sprintf(`
data_dir: %s
identity: { name: control }
log: { level: error }
store: {}
blobs: {}
listen: { uds: %s }
auth: { bootstrap: { token: { inline: %s } } }
`, dir, socket, bootstrapToken)

	path := filepath.Join(dir, "graphene.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	kern, err := appkernel.New(ctx, cfg, log)
	if err != nil {
		t.Fatalf("assemble kernel: %v", err)
	}

	t.Cleanup(func() { _ = kern.Close() })

	go func() {
		if err := kern.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("kernel run: %v", err)
		}
	}()

	client, err := appctl.Connect(appctl.Target{Socket: socket, Token: bootstrapToken})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	t.Cleanup(func() { _ = client.Close() })

	waitReady(ctx, t, client)

	return client
}

func waitReady(ctx context.Context, t *testing.T, client *appctl.Client) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := client.Definitions(ctx); err == nil {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("kernel never became reachable")
}

// TestApplyGetDelete drives the operator's loop: write a resource from
// YAML, read it back, and remove it — all through the public API.
func TestApplyGetDelete(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	client := startKernel(ctx, t)

	doc := fmt.Sprintf(`
key:
  kind: %s
  path: [k1]
spec:
  fields:
    os: { stringValue: linux }
    arch: { stringValue: amd64 }
`, builtin.KindKernel)

	applied, err := client.Apply(ctx, []byte(doc))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	if len(applied) != 1 || applied[0].GetKind() != builtin.KindKernel {
		t.Fatalf("apply: %+v", applied)
	}

	got, err := client.Get(ctx, builtin.KindKernel, []string{"k1"})
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.GetSpec().ToGo()["os"] != "linux" {
		t.Fatalf("spec round trip: %v", got.GetSpec().ToGo())
	}

	// What get prints is what apply reads: re-applying the printed form
	// must be a no-op update, not a conflict.
	printed, err := appctl.EncodeResources([]*graphenepbv1.Resource{got})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	if _, err := client.Apply(ctx, printed); err != nil {
		t.Fatalf("re-apply printed form: %v", err)
	}

	// The kernel serving this test registered ITSELF at startup, so the
	// listing holds two: its own record and the one just applied.
	listed, err := client.List(ctx, builtin.KindKernel, nil, nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if !listedPath(listed, "control") || !listedPath(listed, "k1") {
		t.Fatalf("list: %v", listed)
	}

	if err := client.Delete(ctx, builtin.KindKernel, []string{"k1"}, 0); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if _, err := client.Get(ctx, builtin.KindKernel, []string{"k1"}); err == nil {
		t.Fatal("resource still readable after delete")
	}
}

// TestWatchStream pins the stream contract ctl relies on: the catch-up
// marker arrives before live changes.
func TestWatchStream(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	client := startKernel(ctx, t)

	watchCtx, stop := context.WithCancel(ctx)
	defer stop()

	events := make(chan *graphenepbv1.WatchEvent, 8)

	go func() {
		_ = client.Watch(watchCtx, builtin.KindKernel, nil, nil, func(event *graphenepbv1.WatchEvent) error {
			events <- event

			return nil
		})
	}()

	// Catch-up first (this kernel's own registration is already there),
	// then the marker. Nothing live may arrive before it.
	for {
		event := recv(t, events)
		if event.GetType() == graphenepbv1.EventType_EVENT_TYPE_SYNC {
			break
		}

		if event.GetType() != graphenepbv1.EventType_EVENT_TYPE_PUT {
			t.Fatalf("catch-up event: got %v", event.GetType())
		}
	}

	doc := fmt.Sprintf("key:\n  kind: %s\n  path: [k2]\nspec:\n  fields:\n    os: { stringValue: linux }\n    arch: { stringValue: arm64 }\n",
		builtin.KindKernel)
	if _, err := client.Apply(ctx, []byte(doc)); err != nil {
		t.Fatalf("apply: %v", err)
	}

	live := recv(t, events)
	if live.GetType() != graphenepbv1.EventType_EVENT_TYPE_PUT {
		t.Fatalf("live event: got %v, want put", live.GetType())
	}

	if got := live.GetResource().GetKey().GetPath()[0]; got != "k2" {
		t.Fatalf("live event key: got %q", got)
	}
}

// TestRendering checks the operator-facing output: definitions as a table,
// events with their revisions.
func TestRendering(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	client := startKernel(ctx, t)

	defs, err := client.Definitions(ctx)
	if err != nil {
		t.Fatalf("definitions: %v", err)
	}

	var out bytes.Buffer
	if err := appctl.WriteDefinitions(&out, defs); err != nil {
		t.Fatalf("write definitions: %v", err)
	}

	text := out.String()
	for _, kind := range []string{builtin.KindKernel, builtin.KindKernelLease, builtin.KindRole, builtin.KindIdentity} {
		if !strings.Contains(text, kind) {
			t.Fatalf("definitions table missing %s:\n%s", kind, text)
		}
	}

	out.Reset()

	if err := appctl.WriteEvent(&out, &graphenepbv1.WatchEvent{
		Type:          graphenepbv1.EventType_EVENT_TYPE_SYNC,
		StoreRevision: 42,
	}); err != nil {
		t.Fatalf("write sync event: %v", err)
	}

	if !strings.Contains(out.String(), "synced at revision 42") {
		t.Fatalf("sync rendering: %q", out.String())
	}
}

func TestSelectorParsing(t *testing.T) {
	t.Parallel()

	got, err := appctl.ParseSelector([]string{"spec.placement=k1", "status.phase=running"})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if len(got) != 2 || got[0].GetPath() != "spec.placement" || got[1].GetValue() != "running" {
		t.Fatalf("parsed: %+v", got)
	}

	if _, err := appctl.ParseSelector([]string{"nonsense"}); err == nil {
		t.Fatal("selector without = accepted")
	}
}

func recv(t *testing.T, events <-chan *graphenepbv1.WatchEvent) *graphenepbv1.WatchEvent {
	t.Helper()

	select {
	case event := <-events:
		return event
	case <-time.After(20 * time.Second):
		t.Fatal("timeout waiting for a watch event")

		return nil
	}
}

func listedPath(resources []*graphenepbv1.Resource, name string) bool {
	for _, res := range resources {
		if res.GetKey().GetPath()[0] == name {
			return true
		}
	}

	return false
}
