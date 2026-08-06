package app_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/graphene-ci/graphene/internal/app"
	"github.com/graphene-ci/graphene/internal/auth"
)

// patience bounds a wait that is expected to end at once. Nothing here
// sleeps for it: the config is applied by the loop, and this only stops a
// bug from hanging the suite.
const patience = 5 * time.Second

// open starts a kernel configured by a file in a fresh directory.
func open(t *testing.T, config Config) *app.App {
	t.Helper()

	opened, err := app.Open(context.Background(), app.Bootstrap{
		Config:  config.path,
		Version: "test",
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	t.Cleanup(func() { _ = opened.Close() })

	return opened
}

// Config is a written configuration file and where it is.
type Config struct {
	path  string
	store string
}

// write puts a configuration on disk and hands back where it is.
func write(t *testing.T, at string, listen string) Config {
	t.Helper()

	dir := t.TempDir()
	config := Config{
		path:  filepath.Join(dir, "kernel.yaml"),
		store: filepath.Join(dir, "kernel.db"),
	}

	rewrite(t, config, listen)

	return config
}

// rewrite edits the file the way an administrator would.
func rewrite(t *testing.T, config Config, listen string) {
	t.Helper()

	if err := app.WriteConfig(config.path,
		app.NewConfig(config.store, "local", listen, 0)); err != nil {
		t.Fatalf("write config: %v", err)
	}
}

// A file that is not there is every default rather than a failure: a
// kernel started with no configuration should come up somewhere sensible
// and say where.
func TestAMissingFileIsEveryDefault(t *testing.T) {
	t.Parallel()

	config, err := app.ReadConfig(filepath.Join(t.TempDir(), "absent.yaml"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if config.Listen() != "127.0.0.1:7373" || config.Cache() == 0 || config.Name() == "" {
		t.Fatalf("defaults came back as %s", config)
	}
}

// A configuration written down and read back is the same configuration.
//
// Every field, non-zero, or it proves nothing: an earlier version of this
// stored the cache as one integer kind and read it as another, and the
// number came back as a default rather than as a mistake.
func TestAConfigSurvivesBeingWrittenDown(t *testing.T) {
	t.Parallel()

	at := filepath.Join(t.TempDir(), "kernel.yaml")
	original := app.NewConfig("/tmp/store.db", "somewhere", "0.0.0.0:9999", 16)

	if err := app.WriteConfig(at, original); err != nil {
		t.Fatalf("write: %v", err)
	}

	back, err := app.ReadConfig(at)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if !back.Eq(original) {
		t.Fatalf("%s came back as %s", original, back)
	}
}

// Everything a kernel publishes goes through Define, which is idempotent
// against the shape. So starting is safe to repeat.
func TestStartingIsSafeToRepeat(t *testing.T) {
	t.Parallel()

	config := write(t, "", free(t))
	ctx := context.Background()

	first := open(t, config)

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

	second := open(t, config)

	head, err = second.Kernel().Definition(ctx, auth.RoleKind)
	if err != nil {
		t.Fatalf("definition: %v", err)
	}

	if !head.Version().Eq(1) {
		t.Fatalf("starting again published version %s", head.Version())
	}
}

// The record is a REPORT and its spec is empty. Nothing about a kernel is
// anybody's to set from the API, because a configuration reachable only
// through the kernel could not be fixed when it was what broke it.
func TestAKernelReportsWhatItIsRunningWith(t *testing.T) {
	t.Parallel()

	at := free(t)
	running := open(t, write(t, "", at))

	id, err := app.KernelId("local")
	if err != nil {
		t.Fatalf("id: %v", err)
	}

	stored, err := running.Kernel().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if len(stored.Value.Spec().GetFields()) != 0 {
		t.Fatalf("the spec carries %v", stored.Value.Spec().ToGo())
	}

	reported := stored.Value.Status().ToGo()
	if reported["version"] != "test" || reported["listen"] != at {
		t.Fatalf("reported %v", reported)
	}

	if reported["os"] == "" || reported["arch"] == "" {
		t.Fatal("a controller is a binary; the platform has to be readable")
	}
}

// listening reports what is answering on an address.
func listening(at string) bool {
	conn, err := net.DialTimeout("tcp", at, 200*time.Millisecond)
	if err != nil {
		return false
	}

	_ = conn.Close()

	return true
}

// An address changed in the file moves the socket, and nothing else is
// restarted.
//
// This is what the two workers buy. The one that serves cannot notice the
// change — it is blocked inside Serve — and the one that notices cannot
// serve. Between them they hand over without either spawning anything, so
// a kernel that has rebound a hundred times has the goroutines it started
// with.
func TestChangingTheAddressMovesTheSocket(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := free(t)
	config := write(t, "", first)
	running := open(t, config)

	endpoint := running.Endpoint(discard())
	workers := start(ctx, endpoint, running)

	waitUntil(t, func() bool { return listening(first) }, "nothing answered on the first address")

	second := free(t)
	rewrite(t, config, second)

	waitUntil(t, func() bool { return listening(second) }, "nothing answered on the second address")
	waitUntil(t, func() bool { return !listening(first) }, "the first address was still answering")

	cancel()
	workers()
}

// An address that will not bind is waited on rather than guessed around:
// the file can be corrected, so inventing an address would only hide the
// mistake.
func TestAnAddressThatWillNotBindIsWaitedOn(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := write(t, "", "256.0.0.1:1")
	running := open(t, config)

	endpoint := running.Endpoint(discard())
	workers := start(ctx, endpoint, running)

	// It did not come up, and it did not die either.
	working := free(t)
	rewrite(t, config, working)

	waitUntil(t, func() bool { return listening(working) },
		"a corrected address never took effect")

	cancel()
	workers()
}

func discard() *slog.Logger {
	if os.Getenv("LOUD") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// free finds an address nothing is using.
func free(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	at := listener.Addr().String()

	if err := listener.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	return at
}

// start runs the three workers the way main does, and hands back a way to
// wait for all of them.
//
// A WaitGroup and not a channel of three: ranging over a channel reads
// until it CLOSES, so counting on it to yield exactly three is a hang
// waiting for a fourth that nobody sends. This test hung on that once.
func start(ctx context.Context, endpoint *app.Endpoint, running *app.App) func() {
	var workers sync.WaitGroup

	run := func(worker func()) {
		workers.Add(1)

		go func() {
			defer workers.Done()

			worker()
		}()
	}

	run(func() { _ = endpoint.Serve(ctx) })
	run(func() { _ = endpoint.Rebind(ctx) })
	run(func() { _ = running.Watch(ctx, discard()) })

	return workers.Wait
}

func waitUntil(t *testing.T, ready func() bool, complaint string) {
	t.Helper()

	deadline := time.After(patience)

	for !ready() {
		select {
		case <-deadline:
			t.Fatal(complaint)
		default:
		}
	}
}
