package app_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	hv1 "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/gopherex/xlog"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/app"
	"github.com/graphene-ci/graphene/internal/app/config"
	"github.com/graphene-ci/graphene/internal/app/report"
	"github.com/graphene-ci/graphene/internal/app/server"
	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/link"
)

// patience bounds a wait that is expected to end at once. Nothing here
// sleeps for it: the config is applied by the loop, and this only stops a
// bug from hanging the suite.
const patience = 5 * time.Second

// open starts a kernel configured by a file in a fresh directory.
func open(t *testing.T, on written) *app.App {
	t.Helper()

	opened, err := app.Open(context.Background(), app.Bootstrap{
		Config:  on.path,
		Version: "test",
	}, discard())
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	t.Cleanup(func() { _ = opened.Close() })

	return opened
}

// own is the store a kernel keeps, or a failed test: these all configure
// a local kernel, so one with no store of its own is a bug in the setup
// rather than a case to handle.
func own(t *testing.T, of *app.App) kernel.Kernel {
	t.Helper()

	kept, keeps := of.Own()
	if !keeps {
		t.Fatal("the kernel kept no store of its own")
	}

	return kept
}

// written is a configuration file on disk and where it is.
type written struct {
	path  string
	store string
}

// write puts a configuration on disk and hands back where it is.
func write(t *testing.T, listen string) written {
	t.Helper()

	dir := t.TempDir()
	on := written{
		path:  filepath.Join(dir, "kernel.yaml"),
		store: filepath.Join(dir, "kernel.db"),
	}

	rewrite(t, on, listen)

	return on
}

// rewrite edits the file the way an administrator would.
func rewrite(t *testing.T, on written, listen string) {
	t.Helper()

	if err := config.Write(on.path, config.NewLocal("local", listen, on.store, 0, "")); err != nil {
		t.Fatalf("write config: %v", err)
	}
}

// A file that is not there is every default rather than a failure: a
// kernel started with no configuration should come up somewhere sensible
// and say where.
func TestAMissingFileIsEveryDefault(t *testing.T) {
	t.Parallel()

	config, err := config.Read(filepath.Join(t.TempDir(), "absent.yaml"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	local, keeps := config.Local()
	if !keeps {
		t.Fatalf("a file that is not there came back as %s", config)
	}

	if config.Listen() != "127.0.0.1:7373" || local.Cache() == 0 || config.Name() == "" {
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
	original := config.NewLocal("somewhere", "0.0.0.0:9999", "/tmp/store.db", 16, "root.s3cret")

	if err := config.Write(at, original); err != nil {
		t.Fatalf("write: %v", err)
	}

	back, err := config.Read(at)
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

	on := write(t, free(t))
	ctx := context.Background()

	first := open(t, on)

	head, err := own(t, first).Definition(ctx, auth.RoleKind)
	if err != nil {
		t.Fatalf("definition: %v", err)
	}

	if !head.Version().Eq(1) {
		t.Fatalf("first start published version %s", head.Version())
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	second := open(t, on)

	head, err = own(t, second).Definition(ctx, auth.RoleKind)
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
	running := open(t, write(t, at))

	id, err := report.Id("local")
	if err != nil {
		t.Fatalf("id: %v", err)
	}

	stored, err := own(t, running).Get(context.Background(), id)
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
	on := write(t, first)
	running := open(t, on)

	endpoint := server.New(running, keyForTests(t),
		graphenepbv1.UnimplementedKernelServiceServer{}, nil,
		hv1.UnimplementedHealthServer{}, discard())
	workers := start(ctx, endpoint, running)

	waitUntil(t, func() bool { return listening(first) }, "nothing answered on the first address")

	second := free(t)
	rewrite(t, on, second)

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

	on := write(t, "256.0.0.1:1")
	running := open(t, on)

	endpoint := server.New(running, keyForTests(t),
		graphenepbv1.UnimplementedKernelServiceServer{}, nil,
		hv1.UnimplementedHealthServer{}, discard())
	workers := start(ctx, endpoint, running)

	// It did not come up, and it did not die either.
	working := free(t)
	rewrite(t, on, working)

	waitUntil(t, func() bool { return listening(working) },
		"a corrected address never took effect")

	cancel()
	workers()
}

func discard() *xlog.Logger {
	if os.Getenv("LOUD") != "" {
		return xlog.NewConsole(xlog.WithWriter(os.Stderr))
	}

	return xlog.New(xlog.NopCore{})
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
func start(ctx context.Context, endpoint *server.Endpoint, running *app.App) func() {
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
	run(func() { _ = app.Watch(ctx, running, discard()) })

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

// A kernel makes its first caller once, and writes the credential into
// its own file.
//
// ONE FILE PER KERNEL: the credential goes where the store it belongs to
// is named. And once — a kernel restarting is not a reason to mint a new
// one and quietly invalidate what an operator saved.
func TestTheFirstCallerIsWrittenIntoTheFile(t *testing.T) {
	t.Parallel()

	on := write(t, free(t))

	first := open(t, on)

	local, keeps := first.Config().Local()
	if !keeps {
		t.Fatal("a local kernel came back keeping no store")
	}

	token := local.Token()
	if token == "" {
		t.Fatal("a fresh store came up with no first caller")
	}

	// The file says it too, which is the half that survives a restart.
	back, err := config.Read(on.path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	written, _ := back.Local()
	if written.Token() != token {
		t.Fatalf("the file says %q, the kernel says %q", written.Token(), token)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	second := open(t, on)

	again, _ := second.Config().Local()
	if again.Token() != token {
		t.Fatalf("starting again minted %q over %q", again.Token(), token)
	}
}

// A credential written into the file BEFORE the first start is the one
// the store gets, which is how a kernel is installed with something
// somebody already knows.
func TestAGivenCredentialIsTheOneMade(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	on := written{
		path:  filepath.Join(dir, "kernel.yaml"),
		store: filepath.Join(dir, "kernel.db"),
	}

	given := "root.chosen-in-advance"

	if err := config.Write(on.path,
		config.NewLocal("local", free(t), on.store, 0, given)); err != nil {
		t.Fatalf("write config: %v", err)
	}

	running := open(t, on)

	local, _ := running.Config().Local()
	if local.Token() != given {
		t.Fatalf("the kernel replaced the credential with %q", local.Token())
	}

	// And it works: the identity in the store answers to it.
	name, secret, err := auth.Split(given)
	if err != nil {
		t.Fatalf("split: %v", err)
	}

	who, err := auth.NewPrincipal(name)
	if err != nil {
		t.Fatalf("principal: %v", err)
	}

	id, err := auth.IdentityId(who)
	if err != nil {
		t.Fatalf("identity id: %v", err)
	}

	stored, err := own(t, running).Get(context.Background(), id)
	if err != nil {
		t.Fatalf("the given identity was never made: %v", err)
	}

	digests, _ := stored.Value.Spec().Field(auth.DigestsField).AsList()
	if len(digests) != 1 || digests[0].GetStringValue() != auth.Digest(secret) {
		t.Fatalf("the identity does not know the given secret")
	}
}

// keyForTests is one key for the whole test binary. These tests are about
// addresses moving and configuration being reread, not about which kernel
// answered, so one identity is the whole of what they need.
var testKey = sync.OnceValues(func() (link.Identity, error) {
	dir, err := os.MkdirTemp("", "graphene-link-")
	if err != nil {
		return link.Identity{}, err
	}

	return link.Open(dir)
})

func keyForTests(t *testing.T) link.Identity {
	t.Helper()

	identity, err := testKey()
	if err != nil {
		t.Fatalf("link key: %v", err)
	}

	return identity
}
