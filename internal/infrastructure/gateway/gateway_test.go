package gateway_test

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/gopherex/schemapb/go/schemapb"
	"github.com/gopherex/xlog"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/blob"
	"github.com/graphene-ci/graphene/internal/convert"
	"github.com/graphene-ci/graphene/internal/infrastructure/blob/fs"
	"github.com/graphene-ci/graphene/internal/infrastructure/gateway"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/process"
	"github.com/graphene-ci/graphene/internal/store/kv/memory"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

const kernelName = "k1"

// A process holds no credential, so what it may do is decided entirely by
// the identity its record names. This opens a door for one, dials it the
// way a process would — nothing presented, nothing to present — and
// checks it gets exactly what that identity was granted and nothing else.
func TestADoorAnswersAsTheIdentityTheRecordNames(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	world := newWorld(ctx, t, t.TempDir())
	world.role(ctx, "process-reader", grant("get", process.Kind.String(), kernelName))
	world.identity(ctx, "watcher", "process-reader")
	world.process(ctx, "probe", "watcher")

	door, err := world.gateway.Open("probe", "watcher")
	if err != nil {
		t.Fatalf("open door: %v", err)
	}

	t.Cleanup(func() { _ = door.Close() })

	client := dial(t, door.Env()[process.EnvSocket])

	// What it was granted: its own record, which is what a controller
	// reads first.
	id, err := process.Id(kernelName, "probe")
	if err != nil {
		t.Fatalf("id: %v", err)
	}

	if _, err := client.Get(ctx, &graphenepbv1.GetRequest{Id: convert.IdToPb(id)}); err != nil {
		t.Fatalf("reading what it may read: %v", err)
	}

	// And what it was not. An Identity is the strongest kind there is,
	// and nothing about holding a door grants a look at one.
	elsewhere, err := auth.IdentityId("watcher")
	if err != nil {
		t.Fatalf("identity id: %v", err)
	}

	_, err = client.Get(ctx, &graphenepbv1.GetRequest{Id: convert.IdToPb(elsewhere)})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("reading what it may not: got %v, want PermissionDenied", err)
	}
}

// The grant is confined by a path prefix, and the prefix names a kernel.
// A process on one kernel therefore cannot read another kernel's
// processes — which is the whole reason the kind puts the kernel first.
func TestADoorIsConfinedToItsOwnKernel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	world := newWorld(ctx, t, t.TempDir())
	world.role(ctx, "process-reader", grant("get", process.Kind.String(), kernelName))
	world.identity(ctx, "watcher", "process-reader")

	door, err := world.gateway.Open("probe", "watcher")
	if err != nil {
		t.Fatalf("open door: %v", err)
	}

	t.Cleanup(func() { _ = door.Close() })

	client := dial(t, door.Env()[process.EnvSocket])

	elsewhere, err := process.Id("k2", "probe")
	if err != nil {
		t.Fatalf("id: %v", err)
	}

	_, err = client.Get(ctx, &graphenepbv1.GetRequest{Id: convert.IdToPb(elsewhere)})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("reading another kernel's process: got %v, want PermissionDenied", err)
	}
}

// A process that asked for no identity gets a working door onto a kernel
// that refuses everything: nobody holds no grants. The connection works
// and the answer is no, which is a better failure than a socket that is
// not there.
func TestADoorForNobodyRefusesEverything(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	world := newWorld(ctx, t, t.TempDir())

	door, err := world.gateway.Open("anonymous", "")
	if err != nil {
		t.Fatalf("open door: %v", err)
	}

	t.Cleanup(func() { _ = door.Close() })

	client := dial(t, door.Env()[process.EnvSocket])

	id, err := process.Id(kernelName, "anonymous")
	if err != nil {
		t.Fatalf("id: %v", err)
	}

	_, err = client.Get(ctx, &graphenepbv1.GetRequest{Id: convert.IdToPb(id)})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("a door for nobody: got %v, want Unauthenticated", err)
	}
}

// Closing a door takes the socket away. One that outlived its process
// would be a way in for whatever came next.
func TestClosingADoorTakesItAway(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	world := newWorld(ctx, t, t.TempDir())

	door, err := world.gateway.Open("brief", "")
	if err != nil {
		t.Fatalf("open door: %v", err)
	}

	path := door.Env()[process.EnvSocket]

	if err := door.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Closing twice is not an error: a caller unwinding does not have to
	// remember whether it already did.
	if err := door.Close(); err != nil {
		t.Fatalf("close twice: %v", err)
	}

	var dialer net.Dialer
	if _, err := dialer.DialContext(ctx, "unix", path); err == nil {
		t.Fatal("the socket is still there after the door was closed")
	}
}

// A path past what the operating system allows is refused in words. It
// surfaces as EINVAL otherwise — "invalid argument" — which says nothing
// about length, and a kernel under a long directory would refuse to run
// anything for a reason nobody could act on.
func TestALongPathSaysWhatIsWrong(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	deep := "/tmp/" + strings.Repeat("long-enough-to-matter/", 6)
	world := newWorld(ctx, t, deep)

	_, err := world.gateway.Open("probe", "")
	if err == nil || !strings.Contains(err.Error(), "longer than the operating system allows") {
		t.Fatalf("a path past the limit: %v", err)
	}
}

type world struct {
	kernel  kernel.Kernel
	gateway *gateway.Gateway
	t       *testing.T
}

func newWorld(ctx context.Context, t *testing.T, dir string) *world {
	t.Helper()

	bytes := memory.New()

	t.Cleanup(func() { _ = bytes.Close() })

	own := kernel.New(bytes)
	if err := auth.Bootstrap(ctx, own); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	if _, err := own.Define(ctx, process.Definition()); err != nil {
		t.Fatalf("define process: %v", err)
	}

	return &world{
		kernel:  own,
		gateway: gateway.Here(dir, auth.New(own), own, blobStore(t), xlog.New(xlog.NopCore{})),
		t:       t,
	}
}

// grant builds one stored permission as it lives in a role's spec.
func grant(verb, over, prefix string) any {
	return map[string]any{"verb": verb, "kind": over, "prefix": prefix}
}

func (w *world) role(ctx context.Context, name string, grants ...any) {
	w.t.Helper()

	id, err := auth.RoleId(name)
	if err != nil {
		w.t.Fatalf("role id: %v", err)
	}

	w.put(ctx, id, map[string]any{"grants": grants})
}

func (w *world) identity(ctx context.Context, name string, roles ...string) {
	w.t.Helper()

	who, err := auth.NewPrincipal(name)
	if err != nil {
		w.t.Fatalf("principal: %v", err)
	}

	id, err := auth.IdentityId(who)
	if err != nil {
		w.t.Fatalf("identity id: %v", err)
	}

	named := make([]any, 0, len(roles))
	for _, role := range roles {
		named = append(named, role)
	}

	// No digests: an identity a door answers as has no credential of its
	// own, and requiring one would mean inventing a secret nobody uses.
	w.put(ctx, id, map[string]any{"roles": named, "digests": []any{}})
}

func (w *world) process(ctx context.Context, name, identity string) {
	w.t.Helper()

	id, err := process.Id(kernelName, name)
	if err != nil {
		w.t.Fatalf("process id: %v", err)
	}

	bytes, err := blob.Issue()
	if err != nil {
		w.t.Fatalf("blob id: %v", err)
	}

	w.put(ctx, id, map[string]any{
		"blob":     bytes.String(),
		"format":   process.RawExec,
		"identity": identity,
	})
}

func (w *world) put(ctx context.Context, id resource.Id, spec map[string]any) {
	w.t.Helper()

	intent, err := resource.NewIntent(id, schemapb.MustStructFromGo(spec))
	if err != nil {
		w.t.Fatalf("intent: %v", err)
	}

	if _, err := w.kernel.Put(ctx, intent, revision.Absent); err != nil {
		w.t.Fatalf("put %s: %v", id, err)
	}
}

// dial reaches a door the way a spawned process does: a socket path out
// of the environment, and nothing else.
func dial(t *testing.T, path string) graphenepbv1.KernelServiceClient {
	t.Helper()

	conn, err := grpc.NewClient("passthrough:///door",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			var dialer net.Dialer

			return dialer.DialContext(ctx, "unix", path)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	return graphenepbv1.NewKernelServiceClient(conn)
}

// blobStore is a byte store for the door to answer for. On disk rather than
// in memory because there is no in-memory one: a store of blobs is a
// directory, and a temporary directory is the same thing for a test.
func blobStore(t *testing.T) blob.Store {
	t.Helper()

	store, err := fs.Open(t.TempDir())
	if err != nil {
		t.Fatalf("blobs: %v", err)
	}

	t.Cleanup(func() { _ = store.Close() })

	return store
}
