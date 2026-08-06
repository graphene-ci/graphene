package server_test

import (
	"context"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/gopherex/schemapb/go/schemapb"
	"github.com/gopherex/xlog"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/app/api"
	"github.com/graphene-ci/graphene/internal/app/health"
	"github.com/graphene-ci/graphene/internal/app/server"
	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store/kv/memory"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// The secret half of the token these tests carry.
const secret = "s3cret"

var processShape = path.MustNewTPath("kernel", "name")

// A subtree is listable, which is less obvious than it sounds.
//
// A prefix arrives naming only the positions it filled — a filled path
// carries nothing else — so it is not the same SHAPE as a whole path, and
// a grant compared shapes for equality. Every list of a subtree was
// refused, over a wire that otherwise worked.
func TestASubtreeIsListable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	at, _, _ := serving(t, ctx)
	client := grpcClient(t, at)

	for _, name := range []string{"one", "two"} {
		if _, err := client.Put(as(ctx), &graphenepbv1.PutRequest{
			Id:   at1("local", name),
			Spec: schemapb.MustStructFromGo(map[string]any{"bundle": "b"}),
		}); err != nil {
			t.Fatalf("put %s: %v", name, err)
		}
	}

	listing, err := client.List(as(ctx), &graphenepbv1.ListRequest{Prefix: under("local")})
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	found := 0

	for {
		answer, err := listing.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatalf("recv: %v", err)
		}

		if answer.GetRecord() == nil {
			t.Fatalf("a listed line carried no record: %v", answer)
		}

		found++
	}

	if found != 2 {
		t.Fatalf("listed %d of 2", found)
	}
}

// A watch does not hold the kernel open.
//
// A watch ends when its caller hangs up, and a caller has no reason to
// hang up because the kernel is stopping — so a graceful stop that waited
// for its handlers would wait for exactly the calls that never end, and a
// shutdown would only ever finish by timing out and killing the process.
// Every stream is tied to the run for that reason, and this is the test
// that says so.
func TestAWatchDoesNotHoldTheKernelOpen(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	at, _, wait := serving(t, ctx)

	// The CALLER'S context is not the run's, which is the whole point: a
	// client that has not hung up is what a graceful stop would otherwise
	// wait for. Cancelling both at once would prove nothing.
	watching, err := grpcClient(t, at).Watch(as(context.Background()),
		&graphenepbv1.WatchRequest{Prefix: under("local")})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	// A write, and the event for it: proof the handler is inside the
	// kernel and blocked on the next one rather than not started yet.
	if _, err := grpcClient(t, at).Put(as(ctx), &graphenepbv1.PutRequest{
		Id:   at1("local", "three"),
		Spec: schemapb.MustStructFromGo(map[string]any{"bundle": "b"}),
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	if _, err := watching.Recv(); err != nil {
		t.Fatalf("recv: %v", err)
	}

	cancel()

	stopped := make(chan struct{})

	go func() { defer close(stopped); wait() }()

	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("the kernel would not stop while a watch was open")
	}
}

// Health answers on the kernel's own port, which is the point of it.
//
// A check answering from a second listener is a check on the second
// listener: it can be up while this one is not, and then a supervisor is
// told a kernel is fine by a socket that is not the kernel.
func TestHealthAnswersOnTheServingPort(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	at, _, _ := serving(t, ctx)

	// No credential: a supervisor holds none, and refusing it one would
	// make "is it alive" a question only an authorised caller could ask.
	asked, err := hv1.NewHealthClient(dial(t, at)).Check(ctx, &hv1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("check: %v", err)
	}

	if asked.GetStatus() != hv1.HealthCheckResponse_SERVING {
		t.Fatalf("a working kernel answered %s", asked.GetStatus())
	}
}

// serving stands a kernel, a service and an endpoint up on a free port,
// and hands back where it is listening and a way to wait for it to stop.
func serving(t *testing.T, ctx context.Context) (string, kernel.Kernel, func()) {
	t.Helper()

	bytes := memory.New()
	t.Cleanup(func() { _ = bytes.Close() })

	k := kernel.New(bytes)

	if err := auth.Bootstrap(context.Background(), k); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	if _, err := k.Define(context.Background(), processDefinition()); err != nil {
		t.Fatalf("define: %v", err)
	}

	grant(t, k)

	at := free(t)
	checks := health.New(k, discard())
	endpoint := server.New(fixed(at),
		api.New(auth.New(k), k, discard()), nil, checks.Server(), discard())

	var workers sync.WaitGroup

	workers.Add(3)

	go func() { defer workers.Done(); _ = endpoint.Serve(ctx) }()
	go func() { defer workers.Done(); _ = endpoint.Rebind(ctx) }()
	go func() { defer workers.Done(); checks.Poll(ctx) }()

	t.Cleanup(workers.Wait)

	waitUntil(t, func() bool {
		conn, err := net.DialTimeout("tcp", at, 200*time.Millisecond)
		if err != nil {
			return false
		}

		_ = conn.Close()

		return true
	}, "nothing came up")

	return at, k, workers.Wait
}

// fixed is a configuration that never changes, which is every case but
// the rebind's — and the rebind is tested next to the application, where
// the configuration it follows lives.
type fixed string

func (f fixed) Listen() string           { return string(f) }
func (f fixed) Changed() <-chan struct{} { return make(chan struct{}) }

func processDefinition() def.Definition {
	return def.MustNew(
		kind.MustNew("Process"), processShape,
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-spec"}).
			Fields(schemapb.Str("bundle").Required()).MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-status"}).
			MustBuild()),
	)
}

// at1 names one resource; under names the subtree above it.
func at1(kernelName, name string) *graphenepbv1.Id {
	return &graphenepbv1.Id{
		Kind: "Process",
		Path: []*graphenepbv1.Segment{
			{Name: "kernel", Value: kernelName},
			{Name: "name", Value: name},
		},
	}
}

func under(kernelName string) *graphenepbv1.Id {
	return &graphenepbv1.Id{
		Kind: "Process",
		Path: []*graphenepbv1.Segment{{Name: "kernel", Value: kernelName}},
	}
}

// grant writes a role that may read and write Processes, and an identity
// holding it — through the kernel, which is how a first identity always
// arrives.
func grant(t *testing.T, k kernel.Kernel) {
	t.Helper()

	roleId, err := auth.RoleId("tester")
	if err != nil {
		t.Fatalf("role id: %v", err)
	}

	rules := []any{}
	for _, verb := range []string{"get", "list", "put", "watch"} {
		rules = append(rules, map[string]any{"verb": verb, "kind": "Process", "prefix": ""})
	}

	write(t, k, roleId, map[string]any{"grants": rules})

	who, err := auth.NewPrincipal("tester")
	if err != nil {
		t.Fatalf("principal: %v", err)
	}

	id, err := auth.IdentityId(who)
	if err != nil {
		t.Fatalf("identity id: %v", err)
	}

	write(t, k, id, map[string]any{
		"roles":   []any{"tester"},
		"digests": []any{auth.Digest(secret)},
	})
}

func write(t *testing.T, k kernel.Kernel, id resource.Id, spec map[string]any) {
	t.Helper()

	intent, err := resource.NewIntent(id, schemapb.MustStructFromGo(spec))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	if _, err := k.Put(context.Background(), intent, revision.Absent); err != nil {
		t.Fatalf("put %s: %v", id, err)
	}
}

// as puts the caller's token on a call.
func as(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer tester."+secret)
}

func grpcClient(t *testing.T, at string) graphenepbv1.KernelServiceClient {
	t.Helper()

	return graphenepbv1.NewKernelServiceClient(dial(t, at))
}

func dial(t *testing.T, at string) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient(at, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

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

func waitUntil(t *testing.T, ready func() bool, complaint string) {
	t.Helper()

	deadline := time.After(5 * time.Second)

	for !ready() {
		select {
		case <-deadline:
			t.Fatal(complaint)
		default:
		}
	}
}

func discard() *xlog.Logger {
	if os.Getenv("LOUD") != "" {
		return xlog.NewConsole(xlog.WithWriter(os.Stderr))
	}

	return xlog.New(xlog.NopCore{})
}
