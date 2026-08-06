package upstream_test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/gopherex/schemapb/go/schemapb"
	"github.com/gopherex/xlog"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/app/api"
	"github.com/graphene-ci/graphene/internal/app/config"
	"github.com/graphene-ci/graphene/internal/app/report"
	"github.com/graphene-ci/graphene/internal/app/upstream"
	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/link"
	"github.com/graphene-ci/graphene/internal/store/kv/memory"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

const secret = "s3cret"

var processShape = path.MustNewTPath("kernel", "name")

// A CALL THROUGH A SUBORDINATE IS THE CALLER'S CALL.
//
// The whole meaning of the mode is here: what a person writes through the
// kernel in front lands in the kernel behind, written by THEM. A proxy
// that re-signed each call with its own credential would put one name on
// everything, and the kernel above could not tell an operator from a
// controller.
func TestAWriteThroughASubordinateLandsAbove(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	above, at := standing(t)
	through := subordinate(t, at)

	if _, err := through.Put(as(ctx, "tester"), &graphenepbv1.PutRequest{
		Id:   at1("local", "one"),
		Spec: schemapb.MustStructFromGo(map[string]any{"bundle": "b"}),
	}); err != nil {
		t.Fatalf("put through the subordinate: %v", err)
	}

	// Read it out of the kernel above DIRECTLY, not back through the
	// proxy: the proxy answering its own write would prove only that it
	// remembered it.
	stored, err := above.Get(ctx, idOf(t, "local", "one"))
	if err != nil {
		t.Fatalf("the write never reached the kernel above: %v", err)
	}

	if stored.Value.Spec().ToGo()["bundle"] != "b" {
		t.Fatalf("what landed above was %v", stored.Value.Spec().ToGo())
	}
}

// The subordinate's own credential is NOT the caller's.
//
// This is the test that would catch a proxy quietly substituting its own
// token: the kernel's identity may write its own record and nothing else,
// so a Process written under it is refused. If the refusal ever stops
// arriving, every caller has silently become the kernel.
func TestASubordinateDoesNotLendItsCredential(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, at := standing(t)
	through := subordinate(t, at)

	_, err := through.Put(as(ctx, "edge"), &graphenepbv1.PutRequest{
		Id:   at1("local", "two"),
		Spec: schemapb.MustStructFromGo(map[string]any{"bundle": "b"}),
	})

	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("the kernel's own credential wrote a Process: %v", err)
	}

	// And a caller with none is refused as one with none, rather than
	// silently promoted to the kernel it is talking through.
	if _, err := through.Get(ctx, &graphenepbv1.GetRequest{
		Id: at1("local", "one"),
	}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("an anonymous call through the proxy answered %v", err)
	}
}

// A stream is relayed the same way a call is, credential and all.
func TestAStreamIsRelayed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	above, at := standing(t)
	through := subordinate(t, at)

	for _, name := range []string{"one", "two"} {
		write(t, above, idOf(t, "local", name), map[string]any{"bundle": "b"})
	}

	listing, err := through.List(as(ctx, "tester"), &graphenepbv1.ListRequest{
		Prefix: under("local"),
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	found := 0

	for {
		answer, err := listing.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			t.Fatalf("recv: %v", err)
		}

		if answer.GetRecord() == nil {
			t.Fatalf("a relayed line carried no record: %v", answer)
		}

		found++
	}

	if found != 2 {
		t.Fatalf("the relay delivered %d of 2", found)
	}
}

// A subordinate records ITSELF above, under its own name and its own
// credential — which is what makes a fleet one list however it is
// arranged.
func TestASubordinateRecordsItselfAbove(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	above, at := standing(t)

	forwarding, err := config.NewUpstream("edge", "127.0.0.1:0", at, "edge."+secret, t.TempDir(), pinOf(t, at))
	if err != nil {
		t.Fatalf("config: %v", err)
	}

	to, _ := forwarding.Upstream()

	connected, err := upstream.Open(to)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	t.Cleanup(func() { _ = connected.Close() })

	if err := report.Write(ctx, connected.Recording(), forwarding, "test"); err != nil {
		t.Fatalf("record itself: %v", err)
	}

	id, err := report.Id("edge")
	if err != nil {
		t.Fatalf("id: %v", err)
	}

	stored, err := above.Get(ctx, id)
	if err != nil {
		t.Fatalf("the subordinate never appeared above: %v", err)
	}

	reported := stored.Value.Status().ToGo()
	if reported["upstream"] != at {
		t.Fatalf("it recorded upstream %v, serving at %s", reported["upstream"], at)
	}

	if reported["store"] != nil {
		t.Fatalf("a kernel with no store recorded one: %v", reported["store"])
	}
}

// standing puts a kernel with a store behind a socket, and hands back
// both.
func standing(t *testing.T) (kernel.Kernel, string) {
	t.Helper()

	ctx := context.Background()

	bytes := memory.New()

	t.Cleanup(func() { _ = bytes.Close() })

	k := kernel.New(bytes)

	if err := auth.Bootstrap(ctx, k); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	if err := report.Publish(ctx, k); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if _, err := k.Define(ctx, processDefinition()); err != nil {
		t.Fatalf("define: %v", err)
	}

	// One identity that may work with Processes, and one that is only
	// allowed to be a kernel: the difference between them is what the
	// proxy is tested against.
	grant(t, k, "tester", rules("Process", "get", "list", "put", "watch")...)
	grant(t, k, "edge", rules("Kernel", "get", "put", "report")...)

	return k, serve(t, api.New(auth.New(k), k, discard()))
}

// subordinate puts a kernel with NO store in front of that socket, and
// hands back a client to it.
func subordinate(t *testing.T, at string) graphenepbv1.KernelServiceClient {
	t.Helper()

	forwarding, err := config.NewUpstream("edge", "127.0.0.1:0", at, "edge."+secret, t.TempDir(), pinOf(t, at))
	if err != nil {
		t.Fatalf("config: %v", err)
	}

	to, _ := forwarding.Upstream()

	connected, err := upstream.Open(to)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	t.Cleanup(func() { _ = connected.Close() })

	return graphenepbv1.NewKernelServiceClient(dial(t, serve(t, connected.Serving())))
}

// serve stands one service on a free port and hands back the address.
func serve(t *testing.T, service graphenepbv1.KernelServiceServer) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := grpc.NewServer(grpc.Creds(keyForTests(t).Serving()))
	graphenepbv1.RegisterKernelServiceServer(server, service)

	go func() { _ = server.Serve(listener) }()

	t.Cleanup(server.Stop)

	return listener.Addr().String()
}

func dial(t *testing.T, at string) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient(at, reaching(t))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func as(ctx context.Context, who string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+who+"."+secret)
}

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

func idOf(t *testing.T, kernelName, name string) resource.Id {
	t.Helper()

	at, err := processShape.New(kernelName, name)
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	return resource.NewId(kind.MustNew("Process"), at)
}

func processDefinition() def.Definition {
	return def.MustNew(
		kind.MustNew("Process"), processShape,
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-spec"}).
			Fields(schemapb.Str("bundle").Required()).MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-status"}).
			MustBuild()),
	)
}

func rules(named string, verbs ...string) []any {
	stated := make([]any, 0, len(verbs))

	for _, verb := range verbs {
		stated = append(stated, map[string]any{"verb": verb, "kind": named, "prefix": ""})
	}

	return stated
}

func grant(t *testing.T, k kernel.Kernel, name string, granted ...any) {
	t.Helper()

	roleId, err := auth.RoleId(name)
	if err != nil {
		t.Fatalf("role id: %v", err)
	}

	write(t, k, roleId, map[string]any{"grants": granted})

	who, err := auth.NewPrincipal(name)
	if err != nil {
		t.Fatalf("principal: %v", err)
	}

	id, err := auth.IdentityId(who)
	if err != nil {
		t.Fatalf("identity id: %v", err)
	}

	write(t, k, id, map[string]any{
		"roles":   []any{name},
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

func discard() *xlog.Logger {
	if os.Getenv("LOUD") != "" {
		return xlog.NewConsole(xlog.WithWriter(os.Stderr))
	}

	return xlog.New(xlog.NopCore{})
}

// One key for the whole test binary. These tests are about forwarding
// and not about pinning — link_test.go is about pinning — so a single
// identity keeps them from standing up a certificate authority's worth of
// scaffolding to say "the far side is the far side".
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

// pinOf is what a client is told to expect.
func pinOf(t *testing.T, _ string) string {
	t.Helper()

	return keyForTests(t).Pin().String()
}

// reaching is how every test in this package dials.
func reaching(t *testing.T) grpc.DialOption {
	t.Helper()

	creds, err := link.Reaching(keyForTests(t).Pin())
	if err != nil {
		t.Fatalf("reaching: %v", err)
	}

	return grpc.WithTransportCredentials(creds)
}
