package api_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/gopherex/schemapb/go/schemapb"
	"github.com/gopherex/xlog"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/app/api"
	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/convert"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store/kv/memory"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// The secret half of the tokens these tests carry. The name half is
// whichever identity is being spoken for.
const secret = "s3cret"

var processShape = path.MustNewTPath("kernel", "name")

// processKind is the kind these tests write instances of.
func processKind() def.Definition {
	return def.MustNew(
		kind.MustNew("Process"), processShape,
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-spec"}).
			Fields(schemapb.Str("bundle").Required()).MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-status"}).
			MustBuild()),
	)
}

// dial stands the whole stack up and hands back a client.
func dial(t *testing.T) (graphenepbv1.KernelServiceClient, kernel.Kernel) {
	t.Helper()

	ctx := context.Background()

	bytes := memory.New()
	t.Cleanup(func() { _ = bytes.Close() })

	k := kernel.New(bytes)

	if err := auth.Bootstrap(ctx, k); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := grpc.NewServer()
	graphenepbv1.RegisterKernelServiceServer(server, api.New(auth.New(k), k, discard(t)))

	go func() { _ = server.Serve(listener) }()

	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	return graphenepbv1.NewKernelServiceClient(conn), k
}

// as puts a caller's token on the call.
func as(ctx context.Context, name string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+name+"."+secret)
}

// grant writes a role and an identity that holds it, through the kernel
// rather than the wire — which is how a first identity always arrives.
func grant(t *testing.T, k kernel.Kernel, name string, grants ...any) {
	t.Helper()

	roleId, err := auth.RoleId(name)
	if err != nil {
		t.Fatalf("role id: %v", err)
	}

	write(t, k, roleId, map[string]any{"grants": grants})

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

func rule(verb, named, prefix string) any {
	return map[string]any{"verb": verb, "kind": named, "prefix": prefix}
}

// The whole stack, over a socket: a kind declared, an instance written,
// and the same instance read back with the revision it landed at.
func TestAKindAndAnInstanceOverTheWire(t *testing.T) {
	t.Parallel()

	client, k := dial(t)
	ctx := context.Background()

	grant(t, k, "admin",
		rule("define", "Process", ""),
		rule("put", "Process", ""),
		rule("get", "Process", ""),
		rule("list", "Process", ""),
	)

	called := as(ctx, "admin")

	process := def.MustNew(
		kind.MustNew("Process"), processShape,
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-spec"}).
			Fields(schemapb.Str("bundle").Required()).MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-status"}).
			MustBuild()),
	)

	published, err := def.Publish(process, 1)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	defined, err := client.Define(called, &graphenepbv1.DefineRequest{
		Definition: convert.DefinitionToPb(published),
	})
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	if defined.GetDefinition().GetVersion() != 1 {
		t.Fatalf("defined at version %d", defined.GetDefinition().GetVersion())
	}

	at, err := processShape.New("local", "web")
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	id := convert.IdToPb(resource.NewId(kind.MustNew("Process"), at))

	written, err := client.Put(called, &graphenepbv1.PutRequest{
		Id:     id,
		Spec:   schemapb.MustStructFromGo(map[string]any{"bundle": "b1"}),
		Expect: 0,
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	read, err := client.Get(called, &graphenepbv1.GetRequest{Id: id})
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if read.GetRecord().GetRevision() != written.GetRevision() {
		t.Fatalf("read at %d, written at %d",
			read.GetRecord().GetRevision(), written.GetRevision())
	}

	// The generation the kernel counted came back with it, which is the
	// difference between reading the store and reading the kernel.
	if read.GetRecord().GetResource().GetGeneration() != 1 {
		t.Fatalf("generation came back as %d", read.GetRecord().GetResource().GetGeneration())
	}
}

// A caller with no credential, a malformed one and a wrong one are all
// told the same thing, and it is not "internal error".
func TestCredentialsAreAnsweredWithUnauthenticated(t *testing.T) {
	t.Parallel()

	client, k := dial(t)
	ctx := context.Background()

	grant(t, k, "admin", rule("get", "Process", ""))

	at, err := processShape.New("local", "web")
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	id := convert.IdToPb(resource.NewId(kind.MustNew("Process"), at))

	for name, called := range map[string]context.Context{
		"none":      ctx,
		"malformed": metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer nonsense"),
		"wrong":     metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer admin.guess"),
		"unknown":   as(ctx, "stranger"),
	} {
		_, err := client.Get(called, &graphenepbv1.GetRequest{Id: id})
		if code := status.Code(err); code != codes.Unauthenticated {
			t.Fatalf("%s: want Unauthenticated, got %s (%v)", name, code, err)
		}
	}
}

// A caller who is who they say and may not do what they asked hears that,
// and hears nothing about whether the thing exists.
func TestAGrantIsEnforcedOverTheWire(t *testing.T) {
	t.Parallel()

	client, k := dial(t)
	ctx := context.Background()

	// The kind exists first, which is what makes a confined grant mean
	// anything: a prefix over a kind with no definition has nothing to
	// confine.
	if _, err := k.Define(ctx, processKind()); err != nil {
		t.Fatalf("define: %v", err)
	}

	grant(t, k, "reader", rule("get", "Process", "/local"))

	called := as(ctx, "reader")

	elsewhere, err := processShape.New("remote", "web")
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	_, err = client.Get(called, &graphenepbv1.GetRequest{
		Id: convert.IdToPb(resource.NewId(kind.MustNew("Process"), elsewhere)),
	})

	if code := status.Code(err); code != codes.PermissionDenied {
		t.Fatalf("want PermissionDenied, got %s (%v)", code, err)
	}

	// And what it may reach it reaches, finding nothing there rather than
	// being refused.
	here, err := processShape.New("local", "web")
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	_, err = client.Get(called, &graphenepbv1.GetRequest{
		Id: convert.IdToPb(resource.NewId(kind.MustNew("Process"), here)),
	})

	if code := status.Code(err); code != codes.NotFound {
		t.Fatalf("want NotFound, got %s (%v)", code, err)
	}
}

// A stale write is Aborted and not Internal: it is the one failure that
// is normal, and a caller re-reads and decides again.
func TestAStaleWriteIsAborted(t *testing.T) {
	t.Parallel()

	client, k := dial(t)
	ctx := context.Background()

	grant(t, k, "writer",
		rule("define", "Process", ""),
		rule("put", "Process", ""),
	)

	called := as(ctx, "writer")

	process := def.MustNew(
		kind.MustNew("Process"), processShape,
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-spec"}).
			Fields(schemapb.Str("bundle")).MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-status"}).
			MustBuild()),
	)

	published, err := def.Publish(process, 1)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	if _, err := client.Define(called, &graphenepbv1.DefineRequest{
		Definition: convert.DefinitionToPb(published),
	}); err != nil {
		t.Fatalf("define: %v", err)
	}

	at, err := processShape.New("local", "web")
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	id := convert.IdToPb(resource.NewId(kind.MustNew("Process"), at))
	spec := schemapb.MustStructFromGo(map[string]any{"bundle": "b1"})

	if _, err := client.Put(called, &graphenepbv1.PutRequest{Id: id, Spec: spec}); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Creating over something that is already there.
	_, err = client.Put(called, &graphenepbv1.PutRequest{Id: id, Spec: spec})
	if code := status.Code(err); code != codes.Aborted {
		t.Fatalf("want Aborted, got %s (%v)", code, err)
	}

	// And a kind nobody defined is a precondition, not a missing record.
	_, err = client.Put(called, &graphenepbv1.PutRequest{
		Id:   convert.IdToPb(resource.NewId(kind.MustNew("Volume"), at)),
		Spec: spec,
	})

	if code := status.Code(err); code != codes.PermissionDenied {
		t.Fatalf("an unheld kind: want PermissionDenied, got %s (%v)", code, err)
	}
}

// discard is where a server's log goes in a test: nowhere. What is being
// checked is what a CALLER is told, and the log is deliberately the other
// half of that — the half a caller never sees.
func discard(t *testing.T) *xlog.Logger {
	t.Helper()

	return xlog.New(xlog.NopCore{})
}
