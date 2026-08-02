package service_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/registry"
	"github.com/graphene-ci/graphene/internal/core/service"
	"github.com/graphene-ci/graphene/internal/infrastructure/auth/static"
	"github.com/graphene-ci/graphene/internal/infrastructure/server"
	"github.com/graphene-ci/graphene/internal/infrastructure/store/bbolt"
)

const (
	adminToken  = "admin-token"
	kernelToken = "kernel-k1-token"
)

// newEnv starts a fully authenticated server (admin + kernel k1 tokens)
// and returns a client factory keyed by token.
func newEnv(t *testing.T) func(token string) graphenepbv1.ResourceServiceClient {
	t.Helper()

	st, err := bbolt.Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	t.Cleanup(func() { _ = st.Close() })

	source := static.New(
		static.Entry{Token: adminToken, Credentials: static.Admin("root")},
		static.Entry{Token: kernelToken, Credentials: static.Kernel("k1")},
	)

	srv := grpc.NewServer(
		grpc.UnaryInterceptor(server.UnaryAuth(source)),
		grpc.StreamInterceptor(server.StreamAuth(source)),
	)
	graphenepbv1.RegisterResourceServiceServer(srv, service.NewResources(st, registry.New(st)))

	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()

	t.Cleanup(srv.Stop)

	return func(token string) graphenepbv1.ResourceServiceClient {
		conn, err := grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(bearerUnary(token)),
			grpc.WithStreamInterceptor(bearerStream(token)),
		)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}

		t.Cleanup(func() { _ = conn.Close() })

		return graphenepbv1.NewResourceServiceClient(conn)
	}
}

func bearerUnary(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		return invoker(withBearer(ctx, token), method, req, reply, cc, opts...)
	}
}

func bearerStream(token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
		method string, streamer grpc.Streamer, opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		return streamer(withBearer(ctx, token), desc, cc, method, opts...)
	}
}

func withBearer(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func newClient(t *testing.T) graphenepbv1.ResourceServiceClient {
	t.Helper()

	return newEnv(t)(adminToken)
}

func defineVM(t *testing.T, c graphenepbv1.ResourceServiceClient) {
	t.Helper()

	spec := schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "vm-spec"}).
		Fields(
			schemapb.Str("type").Required(),
			schemapb.Str("placement"),
		).
		MustBuild()
	stat := schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "vm-status"}).
		Fields(schemapb.Str("phase")).
		MustBuild()

	_, err := c.Define(context.Background(), &graphenepbv1.DefineRequest{
		Definition: &graphenepbv1.ResourceDefinition{
			Kind:         "aws.vm",
			PathSegments: []string{"tenant", "env", "workflow", "name"},
			SpecSchema:   spec,
			StatusSchema: stat,
		},
	})
	if err != nil {
		t.Fatalf("define: %v", err)
	}
}

func vmResource(name string, fields map[string]any) *graphenepbv1.Resource {
	return &graphenepbv1.Resource{
		Key: &graphenepbv1.Key{
			Kind: "aws.vm",
			Path: []string{"acme", "prod", "deploy", name},
		},
		Spec: schemapb.MustStructFromGo(fields),
	}
}

func TestPutGetRoundtrip(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	ctx := context.Background()

	defineVM(t, c)

	put, err := c.Put(ctx, &graphenepbv1.PutRequest{
		Resource: vmResource("app", map[string]any{"type": "t3.medium"}),
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	if put.GetRevision() == 0 {
		t.Fatal("put: zero revision")
	}

	got, err := c.Get(ctx, &graphenepbv1.GetRequest{
		Key: &graphenepbv1.Key{Kind: "aws.vm", Path: []string{"acme", "prod", "deploy", "app"}},
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	res := got.GetResource()
	if res.GetRevision() != put.GetRevision() || res.GetCreatedRevision() != put.GetRevision() {
		t.Fatalf("revisions: rev=%d created=%d want %d", res.GetRevision(), res.GetCreatedRevision(), put.GetRevision())
	}

	if res.GetDefinitionVersion() != 1 {
		t.Fatalf("definition_version: got %d want 1 (pinned latest)", res.GetDefinitionVersion())
	}

	if res.GetSpec().ToGo()["type"] != "t3.medium" {
		t.Fatalf("spec lost: %v", res.GetSpec().ToGo())
	}

	// CAS: create again must abort.
	_, err = c.Put(ctx, &graphenepbv1.PutRequest{
		Resource: vmResource("app", map[string]any{"type": "t3.large"}),
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("second create: want Aborted, got %v", err)
	}
}

func TestPutValidation(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	ctx := context.Background()

	defineVM(t, c)

	// Missing required spec field.
	_, err := c.Put(ctx, &graphenepbv1.PutRequest{
		Resource: vmResource("app", map[string]any{"placement": "k1"}),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad spec: want InvalidArgument, got %v", err)
	}

	// Unknown kind.
	bad := vmResource("app", map[string]any{"type": "x"})

	bad.Key.Kind = "Nope"
	if _, err := c.Put(ctx, &graphenepbv1.PutRequest{Resource: bad}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown kind: want NotFound, got %v", err)
	}

	// The reserved kind is not writable via Put.
	bad = vmResource("app", map[string]any{"type": "x"})

	bad.Key.Kind = registry.KindKind
	if _, err := c.Put(ctx, &graphenepbv1.PutRequest{Resource: bad}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("kind Kind: want InvalidArgument, got %v", err)
	}
}

func TestListWithSelector(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	ctx := context.Background()

	defineVM(t, c)

	for name, placement := range map[string]string{"a": "k1", "b": "k2", "c": "k1"} {
		_, err := c.Put(ctx, &graphenepbv1.PutRequest{
			Resource: vmResource(name, map[string]any{"type": "t", "placement": placement}),
		})
		if err != nil {
			t.Fatalf("put %s: %v", name, err)
		}
	}

	list, err := c.List(ctx, &graphenepbv1.ListRequest{
		Kind:       "aws.vm",
		PathPrefix: []string{"acme", "prod"},
		Selector:   []*graphenepbv1.FieldMatch{{Path: "spec.placement", Value: "k1"}},
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(list.GetResources()) != 2 {
		t.Fatalf("selector list: got %d, want 2", len(list.GetResources()))
	}
}

// recvData returns the next non-sync watch event: every watch opens with
// the catch-up marker.
func recvData(t *testing.T, stream graphenepbv1.ResourceService_WatchClient) *graphenepbv1.WatchEvent {
	t.Helper()

	for {
		event, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}

		if event.GetType() != graphenepbv1.EventType_EVENT_TYPE_SYNC {
			return event
		}
	}
}

func TestWatchWithSelector(t *testing.T) {
	t.Parallel()

	c := newClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	defineVM(t, c)

	watcher, err := c.Watch(ctx, &graphenepbv1.WatchRequest{
		Kind:     "aws.vm",
		Selector: []*graphenepbv1.FieldMatch{{Path: "spec.placement", Value: "k1"}},
	})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	// Non-matching then matching.
	if _, err := c.Put(ctx, &graphenepbv1.PutRequest{
		Resource: vmResource("other", map[string]any{"type": "t", "placement": "k2"}),
	}); err != nil {
		t.Fatal(err)
	}

	put, err := c.Put(ctx, &graphenepbv1.PutRequest{
		Resource: vmResource("mine", map[string]any{"type": "t", "placement": "k1"}),
	})
	if err != nil {
		t.Fatal(err)
	}

	event := recvData(t, watcher)

	if event.GetType() != graphenepbv1.EventType_EVENT_TYPE_PUT ||
		event.GetStoreRevision() != put.GetStoreRevision() {
		t.Fatalf("watch: got %v rev=%d, want put rev=%d", event.GetType(), event.GetStoreRevision(), put.GetStoreRevision())
	}

	if got := event.GetResource().GetKey().GetPath()[3]; got != "mine" {
		t.Fatalf("watch: got %q, want mine (k2 event must be filtered)", got)
	}
}

func TestFinalizerFlow(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	ctx := context.Background()

	defineVM(t, c)

	res := vmResource("app", map[string]any{"type": "t"})
	res.Finalizers = []string{"teardown"}

	put, err := c.Put(ctx, &graphenepbv1.PutRequest{Resource: res})
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	key := &graphenepbv1.Key{Kind: "aws.vm", Path: []string{"acme", "prod", "deploy", "app"}}

	// Delete with finalizers → deleting mark, record stays.
	if _, err := c.Delete(ctx, &graphenepbv1.DeleteRequest{Key: key, ExpectedRevision: put.GetRevision()}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	got, err := c.Get(ctx, &graphenepbv1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}

	if !got.GetResource().GetDeleting() {
		t.Fatal("resource not marked deleting")
	}

	// Second delete is a no-op while in progress.
	if _, err := c.Delete(ctx, &graphenepbv1.DeleteRequest{Key: key, ExpectedRevision: got.GetResource().GetRevision()}); err != nil {
		t.Fatalf("repeat delete: %v", err)
	}

	// Controller finished teardown: Put without the finalizer commits the
	// removal.
	final := got.GetResource()

	final.Finalizers = nil
	if _, err := c.Put(ctx, &graphenepbv1.PutRequest{
		Resource:         final,
		ExpectedRevision: final.GetRevision(),
	}); err != nil {
		t.Fatalf("finalize put: %v", err)
	}

	if _, err := c.Get(ctx, &graphenepbv1.GetRequest{Key: key}); status.Code(err) != codes.NotFound {
		t.Fatalf("after finalize: want NotFound, got %v", err)
	}
}

func TestDefinitionsRoundtrip(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	ctx := context.Background()

	defineVM(t, c)

	defs, err := c.ListDefinitions(ctx, &graphenepbv1.ListDefinitionsRequest{})
	if err != nil {
		t.Fatalf("list definitions: %v", err)
	}

	if len(defs.GetDefinitions()) != 1 || defs.GetDefinitions()[0].GetKind() != "aws.vm" {
		t.Fatalf("definitions: %v", defs.GetDefinitions())
	}

	got, err := c.GetDefinition(ctx, &graphenepbv1.GetDefinitionRequest{Kind: "aws.vm"})
	if err != nil {
		t.Fatalf("get definition: %v", err)
	}

	if got.GetDefinition().GetVersion() != 1 {
		t.Fatalf("definition version: %d", got.GetDefinition().GetVersion())
	}
}

func defineExecution(t *testing.T, c graphenepbv1.ResourceServiceClient) {
	t.Helper()

	spec := schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "execution-spec"}).
		Fields(
			schemapb.Str("entrypoint").Required(),
			schemapb.Str("placement").Required(),
		).
		MustBuild()
	stat := schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "execution-status"}).
		Fields(schemapb.Str("phase")).
		MustBuild()

	_, err := c.Define(context.Background(), &graphenepbv1.DefineRequest{
		Definition: &graphenepbv1.ResourceDefinition{
			Kind:         "Execution",
			PathSegments: []string{"tenant", "env", "workflow", "run", "node", "attempt"},
			SpecSchema:   spec,
			StatusSchema: stat,
		},
	})
	if err != nil {
		t.Fatalf("define execution: %v", err)
	}
}

func executionResource(node, placement string) *graphenepbv1.Resource {
	return &graphenepbv1.Resource{
		Key: &graphenepbv1.Key{
			Kind: "Execution",
			Path: []string{"acme", "prod", "deploy", "1", node, "1"},
		},
		Spec: schemapb.MustStructFromGo(map[string]any{
			"entrypoint": "shell.bash",
			"placement":  placement,
		}),
	}
}

func TestKernelSeesOnlyItsExecutions(t *testing.T) {
	t.Parallel()

	env := newEnv(t)
	admin, kern := env(adminToken), env(kernelToken)
	ctx := context.Background()

	defineExecution(t, admin)

	for node, placement := range map[string]string{"build": "k1", "test": "k2"} {
		if _, err := admin.Put(ctx, &graphenepbv1.PutRequest{Resource: executionResource(node, placement)}); err != nil {
			t.Fatalf("put %s: %v", node, err)
		}
	}

	// List: the mandatory grant filter hides the foreign execution even
	// without a client selector.
	list, err := kern.List(ctx, &graphenepbv1.ListRequest{Kind: "Execution"})
	if err != nil {
		t.Fatalf("kernel list: %v", err)
	}

	if len(list.GetResources()) != 1 {
		t.Fatalf("kernel list: got %d executions, want 1", len(list.GetResources()))
	}

	if got := list.GetResources()[0].GetKey().GetPath()[4]; got != "build" {
		t.Fatalf("kernel list: got %q, want build", got)
	}

	// Get of a foreign execution is denied outright.
	_, err = kern.Get(ctx, &graphenepbv1.GetRequest{
		Key: &graphenepbv1.Key{Kind: "Execution", Path: []string{"acme", "prod", "deploy", "1", "test", "1"}},
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("foreign get: want PermissionDenied, got %v", err)
	}
}

func TestKernelWatchIsFiltered(t *testing.T) {
	t.Parallel()

	env := newEnv(t)
	admin, kern := env(adminToken), env(kernelToken)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	defineExecution(t, admin)

	watcher, err := kern.Watch(ctx, &graphenepbv1.WatchRequest{Kind: "Execution"})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	// Foreign first, then own: only the own one must arrive.
	if _, err := admin.Put(ctx, &graphenepbv1.PutRequest{Resource: executionResource("test", "k2")}); err != nil {
		t.Fatal(err)
	}

	if _, err := admin.Put(ctx, &graphenepbv1.PutRequest{Resource: executionResource("build", "k1")}); err != nil {
		t.Fatal(err)
	}

	event := recvData(t, watcher)
	if got := event.GetResource().GetKey().GetPath()[4]; got != "build" {
		t.Fatalf("watch leaked foreign execution: got %q", got)
	}
}

func TestKernelWritesStatusOnly(t *testing.T) {
	t.Parallel()

	env := newEnv(t)
	admin, kern := env(adminToken), env(kernelToken)
	ctx := context.Background()

	defineExecution(t, admin)

	put, err := admin.Put(ctx, &graphenepbv1.PutRequest{Resource: executionResource("build", "k1")})
	if err != nil {
		t.Fatal(err)
	}

	got, err := kern.Get(ctx, &graphenepbv1.GetRequest{
		Key: &graphenepbv1.Key{Kind: "Execution", Path: []string{"acme", "prod", "deploy", "1", "build", "1"}},
	})
	if err != nil {
		t.Fatalf("kernel get own: %v", err)
	}

	// Status write on its own execution — allowed.
	res := got.GetResource()
	res.Status = schemapb.MustStructFromGo(map[string]any{"phase": "running"})

	if _, err := kern.Put(ctx, &graphenepbv1.PutRequest{Resource: res, ExpectedRevision: put.GetRevision()}); err != nil {
		t.Fatalf("kernel status put: %v", err)
	}

	// Spec write — denied (Parts).
	fresh, err := kern.Get(ctx, &graphenepbv1.GetRequest{Key: res.GetKey()})
	if err != nil {
		t.Fatal(err)
	}

	mutated := fresh.GetResource()
	mutated.Spec = schemapb.MustStructFromGo(map[string]any{
		"entrypoint": "shell.bash",
		"placement":  "k2", // trying to move itself out of scope
	})

	_, err = kern.Put(ctx, &graphenepbv1.PutRequest{Resource: mutated, ExpectedRevision: mutated.GetRevision()})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("kernel spec put: want PermissionDenied, got %v", err)
	}

	// Define — denied.
	if _, err := kern.Define(ctx, &graphenepbv1.DefineRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("kernel define: want PermissionDenied, got %v", err)
	}
}

func TestUnauthenticatedRejected(t *testing.T) {
	t.Parallel()

	env := newEnv(t)
	bad := env("no-such-token")

	_, err := bad.List(context.Background(), &graphenepbv1.ListRequest{Kind: "Execution"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unknown token: want Unauthenticated, got %v", err)
	}
}
