package service_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	schemapb "github.com/gopherex/schemapb/go/schemapb"
	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/graphene-ci/graphene/internal/core/registry"
	"github.com/graphene-ci/graphene/internal/core/service"
	"github.com/graphene-ci/graphene/internal/infrastructure/store/bbolt"
)

func newClient(t *testing.T) graphenepbv1.ResourceServiceClient {
	t.Helper()
	st, err := bbolt.Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	srv := grpc.NewServer()
	graphenepbv1.RegisterResourceServiceServer(srv, service.NewResources(st, registry.New(st)))

	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return graphenepbv1.NewResourceServiceClient(conn)
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

func TestWatchWithSelector(t *testing.T) {
	c := newClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defineVM(t, c)

	w, err := c.Watch(ctx, &graphenepbv1.WatchRequest{
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

	ev, err := w.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if ev.GetType() != graphenepbv1.EventType_EVENT_TYPE_PUT ||
		ev.GetStoreRevision() != put.GetStoreRevision() {
		t.Fatalf("watch: got %v rev=%d, want put rev=%d", ev.GetType(), ev.GetStoreRevision(), put.GetStoreRevision())
	}
	if got := ev.GetResource().GetKey().GetPath()[3]; got != "mine" {
		t.Fatalf("watch: got %q, want mine (k2 event must be filtered)", got)
	}
}

func TestFinalizerFlow(t *testing.T) {
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
