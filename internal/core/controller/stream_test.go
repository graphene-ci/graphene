package controller_test

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/auth"
	"github.com/graphene-ci/graphene/internal/core/builtin"
	"github.com/graphene-ci/graphene/internal/core/controller"
	"github.com/graphene-ci/graphene/internal/core/registry"
	"github.com/graphene-ci/graphene/internal/core/service"
	"github.com/graphene-ci/graphene/internal/core/store"
	"github.com/graphene-ci/graphene/internal/infrastructure/store/bbolt"
)

// A controller is a client, so the truth being in this process or a link
// away must be invisible to it. This drives the SAME loop over both
// streams and demands the same events out of each.
func TestBothStreamsLookTheSame(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	st, err := bbolt.Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	t.Cleanup(func() { _ = st.Close() })

	admin := auth.WithCredentials(ctx, auth.FullAccess(auth.PrincipalSystem, "test"))

	reg := registry.New(st)
	if err := builtin.Ensure(admin, reg); err != nil {
		t.Fatalf("ensure builtins: %v", err)
	}

	resources := service.NewResources(st, reg)
	client := serveResources(t, resources)

	local := collect(ctx, t, controller.Local(st, builtin.KindKernel))
	remote := collect(ctx, t, controller.Remote(client, builtin.KindKernel))

	if _, err := resources.Put(admin, &graphenepbv1.PutRequest{
		Resource: &graphenepbv1.Resource{
			Key:  &graphenepbv1.Key{Kind: builtin.KindKernel, Path: []string{"k1"}},
			Spec: schemapb.MustStructFromGo(map[string]any{"os": "linux", "arch": "amd64"}),
		},
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	for name, events := range map[string]<-chan controller.Event{"local": local, "remote": remote} {
		event := recv(t, name, events)
		if event.Type != store.EventPut {
			t.Fatalf("%s: type %v, want put", name, event.Type)
		}

		if got := event.Resource.GetKey().GetPath()[0]; got != "k1" {
			t.Fatalf("%s: path %q", name, got)
		}

		if got := event.Resource.GetSpec().ToGo()["arch"]; got != "amd64" {
			t.Fatalf("%s: spec %v", name, got)
		}
	}
}

// collect drives a real Loop over the stream and hands its events to the
// test — the point being that the loop is the same one in both cases.
func collect(ctx context.Context, t *testing.T, stream controller.Stream) <-chan controller.Event {
	t.Helper()

	events := make(chan controller.Event, 8)
	synced := make(chan struct{})
	once := sync.OnceFunc(func() { close(synced) })

	loop := &controller.Loop{
		Stream:  stream,
		Backoff: 10 * time.Millisecond,
		OnSync:  func(uint64) { once() },
		Handle: func(_ context.Context, event controller.Event) error {
			events <- event

			return nil
		},
	}

	go func() { _ = loop.Run(ctx) }()

	// Waiting for the catch-up boundary before writing keeps the test
	// about live delivery rather than about who won a race.
	select {
	case <-synced:
	case <-time.After(10 * time.Second):
		t.Fatal("stream never synced")
	}

	return events
}

func recv(t *testing.T, name string, events <-chan controller.Event) controller.Event {
	t.Helper()

	select {
	case event := <-events:
		return event
	case <-time.After(15 * time.Second):
		t.Fatalf("%s: no event", name)

		return controller.Event{}
	}
}

func serveResources(t *testing.T, resources graphenepbv1.ResourceServiceServer) graphenepbv1.ResourceServiceClient {
	t.Helper()

	// The system principal travels in-process; this test is about the
	// stream, not about tokens.
	inject := func(ctx context.Context) context.Context {
		return auth.WithCredentials(ctx, auth.FullAccess(auth.PrincipalSystem, "test"))
	}

	srv := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req any,
			_ *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
		) (any, error) {
			return handler(inject(ctx), req)
		}),
		grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream,
			_ *grpc.StreamServerInfo, handler grpc.StreamHandler,
		) error {
			return handler(srv, &injectedStream{ServerStream: stream, ctx: inject(stream.Context())})
		}),
	)
	graphenepbv1.RegisterResourceServiceServer(srv, resources)

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

type injectedStream struct {
	grpc.ServerStream

	ctx context.Context
}

func (s *injectedStream) Context() context.Context { return s.ctx }
