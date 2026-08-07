package remote_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/gopherex/xlog"

	blobpb "github.com/graphene-ci/graphenepb/v1/blob"

	"github.com/graphene-ci/graphene/internal/app/api"
	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/blob"
	"github.com/graphene-ci/graphene/internal/blob/blobtest"
	"github.com/graphene-ci/graphene/internal/infrastructure/blob/fs"
	"github.com/graphene-ci/graphene/internal/infrastructure/blob/remote"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store/kv/memory"
)

// A store one hop away is a store. The same suite the filesystem passes,
// run against a real server over a real connection — which is what makes
// "the same thing, further away" a fact rather than an intention.
func TestConformance(t *testing.T) {
	t.Parallel()

	blobtest.Run(t, blobtest.Factory{
		Open: func(t *testing.T) blob.Store {
			t.Helper()

			return remote.Over(serve(t))
		},
	})
}

// serve puts a blob service on a connection in memory. Authorisation is
// deliberately not in the picture: what is under test is whether the
// bytes and the refusals survive the wire, and a guard in the middle
// would be a second thing able to fail.
func serve(t *testing.T) blobpb.BlobServiceClient {
	t.Helper()

	store, err := fs.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	t.Cleanup(func() { _ = store.Close() })

	// A REAL kernel behind it, because the record is what decides now:
	// what may be read, what exists, and what may be removed are all
	// questions about a resource. Serving the bytes with nothing behind
	// them would test a service this no longer is.
	ctx := context.Background()

	bytes := memory.New()

	t.Cleanup(func() { _ = bytes.Close() })

	k := kernel.New(bytes)
	if err := auth.Bootstrap(ctx, k); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	if _, err := k.Define(ctx, blob.Definition()); err != nil {
		t.Fatalf("define blob: %v", err)
	}

	if _, _, err := auth.Begin(ctx, k, ""); err != nil {
		t.Fatalf("first identity: %v", err)
	}

	root, err := auth.NewPrincipal(auth.First)
	if err != nil {
		t.Fatalf("principal: %v", err)
	}

	server := grpc.NewServer()
	blobpb.RegisterBlobServiceServer(server, api.NewBlobs(store, auth.New(k),
		func(context.Context) (auth.Principal, error) { return root, nil },
		xlog.New(xlog.NopCore{}),
	))

	listener := bufconn.Listen(1 << 20)
	go func() { _ = server.Serve(listener) }()

	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	return blobpb.NewBlobServiceClient(conn)
}
