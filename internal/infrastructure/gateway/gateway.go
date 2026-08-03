// Package gateway is the door a spawned process talks back through.
//
// A process gets one socket, its own, created before it starts and removed
// when it ends. On that socket it finds the ordinary resource and blob
// services — not a special "process API": whoever holds a token and
// watches a kind is a controller, and a process is no different except in
// how it proves who it is.
//
// It proves nothing. It has no token, and it is not asked for one: the
// socket it is talking on is the claim. The kernel knows which process it
// created that socket for, and says so — either by resolving it into
// credentials here, or by naming it upstream and signing with its own.
//
// WHAT THIS DOES NOT DO: with raw-exec, processes on one kernel are not
// isolated from each other. Another process running as the same user can
// connect to this socket, and no credential scheme would help — it could
// equally read a token out of /proc. Isolating processes is the runner's
// job and raw-exec has never claimed to do it; a kernel's trust boundary
// is the machine it runs on.
package gateway

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/agent"
	"github.com/graphene-ci/graphene/internal/core/auth"
)

// dirMode keeps the socket directory to this kernel's user. It is not
// isolation between processes (see the package comment) — only a fence
// against everyone else on the machine.
const dirMode = 0o700

// build assembles the server a single process talks to. It is a whole
// server per process rather than one shared: the process's identity is
// which socket it is on, so it must not be able to reach another's.
type build func(process string) *grpc.Server

// Gateway opens one socket per process under a directory.
type Gateway struct {
	dir   string
	build build
}

// OverClient answers a process by asking the kernel on the far side of
// the link, naming the process it is acting for.
func OverClient(dir string, conn *grpc.ClientConn) *Gateway {
	resources := graphenepbv1.NewResourceServiceClient(conn)
	blobs := graphenepbv1.NewBlobServiceClient(conn)

	return &Gateway{
		dir: dir,
		build: func(process string) *grpc.Server {
			srv := grpc.NewServer()
			graphenepbv1.RegisterResourceServiceServer(srv,
				&forwardResources{upstream: resources, process: process})
			graphenepbv1.RegisterBlobServiceServer(srv,
				&forwardBlobs{upstream: blobs, process: process})

			return srv
		},
	}
}

// OverService answers a process from this kernel's own store, resolving
// the vouch here rather than making one: the same question, asked of the
// same index, without a round trip to ask it of ourselves.
func OverService(dir, kernel string, resources graphenepbv1.ResourceServiceServer,
	blobs graphenepbv1.BlobServiceServer, vouching auth.Vouching,
) *Gateway {
	return &Gateway{
		dir: dir,
		build: func(process string) *grpc.Server {
			srv := grpc.NewServer(
				grpc.UnaryInterceptor(unaryAs(kernel, process, vouching)),
				grpc.StreamInterceptor(streamAs(kernel, process, vouching)),
			)
			graphenepbv1.RegisterResourceServiceServer(srv, resources)

			if blobs != nil {
				graphenepbv1.RegisterBlobServiceServer(srv, blobs)
			}

			return srv
		},
	}
}

// unaryAs and streamAs answer "who is calling" the only way this socket
// can: whoever it was opened for. There is no token to read and none is
// accepted — a call arriving here IS the process, or the process's
// isolation has already failed and no header would have saved it.
func unaryAs(kernel, process string, vouching auth.Vouching) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := actingAs(ctx, kernel, process, vouching)
		if err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

func streamAs(kernel, process string, vouching auth.Vouching) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := actingAs(stream.Context(), kernel, process, vouching)
		if err != nil {
			return err
		}

		return handler(srv, &asStream{ServerStream: stream, ctx: ctx})
	}
}

func actingAs(ctx context.Context, kernel, process string, vouching auth.Vouching) (context.Context, error) {
	creds, ok := vouching.ActingFor(kernel, process)
	if !ok {
		return nil, status.Errorf(codes.PermissionDenied,
			"no process %q on kernel %q", process, kernel)
	}

	return auth.WithCredentials(ctx, creds), nil
}

type asStream struct {
	grpc.ServerStream

	//nolint:containedctx // grpc.ServerStream carries its context this way
	ctx context.Context
}

func (s *asStream) Context() context.Context { return s.ctx }

// Open prepares a process's socket and returns what to tell it.
func (g *Gateway) Open(process string) (agent.Opened, error) {
	if err := os.MkdirAll(g.dir, dirMode); err != nil {
		return nil, fmt.Errorf("gateway: create %s: %w", g.dir, err)
	}

	path := filepath.Join(g.dir, process+".sock")

	// A socket left behind by a process that died with the kernel would
	// otherwise make the new one unbindable.
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("gateway: clear %s: %w", path, err)
	}

	var listen net.ListenConfig

	lis, err := listen.Listen(context.Background(), "unix", path)
	if err != nil {
		return nil, fmt.Errorf("gateway: listen %s: %w", path, err)
	}

	opened := &door{path: path, srv: g.build(process), done: make(chan struct{})}

	go func() {
		defer close(opened.done)

		_ = opened.srv.Serve(lis)
	}()

	return opened, nil
}

type door struct {
	path string
	srv  *grpc.Server
	done chan struct{}

	once sync.Once
}

// Env is everything the process is told about the system it is in.
func (d *door) Env() map[string]string {
	return map[string]string{
		agent.EnvSocket:  d.path,
		agent.EnvProcess: filepath.Base(d.path[:len(d.path)-len(".sock")]),
	}
}

// Close stops answering and takes the socket away. A process that outlives
// its door finds a closed connection, which is the truthful answer: the
// kernel is no longer vouching for it.
func (d *door) Close() error {
	d.once.Do(func() {
		d.srv.Stop()
		<-d.done
		_ = os.Remove(d.path)
	})

	return nil
}
