package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"

	"github.com/gopherex/xlog"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
	blobpb "github.com/graphene-ci/graphenepb/v1/blob"
)

// SocketName is what a kernel's local door is called, beside everything
// else it owns.
const SocketName = "kernel.sock"

const (
	// socketMode is 0600: whoever can open this is the user the kernel
	// runs as, which is the whole of what it grants. What they may DO is
	// still decided by the credential they present, as over a port.
	socketMode = 0o600
	// dirMode is the kernel's own directory, which nobody else has any
	// business reading: it holds a key, a store and the sockets.
	dirMode = 0o700
)

// Socket is where a kernel listens for callers on its own machine.
func Socket(dir string) string { return filepath.Join(dir, SocketName) }

// Local is the door on this machine: the same service, on a unix socket.
//
// It exists so a kernel can be reached WITHOUT a port — `ssh host
// graphened stdio` relays a pipe into this — and it is a second listener
// rather than a second kernel, which is the difference that matters. The
// alternative, a stdio command that opens the store itself, cannot
// coexist with the kernel that is already running on it: one store is one
// process, and the second one waits for a lock it will never get.
//
// No TLS. Encryption answers "can somebody on the path read this", and
// the path is a file on the machine the kernel runs on. Whatever got the
// caller onto that machine is what is being trusted, and a second tunnel
// inside it would only hide which one was doing the work.
type Local struct {
	path    string
	service graphenepbv1.KernelServiceServer
	bytes   blobpb.BlobServiceServer
	log     *xlog.Logger
}

// Listening builds the local door.
func Listening(
	dir string,
	service graphenepbv1.KernelServiceServer,
	bytes blobpb.BlobServiceServer,
	log *xlog.Logger,
) *Local {
	return &Local{path: Socket(dir), service: service, bytes: bytes, log: log}
}

// Serve answers on the socket until ctx is done.
//
// A socket left behind by a kernel that died is removed first. It is a
// file and not a lock: the store is what refuses a second kernel, and by
// the time this runs that question has been settled.
func (l *Local) Serve(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Dir(l.path), dirMode); err != nil {
		return fmt.Errorf("local door: %w", err)
	}

	if err := os.Remove(l.path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("local door: clear %s: %w", l.path, err)
	}

	var listen net.ListenConfig

	listener, err := listen.Listen(ctx, "unix", l.path)
	if err != nil {
		return fmt.Errorf("local door: listen on %s: %w", l.path, err)
	}

	if err := os.Chmod(l.path, socketMode); err != nil {
		return fmt.Errorf("local door: %s: %w", l.path, err)
	}

	server := grpc.NewServer()
	graphenepbv1.RegisterKernelServiceServer(server, l.service)

	if l.bytes != nil {
		blobpb.RegisterBlobServiceServer(server, l.bytes)
	}

	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()

	l.log.Info("local door", xlog.String("socket", l.path))

	if err := server.Serve(listener); err != nil {
		return fmt.Errorf("local door: %w", err)
	}

	_ = os.Remove(l.path)

	return nil
}
