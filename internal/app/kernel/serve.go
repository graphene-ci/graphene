package kernel

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/infrastructure/server"
	tlsutil "github.com/graphene-ci/graphene/internal/infrastructure/tls"
)

const socketDirMode = 0o750

// ErrNoTokenSource — serving without a token source would mean serving
// without authentication.
var ErrNoTokenSource = errors.New("kernel: cannot serve without a token source")

// serve starts the configured endpoints. The TCP endpoint carries TLS; the
// unix socket does not (the channel is the filesystem, guarded by its
// permissions) — but BOTH require a bearer token: authentication does not
// branch on transport.
func (k *Kernel) serve(ctx context.Context, group *errgroup.Group) error {
	if k.tokens == nil {
		return ErrNoTokenSource
	}

	if addr := k.cfg.Listen.TCP; addr != "" {
		creds, err := k.transportCredentials()
		if err != nil {
			return err
		}

		var listenCfg net.ListenConfig

		lis, err := listenCfg.Listen(ctx, "tcp", addr)
		if err != nil {
			return fmt.Errorf("kernel: listen tcp %s: %w", addr, err)
		}

		k.startServer(ctx, group, lis, grpc.Creds(creds), "tcp", addr)
	}

	if path := k.cfg.Listen.UDS; path != "" && !k.cfg.Listen.DisableUDS {
		lis, err := listenUnix(ctx, path)
		if err != nil {
			return err
		}

		k.startServer(ctx, group, lis, nil, "uds", path)
	}

	return nil
}

//nolint:ireturn // grpc's own credentials constructor returns this interface
func (k *Kernel) transportCredentials() (credentials.TransportCredentials, error) {
	cfg := k.cfg.TLS

	var (
		cert tls.Certificate
		err  error
	)

	if cfg.Mode == "files" {
		cert, err = tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("kernel: load tls certificate: %w", err)
		}
	} else {
		cert, err = tlsutil.Ensure(cfg.Dir, cfg.DNSNames)
		if err != nil {
			return nil, fmt.Errorf("kernel: prepare tls certificate: %w", err)
		}
	}

	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}), nil
}

// startServer registers the services on a fresh grpc.Server sharing the
// kernel's interceptors, and serves the listener until ctx is done.
func (k *Kernel) startServer(ctx context.Context, group *errgroup.Group,
	lis net.Listener, extra grpc.ServerOption, kind, addr string,
) {
	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(server.UnaryAuth(k.tokens)),
		grpc.StreamInterceptor(server.StreamAuth(k.tokens)),
	}
	if extra != nil {
		opts = append(opts, extra)
	}

	srv := grpc.NewServer(opts...)
	graphenepbv1.RegisterResourceServiceServer(srv, k.resources)

	if k.blobSvc != nil {
		graphenepbv1.RegisterBlobServiceServer(srv, k.blobSvc)
	}

	k.log.Info("serving", "transport", kind, "address", addr)

	group.Go(func() error {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("kernel: serve %s: %w", kind, err)
		}

		return nil
	})

	group.Go(func() error {
		<-ctx.Done()
		srv.GracefulStop()

		return nil
	})
}

// listenUnix prepares the socket path: stale sockets from a crashed
// predecessor are removed, since a bound-but-dead socket would otherwise
// make every start fail.
func listenUnix(ctx context.Context, path string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), socketDirMode); err != nil {
		return nil, fmt.Errorf("kernel: mkdir socket dir: %w", err)
	}

	if _, err := os.Stat(path); err == nil {
		var dialer net.Dialer

		if conn, derr := dialer.DialContext(ctx, "unix", path); derr == nil {
			_ = conn.Close()

			return nil, fmt.Errorf("kernel: socket %s is already served", path) //nolint:err113 // path carries the meaning
		}

		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("kernel: remove stale socket: %w", err)
		}
	}

	var listenCfg net.ListenConfig

	lis, err := listenCfg.Listen(ctx, "unix", path)
	if err != nil {
		return nil, fmt.Errorf("kernel: listen unix %s: %w", path, err)
	}

	return lis, nil
}
