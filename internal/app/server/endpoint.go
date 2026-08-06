// Package server is how a kernel is reached, and nothing about what it
// answers.
//
// The answers are api's, and the split is the point: what a kernel does
// is decided in one place, and getting there is decided here. Today that
// is gRPC and only gRPC; another way in would be another file in this
// package and not another copy of the rules.
//
// What is here is the part grpc has no room for. Its Serve blocks and its
// Stop is a separate call, so something outside both has to hold which
// server is current and decide when it stops being — which is what makes
// an address changed in the configuration move the socket.
package server

import (
	"context"
	"net"
	"sync"

	"github.com/gopherex/xlog"
	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
	"google.golang.org/grpc"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"
)

// Configured is what an endpoint needs to know about configuration:
// where to listen, and when that answer moved.
//
// An interface rather than the whole application, because this package
// has no business with the rest of it — and because the dependency would
// otherwise run in a circle.
type Configured interface {
	// Listen is the address to serve on.
	Listen() string
	// Changed is closed the next time the configuration changes. Take it
	// before reading Listen, or lose the change that answers what was
	// just read.
	Changed() <-chan struct{}
}

// Endpoint owns "there is a server listening", across restarts.
//
// TWO long-lived workers, started at the composition root, and none per
// server:
//
//	Serve   builds, listens, blocks, and builds again when it returns
//	Rebind  waits for the address to change and stops what is listening
//
// Rebind stopping the server is what makes Serve return, which is what
// makes it build the next one. Neither spawns anything, so a kernel that
// has rebound a hundred times has the same two goroutines it started
// with.
type Endpoint struct {
	config  Configured
	service graphenepbv1.KernelServiceServer
	health  hv1.HealthServer
	log     *xlog.Logger

	mu      sync.Mutex
	current *listening
	at      string
}

// New builds an endpoint.
//
// It takes the SERVICES and not a way to register them. Both generated
// interfaces are the whole of what a kernel answers, so a server stood up
// here cannot be given anything else, and a second way in — whenever
// there is one — is built from the same values rather than from its own
// registration.
//
// Health rides the same server on purpose. A health check answering from
// somewhere else is a check on somewhere else: a separate port can be
// listening while this one is not, and then a supervisor is told the
// kernel is fine by a socket that is not the kernel.
func New(
	configured Configured,
	service graphenepbv1.KernelServiceServer,
	health hv1.HealthServer,
	log *xlog.Logger,
) *Endpoint {
	return &Endpoint{config: configured, service: service, health: health, log: log}
}

// Serve keeps a server standing until ctx is done.
//
// An address that will not bind is logged and waited on rather than
// guessed around. A kernel configured by a FILE can be told the right
// address with a text editor, so inventing one for it would only hide the
// mistake until somebody wondered why nothing was on the port they asked
// for.
func (e *Endpoint) Serve(ctx context.Context) error {
	for ctx.Err() == nil {
		// SUBSCRIBE BEFORE READING, which is the same order the store's
		// watch is used in and matters for the same reason. Reading the
		// address first and taking the channel after loses any change
		// that lands in between — and the change that lands there is
		// exactly the one correcting the address that just failed to
		// bind, so the kernel would wait for a second edit that nobody
		// has any reason to make.
		changed := e.config.Changed()
		wanted := e.config.Listen()

		listener, err := net.Listen("tcp", wanted)
		if err != nil {
			e.log.Error("cannot listen; waiting for the configuration to change",
				xlog.String("address", wanted), xlog.Err(err))

			if !e.idle(ctx, changed) {
				return nil
			}

			continue
		}

		if err := e.run(ctx, listener); err != nil {
			return err
		}
	}

	return nil
}

// idle waits for the configuration to change, and reports whether it is
// worth trying again.
//
// It is what makes a bad address survivable: the loop stops spinning on a
// port it cannot have and wakes when somebody edits the file.
func (e *Endpoint) idle(ctx context.Context, changed <-chan struct{}) bool {
	select {
	case <-ctx.Done():
		return false
	case <-changed:
		return true
	}
}

// run serves on one listener until somebody stops the server.
func (e *Endpoint) run(ctx context.Context, listener net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	server := grpc.NewServer(grpc.StreamInterceptor(bound(ctx)))
	graphenepbv1.RegisterKernelServiceServer(server, e.service)
	hv1.RegisterHealthServer(server, e.health)

	e.hold(&listening{server: server, cancel: cancel}, listener.Addr().String())

	e.log.Info("serving", xlog.String("address", listener.Addr().String()))

	// Serve returns when the server is stopped, which is the only way it
	// ends: either Rebind stopped it because the address changed, or the
	// shutdown did.
	if err := server.Serve(listener); err != nil {
		return err
	}

	// A context that is done means the whole kernel is going, not that
	// this address is stale, so nothing is rebuilt.
	if ctx.Err() != nil {
		return nil
	}

	e.log.Info("rebinding", xlog.String("from", listener.Addr().String()))

	return nil
}

// Rebind stops what is listening when it is no longer where it should be,
// and blocks until ctx is done.
//
// It also stops it when the kernel is going, which is what makes Serve
// return for the last time.
func (e *Endpoint) Rebind(ctx context.Context) error {
	for {
		changed := e.config.Changed()

		select {
		case <-ctx.Done():
			e.stop()

			return nil

		case <-changed:
			if wanted := e.config.Listen(); wanted != e.address() {
				e.stop()
			}
		}
	}
}

// hold records which server is current.
func (e *Endpoint) hold(current *listening, at string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.current = current
	e.at = at
}

// address is where the current server is listening.
func (e *Endpoint) address() string {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.at
}

// stop asks the current server to finish what it is doing.
func (e *Endpoint) stop() {
	e.mu.Lock()
	current := e.current
	e.mu.Unlock()

	if current != nil {
		current.stop()
	}
}

// listening is one server standing on one socket.
type listening struct {
	server *grpc.Server
	cancel context.CancelFunc

	once sync.Once
}

// stop winds it down, once, however many times it is asked.
//
// The order is what makes it a handover rather than a drop:
//
//  1. cancel, which ends the streams — they wait for a caller that has no
//     reason to hang up because the kernel is rebinding, so nothing else
//     here would ever finish
//  2. GracefulStop, which lets the calls in flight answer
func (l *listening) stop() {
	l.once.Do(func() {
		l.cancel()
		l.server.GracefulStop()
	})
}

// bound ties every stream to the run.
//
// A watch ends when its caller hangs up, and a caller has no reason to
// hang up because the kernel is stopping — so without this a graceful
// stop would wait for exactly the calls that are never going to end, and
// the shutdown would only ever finish by timing out and killing the
// process. AfterFunc rather than a goroutine per stream: it is registered
// while the call runs and taken off when it returns.
func bound(ctx context.Context) grpc.StreamServerInterceptor {
	return func(
		service any,
		stream grpc.ServerStream,
		_ *grpc.StreamServerInfo,
		handle grpc.StreamHandler,
	) error {
		joined, cancel := context.WithCancel(stream.Context())
		defer cancel()

		defer context.AfterFunc(ctx, cancel)()

		return handle(service, bounded{ServerStream: stream, ctx: joined})
	}
}

// bounded is a stream that also ends when the run does.
type bounded struct {
	grpc.ServerStream

	ctx context.Context
}

// Context is the call's, ended by whichever comes first.
func (b bounded) Context() context.Context { return b.ctx }
