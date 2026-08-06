package app

import (
	"context"
	"log/slog"
	"net"
	"sync"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
	"google.golang.org/grpc"

	"github.com/graphene-ci/graphene/internal/wire"
)

// Endpoint owns "there is a server listening", across restarts.
//
// It is the controlling piece that grpc has no room for: grpc's Serve
// blocks and its Stop is a separate call, so something outside both has
// to hold which server is current and decide when it stops being. That
// something is here, and it is a value with a lock rather than a
// goroutine, because the two loops that drive it start where every other
// loop does.
//
// TWO long-lived workers and none per server:
//
//	Serve   builds, listens, blocks, and builds again when it returns
//	Rebind  waits for the address to change and stops what is listening
//
// Rebind stopping the server is what makes Serve return, which is what
// makes it build the next one. Neither spawns anything, so a kernel that
// has rebound a hundred times has the same two goroutines it started
// with.
type Endpoint struct {
	app *App
	log *slog.Logger

	mu      sync.Mutex
	current *grpc.Server
	// at is the address the current server is on, which is not always the
	// configured one: a configured address that would not bind leaves the
	// last one that did.
	at string
}

// Endpoint builds one over this kernel.
func (a *App) Endpoint(log *slog.Logger) *Endpoint {
	return &Endpoint{app: a, log: log}
}

// Serve keeps a server standing until ctx is done.
//
// An address that will not bind is logged and waited on rather than
// guessed around. There used to be a fallback here — keep the last
// address that worked — and it went with the configuration: a kernel
// configured by a file can be told the right address with a text editor,
// so inventing one for it would only hide the mistake until somebody
// wondered why nothing was on the port they had asked for.
func (e *Endpoint) Serve(ctx context.Context) error {
	for ctx.Err() == nil {
		// SUBSCRIBE BEFORE READING, which is the same order the store's
		// watch is used in and matters for the same reason. Reading the
		// address first and taking the channel after loses any change
		// that lands in between — and the change that lands there is
		// exactly the one correcting the address that just failed to
		// bind, so the kernel would wait for a second edit that nobody
		// has any reason to make.
		changed := e.app.Changed()
		wanted := e.app.Config().Listen()

		listener, err := net.Listen("tcp", wanted)
		if err != nil {
			e.log.Error("cannot listen; waiting for the configuration to change",
				"address", wanted, "err", err)

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
	server := grpc.NewServer()
	graphenepbv1.RegisterKernelServiceServer(server,
		wire.New(e.app.Guard(), e.app.Kernel(), e.log))

	e.hold(server, listener.Addr().String())

	e.log.Info("serving", "address", listener.Addr().String())

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

	e.log.Info("rebinding", "from", listener.Addr().String())

	return nil
}

// Rebind stops what is listening when it is no longer where it should be,
// and blocks until ctx is done.
//
// It also stops it when the kernel is going, which is what makes Serve
// return for the last time. GracefulStop rather than Stop: a rebind is a
// handover and not a drop, so calls already in flight on the old socket
// finish on it.
func (e *Endpoint) Rebind(ctx context.Context) error {
	for {
		changed := e.app.Changed()

		select {
		case <-ctx.Done():
			e.stop()

			return nil

		case <-changed:
			if wanted := e.app.Config().Listen(); wanted != e.address() {
				e.stop()
			}
		}
	}
}

// hold records which server is current.
func (e *Endpoint) hold(server *grpc.Server, at string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.current = server
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
	server := e.current
	e.mu.Unlock()

	if server != nil {
		server.GracefulStop()
	}
}
