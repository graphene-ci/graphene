package app

import (
	"fmt"
	"log/slog"
	"net"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
	"google.golang.org/grpc"

	"github.com/graphene-ci/graphene/internal/wire"
)

// Server builds what answers for this kernel, and the socket it answers
// on.
//
// It hands both back rather than serving, because serving needs two
// goroutines and both belong where every other one starts. One runs the
// server; the other waits for the stop and calls GracefulStop, which is
// what makes the first return. It cannot be cleanup instead: cleanup runs
// after the drain, and the drain is waiting for the very call that
// GracefulStop ends.
//
// grpc-go starts goroutines of its own, one per call. That is the edge of
// the rule rather than a hole in it — they belong to a library, they end
// with the calls they serve, and GracefulStop waits for them. What the
// rule is about is code of ours, and none of ours spawns anything: a
// handler streaming events is a loop inside the goroutine grpc already
// provided, which is why the kernel's watch is pulled and not pushed.
//
// The address is read ONCE. A kernel told to listen somewhere else while
// it is running keeps the socket it has: rebinding underneath in-flight
// calls would drop them, and the configuration says where to listen next
// time rather than where to move to now.
func (a *App) Server(log *slog.Logger) (*grpc.Server, net.Listener, error) {
	at := a.Config().Listen()

	listener, err := net.Listen("tcp", at)
	if err != nil {
		return nil, nil, fmt.Errorf("listen on %s: %w", at, err)
	}

	server := grpc.NewServer()
	graphenepbv1.RegisterKernelServiceServer(server, wire.New(a.guard, a.kernel, log))

	return server, listener, nil
}
