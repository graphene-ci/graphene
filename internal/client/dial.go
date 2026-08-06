package client

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/link"
)

// Where a credential rides, and how it is introduced. The same two words
// the kernel reads on the way in.
const (
	header = "authorization"
	scheme = "bearer "
)

// Kernel is one kernel, reached.
type Kernel struct {
	context Context
	conn    *grpc.ClientConn
	client  graphenepbv1.KernelServiceClient
}

// Dial opens a connection to one kernel.
//
// It does not wait for an answer: gRPC connects lazily, so a kernel that
// is not there fails on the CALL rather than here, and the failure names
// the call somebody made rather than the connection they did not know
// they were opening.
func Dial(one Context) (*Kernel, error) {
	// WHICH kernel this is, decided before anything is sent. A client
	// carries a credential and is about to hand it over, so an address
	// that turned out to be somebody else would be handing it to them.
	creds, err := link.Reaching(one.Pin())
	if err != nil {
		return nil, fmt.Errorf("%s: %w", one, err)
	}

	conn, err := grpc.NewClient(one.Address(), grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", one, err)
	}

	return &Kernel{
		context: one,
		conn:    conn,
		client:  graphenepbv1.NewKernelServiceClient(conn),
	}, nil
}

// Close lets the connection go.
func (k *Kernel) Close() error { return k.conn.Close() }

// Context is which kernel this is.
func (k *Kernel) Context() Context { return k.context }

// Calls is the kernel's whole service, ready to be called.
//
// The generated client and not a wrapper around it: every method a kernel
// has is a method a client may want, and a hand-written subset would be a
// second opinion about what the API is.
func (k *Kernel) Calls() graphenepbv1.KernelServiceClient { return k.client }

// As puts this client's credential on a call.
//
// Every call goes through here rather than through connection-level
// credentials, because that is where the guard reads it and because a
// context carrying its own token is the thing being tested when somebody
// switches kernels.
func (k *Kernel) As(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, header, scheme+k.context.Token())
}
