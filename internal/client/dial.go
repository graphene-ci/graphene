package client

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
	blobpb "github.com/graphene-ci/graphenepb/v1/blob"

	"github.com/graphene-ci/graphene/internal/blob"
	"github.com/graphene-ci/graphene/internal/infrastructure/blob/remote"
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
	options, err := reaching(one)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(target(one.Address()), options...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", one, err)
	}

	return &Kernel{
		context: one,
		conn:    conn,
		client:  graphenepbv1.NewKernelServiceClient(conn),
	}, nil
}

// Bytes is the kernel's byte store, as this client may use it.
//
// The same connection and the same credential: bytes are a separate
// service because they are a separate kind of thing, not because they are
// somewhere else.
func (k *Kernel) Bytes() blob.Store {
	return remote.Over(blobpb.NewBlobServiceClient(k.conn))
}

// reaching is how this kernel is talked to: over a port, checked against
// its pin, or over a command's pipes, which are checked by being the
// pipes of a command this client ran.
func reaching(one Context) ([]grpc.DialOption, error) {
	if command, piped := Piped(one.Address()); piped {
		return []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return piping(command)
			}),
		}, nil
	}

	// WHICH kernel this is, decided before anything is sent. A client
	// carries a credential and is about to hand it over, so an address
	// that turned out to be somebody else would be handing it to them.
	creds, err := link.Reaching(one.Pins()...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", one, err)
	}

	return []grpc.DialOption{grpc.WithTransportCredentials(creds)}, nil
}

// target is what gRPC is told to resolve.
//
// A command is not a name to look up, so the resolver is told to pass one
// through and the dialer above answers instead.
func target(address string) string {
	if _, piped := Piped(address); piped {
		return "passthrough:///pipe"
	}

	return address
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
