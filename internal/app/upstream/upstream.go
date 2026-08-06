// Package upstream is a kernel that keeps nothing.
//
// It answers the same service as any other kernel and decides none of it:
// every call is passed to the kernel it was configured with, carrying the
// credential of whoever made it. The kernel above authorises the person
// who actually asked, sees their name in its own audit, and applies their
// grants — which is what "subordinate" has to mean if it is to mean
// anything. A proxy that re-signed each call with its own credential
// would turn every caller into one caller, and the far side would be
// unable to tell an operator from a controller.
//
// It has exactly ONE thing of its own: the record saying it exists,
// written up there under its own token. Everything else about it is a
// question for the kernel above.
package upstream

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/app/config"
)

// Where a credential rides, and how it is introduced. The same two words
// the service reads on the way in, because this is the same protocol
// pointed the other way.
const (
	header = "authorization"
	scheme = "bearer "
)

// Upstream is the connection to the kernel above, and the two things
// built on it: what this kernel answers with, and what it writes about
// itself.
type Upstream struct {
	conn   *grpc.ClientConn
	client graphenepbv1.KernelServiceClient
	token  string
}

// Open connects to the kernel above.
//
// It does not wait for it to answer. gRPC dials lazily and reconnects by
// itself, so a subordinate whose upstream is down comes up, serves, and
// fails each call with what the far side would have said — rather than
// refusing to start and needing somebody to notice it later. Its health
// says what is actually true, which is the point of having health.
func Open(to config.Upstream) (*Upstream, error) {
	// Insecure because there is no transport security anywhere in this
	// system yet, and a connection that pretended otherwise would be
	// worse than one that says what it is. When TLS arrives it arrives
	// here and at the listener together.
	conn, err := grpc.NewClient(to.Address(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("upstream %s: %w", to.Address(), err)
	}

	return &Upstream{
		conn:   conn,
		client: graphenepbv1.NewKernelServiceClient(conn),
		token:  to.Token(),
	}, nil
}

// Close lets the connection go.
func (u *Upstream) Close() error { return u.conn.Close() }

// forwarded carries the CALLER'S credential onward.
//
// Whatever arrived on the way in goes out unchanged: the kernel above
// decides, and it cannot decide about somebody it was never told about.
// A call with no credential is forwarded with none and refused up there,
// which is the same answer given in the same words as if it had arrived
// there directly.
func forwarded(ctx context.Context) context.Context {
	pairs, found := metadata.FromIncomingContext(ctx)
	if !found {
		return ctx
	}

	return metadata.NewOutgoingContext(ctx, pairs.Copy())
}

// own puts THIS kernel's credential on a call.
//
// Used for the one thing a subordinate does on its own behalf — saying
// that it exists — and for nothing else. Anything reached this way is
// done as the kernel and not as whoever happens to be connected.
func (u *Upstream) own(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, header, scheme+u.token)
}
