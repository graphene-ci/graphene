package commands

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/client"
)

// expectation is what a write of this resource must expect: the revision
// it is at, or Absent if it is not there.
//
// THERE IS NO "I DO NOT CARE". Every write carries an expectation, and
// the zero one means "must not exist yet" rather than "whatever is
// there" — which is the kernel refusing blind overwrites by construction
// rather than by convention. A caller that wants to write what is
// already there has to have READ it, and this is that read.
//
// So there is a race, and it is the honest one: somebody writing between
// the read and the write makes this fail with a conflict rather than
// silently losing their work. A person at a terminal reads the message
// and runs the command again.
func expectation(
	ctx context.Context, on *client.Kernel, at *graphenepbv1.Id,
) (uint64, error) {
	found, err := on.Calls().Get(ctx, &graphenepbv1.GetRequest{Id: at})

	switch {
	case status.Code(err) == codes.NotFound:
		// Absent, which is the zero revision: this write creates it.
		return 0, nil
	case err != nil:
		return 0, err
	}

	return found.GetRecord().GetRevision(), nil
}
