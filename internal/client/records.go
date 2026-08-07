package client

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/convert"
	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Records is a kernel spoken to in domain types rather than in messages.
//
// Most of this client does not want one: a command reads a file, sends
// it, and prints what came back, and turning that into domain types on
// the way would be work with nothing to show for it. This exists for the
// commands that write something the BINARY knows the shape of — joining a
// kernel writes a role whose grants are a function of a name — where
// building the message by hand would mean spelling that shape twice.
type Records struct {
	on *Kernel
}

// Records is how a command writes what this binary knows the shape of.
func (k *Kernel) Records() *Records { return &Records{on: k} }

// Get reads one record back.
func (r *Records) Get(ctx context.Context, id resource.Id) (store.Value[resource.Resource], error) {
	answer, err := r.on.Calls().Get(r.on.As(ctx), &graphenepbv1.GetRequest{Id: convert.IdToPb(id)})
	if err != nil {
		return store.Value[resource.Resource]{}, remote(err)
	}

	read, err := convert.ResourceFromPb(answer.GetRecord().GetResource())
	if err != nil {
		return store.Value[resource.Resource]{}, err
	}

	return store.Value[resource.Resource]{
		Value:           read,
		Revision:        revision.Revision(answer.GetRecord().GetRevision()),
		CreatedRevision: revision.Revision(answer.GetRecord().GetCreatedRevision()),
	}, nil
}

// Put writes one.
func (r *Records) Put(
	ctx context.Context, intent resource.Intent, expect revision.Revision,
) (revision.Revision, error) {
	answer, err := r.on.Calls().Put(r.on.As(ctx), &graphenepbv1.PutRequest{
		Id:     convert.IdToPb(intent.Id()),
		Spec:   intent.Spec(),
		Expect: expect.Uint64(),
	})
	if err != nil {
		return revision.None, remote(err)
	}

	return revision.Revision(answer.GetRevision()), nil
}

// remote gives a failure from the kernel the name it would have had here.
//
// Only "there is no such record", because that one is the difference
// between creating something and updating it, and a caller comparing
// errors should not have to know it was talking over a wire. Everything
// else travels as the kernel said it: what a person is shown is the
// sentence the kernel wrote, not a translation of it.
func remote(err error) error {
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("%w: %s", store.ErrNotFound, status.Convert(err).Message())
	}

	return err
}
