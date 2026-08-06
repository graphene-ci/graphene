package upstream

import (
	"context"
	"fmt"

	"github.com/gopherex/schemapb/go/schemapb"
	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/convert"
	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Recorder is the kernel above, spoken to in domain types.
//
// It exists for ONE record — the one saying this kernel exists — and its
// three methods are the three that record takes. Everything else a
// subordinate does goes through the proxy above and never becomes a
// domain type at all: converting a call on the way through would mean
// this kernel could refuse what the kernel above would have allowed.
//
// The calls carry this kernel's OWN credential. Nobody asked for this
// record; the kernel writes it about itself.
type Recorder struct {
	up *Upstream
}

// Recording is how this kernel writes itself down, up there.
func (u *Upstream) Recording() *Recorder { return &Recorder{up: u} }

// Get reads the record back.
//
// A NotFound from up there becomes the same error the store gives, so
// what reads it does not have to know which of the two it is talking to.
func (r *Recorder) Get(
	ctx context.Context, id resource.Id,
) (store.Value[resource.Resource], error) {
	answer, err := r.up.client.Get(r.up.own(ctx), &graphenepbv1.GetRequest{
		Id: convert.IdToPb(id),
	})
	if err != nil {
		return store.Value[resource.Resource]{}, upstreamError(err)
	}

	return recordFromPb(answer.GetRecord())
}

// Put creates the record.
func (r *Recorder) Put(
	ctx context.Context, intent resource.Intent, expect revision.Revision,
) (revision.Revision, error) {
	answer, err := r.up.client.Put(r.up.own(ctx), &graphenepbv1.PutRequest{
		Id:     convert.IdToPb(intent.Id()),
		Spec:   intent.Spec(),
		Expect: expect.Uint64(),
	})
	if err != nil {
		return revision.None, upstreamError(err)
	}

	return revision.Revision(answer.GetRevision()), nil
}

// Report writes what this kernel is running with.
func (r *Recorder) Report(
	ctx context.Context,
	id resource.Id,
	reported *schemapb.StructValue,
	expect revision.Revision,
) (revision.Revision, error) {
	answer, err := r.up.client.Report(r.up.own(ctx), &graphenepbv1.ReportRequest{
		Id:     convert.IdToPb(id),
		Status: reported,
		Expect: expect.Uint64(),
	})
	if err != nil {
		return revision.None, upstreamError(err)
	}

	return revision.Revision(answer.GetRevision()), nil
}

// recordFromPb reads a record back into what the domain calls one.
func recordFromPb(record *graphenepbv1.Record) (store.Value[resource.Resource], error) {
	read, err := convert.ResourceFromPb(record.GetResource())
	if err != nil {
		return store.Value[resource.Resource]{}, err
	}

	return store.Value[resource.Resource]{
		Value:           read,
		Revision:        revision.Revision(record.GetRevision()),
		CreatedRevision: revision.Revision(record.GetCreatedRevision()),
	}, nil
}

// upstreamError gives a failure from up there the name it would have had
// down here.
//
// Only the one that is ASKED ABOUT: "there is no such record" is the
// difference between creating this kernel's record and updating it, and a
// caller comparing errors would otherwise have to know it was talking to
// a proxy. Everything else keeps its own words and is wrapped, because a
// failure that came from another kernel should say so.
func upstreamError(err error) error {
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("%w: %s", store.ErrNotFound, status.Convert(err).Message())
	}

	return fmt.Errorf("upstream: %w", err)
}

// Revision is what the kernel above is at, and is how a subordinate
// answers for its own health.
//
// A proxy whose upstream is gone has nothing to offer anybody, so "am I
// well" is the same question as "is it there" — asked as this kernel,
// because a health check has no caller to borrow a credential from.
func (r *Recorder) Revision(ctx context.Context) (revision.Revision, error) {
	answer, err := r.up.client.Revision(r.up.own(ctx), &graphenepbv1.RevisionRequest{})
	if err != nil {
		return revision.None, upstreamError(err)
	}

	return revision.Revision(answer.GetRevision()), nil
}
