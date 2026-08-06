package api

import (
	"context"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/convert"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Get reads one resource.
func (s *Service) Get(
	ctx context.Context,
	request *graphenepbv1.GetRequest,
) (*graphenepbv1.GetResponse, error) {
	session, err := s.session(ctx)
	if err != nil {
		return nil, err
	}

	id, err := convert.IdFromPb(request.GetId())
	if err != nil {
		return nil, s.fail(err)
	}

	stored, err := session.Get(ctx, id)
	if err != nil {
		return nil, s.fail(err)
	}

	return &graphenepbv1.GetResponse{Record: convert.RecordToPb(stored)}, nil
}

// List walks everything under an id.
//
// The walk runs in the goroutine grpc-go already gave this call. That is
// why the kernel's watch is pulled rather than pushed: nothing here has
// to start anything, so a stream is a loop and not a fan-out.
func (s *Service) List(
	request *graphenepbv1.ListRequest,
	out graphenepbv1.KernelService_ListServer,
) error {
	ctx := out.Context()

	session, err := s.session(ctx)
	if err != nil {
		return err
	}

	prefix, err := convert.IdFromPb(request.GetPrefix())
	if err != nil {
		return s.fail(err)
	}

	for stored, err := range session.List(ctx, prefix) {
		if err != nil {
			return s.fail(err)
		}

		if err := out.Send(&graphenepbv1.ListResponse{Record: convert.RecordToPb(stored)}); err != nil {
			return err
		}
	}

	return nil
}

// Watch follows changes under an id, delivering no snapshot.
func (s *Service) Watch(
	request *graphenepbv1.WatchRequest,
	out graphenepbv1.KernelService_WatchServer,
) error {
	ctx := out.Context()

	session, err := s.session(ctx)
	if err != nil {
		return err
	}

	prefix, err := convert.IdFromPb(request.GetPrefix())
	if err != nil {
		return s.fail(err)
	}

	stream, err := session.Watch(ctx, prefix, revision.Revision(request.GetAfter()))
	if err != nil {
		return s.fail(err)
	}

	defer func() { _ = stream.Close() }()

	for {
		event, err := stream.Next(ctx)
		if err != nil {
			return s.fail(err)
		}

		if err := out.Send(&graphenepbv1.WatchResponse{Event: convert.EventToPb(event)}); err != nil {
			return err
		}
	}
}

// Revision is the cursor to take before a snapshot.
func (s *Service) Revision(
	ctx context.Context,
	_ *graphenepbv1.RevisionRequest,
) (*graphenepbv1.RevisionResponse, error) {
	session, err := s.session(ctx)
	if err != nil {
		return nil, err
	}

	at, err := session.Revision(ctx)
	if err != nil {
		return nil, s.fail(err)
	}

	return &graphenepbv1.RevisionResponse{Revision: at.Uint64()}, nil
}

// Holders is what points at a resource, and what those pointers mean.
func (s *Service) Holders(
	ctx context.Context,
	request *graphenepbv1.HoldersRequest,
) (*graphenepbv1.HoldersResponse, error) {
	session, err := s.session(ctx)
	if err != nil {
		return nil, err
	}

	id, err := convert.IdFromPb(request.GetId())
	if err != nil {
		return nil, s.fail(err)
	}

	holders, err := session.Holders(ctx, id)
	if err != nil {
		return nil, s.fail(err)
	}

	written := make([]*graphenepbv1.Holder, 0, len(holders))

	for _, holder := range holders {
		written = append(written, &graphenepbv1.Holder{
			Id:       convert.IdToPb(holder.Id),
			Strength: convert.StrengthToPb(holder.Strength),
		})
	}

	return &graphenepbv1.HoldersResponse{Holders: written}, nil
}

// Put writes what an author asked for.
func (s *Service) Put(
	ctx context.Context,
	request *graphenepbv1.PutRequest,
) (*graphenepbv1.PutResponse, error) {
	session, err := s.session(ctx)
	if err != nil {
		return nil, err
	}

	id, err := convert.IdFromPb(request.GetId())
	if err != nil {
		return nil, s.fail(err)
	}

	intent, err := resource.NewIntent(id, request.GetSpec())
	if err != nil {
		return nil, s.fail(err)
	}

	at, err := session.Put(ctx, intent, revision.Revision(request.GetExpect()))
	if err != nil {
		return nil, s.fail(err)
	}

	return &graphenepbv1.PutResponse{Revision: at.Uint64()}, nil
}

// Report records what a controller found.
func (s *Service) Report(
	ctx context.Context,
	request *graphenepbv1.ReportRequest,
) (*graphenepbv1.ReportResponse, error) {
	session, err := s.session(ctx)
	if err != nil {
		return nil, err
	}

	id, err := convert.IdFromPb(request.GetId())
	if err != nil {
		return nil, s.fail(err)
	}

	at, err := session.Report(ctx, id, request.GetStatus(), revision.Revision(request.GetExpect()))
	if err != nil {
		return nil, s.fail(err)
	}

	return &graphenepbv1.ReportResponse{Revision: at.Uint64()}, nil
}

// Claim places a claim on a resource's deletion.
func (s *Service) Claim(
	ctx context.Context,
	request *graphenepbv1.ClaimRequest,
) (*graphenepbv1.ClaimResponse, error) {
	session, err := s.session(ctx)
	if err != nil {
		return nil, err
	}

	id, finalizer, err := s.claim(request.GetId(), request.GetFinalizer())
	if err != nil {
		return nil, err
	}

	at, err := session.Claim(ctx, id, finalizer, revision.Revision(request.GetExpect()))
	if err != nil {
		return nil, s.fail(err)
	}

	return &graphenepbv1.ClaimResponse{Revision: at.Uint64()}, nil
}

// Release lets go of one.
func (s *Service) Release(
	ctx context.Context,
	request *graphenepbv1.ReleaseRequest,
) (*graphenepbv1.ReleaseResponse, error) {
	session, err := s.session(ctx)
	if err != nil {
		return nil, err
	}

	id, finalizer, err := s.claim(request.GetId(), request.GetFinalizer())
	if err != nil {
		return nil, err
	}

	at, err := session.Release(ctx, id, finalizer, revision.Revision(request.GetExpect()))
	if err != nil {
		return nil, s.fail(err)
	}

	return &graphenepbv1.ReleaseResponse{Revision: at.Uint64()}, nil
}

// Delete asks a resource to go away.
func (s *Service) Delete(
	ctx context.Context,
	request *graphenepbv1.DeleteRequest,
) (*graphenepbv1.DeleteResponse, error) {
	session, err := s.session(ctx)
	if err != nil {
		return nil, err
	}

	id, err := convert.IdFromPb(request.GetId())
	if err != nil {
		return nil, s.fail(err)
	}

	at, err := session.Delete(ctx, id, revision.Revision(request.GetExpect()))
	if err != nil {
		return nil, s.fail(err)
	}

	return &graphenepbv1.DeleteResponse{Revision: at.Uint64()}, nil
}

// claim reads the two things a claim and a release both name.
func (s *Service) claim(
	id *graphenepbv1.Id,
	finalizer string,
) (resource.Id, resource.Finalizer, error) {
	parsed, err := convert.IdFromPb(id)
	if err != nil {
		return resource.Id{}, "", s.fail(err)
	}

	named, err := resource.NewFinalizer(finalizer)
	if err != nil {
		return resource.Id{}, "", s.fail(err)
	}

	return parsed, named, nil
}
