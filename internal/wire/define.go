package wire

import (
	"context"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/convert"
	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Define publishes a shape for a kind and makes it current.
//
// The definition arrives without a version — which version it becomes is
// the store's word — so it is read at version one and the number the
// store actually gave comes back in the reply.
func (s *Server) Define(
	ctx context.Context,
	request *graphenepbv1.DefineRequest,
) (*graphenepbv1.DefineResponse, error) {
	session, err := s.session(ctx)
	if err != nil {
		return nil, err
	}

	message := request.GetDefinition()
	if message == nil {
		return nil, s.fail(def.ErrNoKind)
	}

	// A version on the way in is ignored rather than refused: it is the
	// store's to assign, and a client that echoed back what it read would
	// otherwise have to strip a field to write it again.
	message.Version = uint32(def.NoVersion.Next())

	published, err := convert.DefinitionFromPb(message)
	if err != nil {
		return nil, s.fail(err)
	}

	head, err := session.Define(ctx, published.Definition())
	if err != nil {
		return nil, s.fail(err)
	}

	return &graphenepbv1.DefineResponse{Definition: convert.DefinitionToPb(head.Published)}, nil
}

// Undefine removes a kind and every version of it.
func (s *Server) Undefine(
	ctx context.Context,
	request *graphenepbv1.UndefineRequest,
) (*graphenepbv1.UndefineResponse, error) {
	session, err := s.session(ctx)
	if err != nil {
		return nil, err
	}

	named, err := kind.New(request.GetKind())
	if err != nil {
		return nil, s.fail(err)
	}

	if err := session.Undefine(ctx, named); err != nil {
		return nil, s.fail(err)
	}

	return &graphenepbv1.UndefineResponse{}, nil
}

// GetDefinition is a kind's shape: whichever is current, or the one a
// resource pinned.
func (s *Server) GetDefinition(
	ctx context.Context,
	request *graphenepbv1.GetDefinitionRequest,
) (*graphenepbv1.GetDefinitionResponse, error) {
	session, err := s.session(ctx)
	if err != nil {
		return nil, err
	}

	named, err := kind.New(request.GetKind())
	if err != nil {
		return nil, s.fail(err)
	}

	if version := def.Version(request.GetVersion()); !version.IsZero() {
		published, err := session.DefinitionAt(ctx, named, version)
		if err != nil {
			return nil, s.fail(err)
		}

		return &graphenepbv1.GetDefinitionResponse{
			Definition: convert.DefinitionToPb(published),
		}, nil
	}

	head, err := session.Definition(ctx, named)
	if err != nil {
		return nil, s.fail(err)
	}

	return &graphenepbv1.GetDefinitionResponse{
		Definition: convert.DefinitionToPb(head.Published),
	}, nil
}

// ListKinds walks every kind that has been defined.
func (s *Server) ListKinds(
	_ *graphenepbv1.ListKindsRequest,
	out graphenepbv1.KernelService_ListKindsServer,
) error {
	ctx := out.Context()

	session, err := s.session(ctx)
	if err != nil {
		return err
	}

	for head, err := range session.Kinds(ctx) {
		if err != nil {
			return s.fail(err)
		}

		message := &graphenepbv1.ListKindsResponse{
			Definition: convert.DefinitionToPb(head.Published),
		}

		if err := out.Send(message); err != nil {
			return err
		}
	}

	return nil
}

// WatchKinds follows what is current: one kind if named, all of them if
// not.
func (s *Server) WatchKinds(
	request *graphenepbv1.WatchKindsRequest,
	out graphenepbv1.KernelService_WatchKindsServer,
) error {
	ctx := out.Context()

	session, err := s.session(ctx)
	if err != nil {
		return err
	}

	after := revision.Revision(request.GetAfter())

	stream, err := s.kinds(ctx, session, request.GetKind(), after)
	if err != nil {
		return err
	}

	defer func() { _ = stream.Close() }()

	for {
		event, err := stream.Next(ctx)
		if err != nil {
			return s.fail(err)
		}

		if err := out.Send(&graphenepbv1.WatchKindsResponse{
			Event: convert.KindEventToPb(event),
		}); err != nil {
			return err
		}
	}
}

// kinds opens the right watch: one kind if the request named one, every
// kind if it did not.
//
// Two calls rather than one with an empty name, because they need two
// different permissions — watching one kind is not watching all of them —
// and the guard is what decides that, not this.
func (s *Server) kinds(
	ctx context.Context,
	session auth.Session,
	named string,
	after revision.Revision,
) (store.Stream[def.Head], error) {
	if named == "" {
		stream, err := session.WatchKinds(ctx, after)
		if err != nil {
			return store.Stream[def.Head]{}, s.fail(err)
		}

		return stream, nil
	}

	parsed, err := kind.New(named)
	if err != nil {
		return store.Stream[def.Head]{}, s.fail(err)
	}

	stream, err := session.WatchKind(ctx, parsed, after)
	if err != nil {
		return store.Stream[def.Head]{}, s.fail(err)
	}

	return stream, nil
}
