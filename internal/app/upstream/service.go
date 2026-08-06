package upstream

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
)

// Service is the whole of what a subordinate kernel answers.
//
// Every method is the same three lines — forward the credential, make the
// call, hand back what came — and they are written out rather than
// generated or reflected over because that is what makes them readable
// and what makes the compiler check them. A reflective proxy would be
// shorter and would not fail to build the day the service grows a method.
//
// Nothing here decides anything. No retry, no caching, no defaulting: a
// subordinate that answered from a cache would answer differently from
// the kernel it stands for, and a caller has no way to tell which one it
// reached.
type Service struct {
	graphenepbv1.UnimplementedKernelServiceServer

	up *Upstream
}

// Serving is what this kernel answers with.
func (u *Upstream) Serving() *Service { return &Service{up: u} }

func (s *Service) Get(
	ctx context.Context, asked *graphenepbv1.GetRequest,
) (*graphenepbv1.GetResponse, error) {
	return s.up.client.Get(forwarded(ctx), asked)
}

func (s *Service) Holders(
	ctx context.Context, asked *graphenepbv1.HoldersRequest,
) (*graphenepbv1.HoldersResponse, error) {
	return s.up.client.Holders(forwarded(ctx), asked)
}

func (s *Service) Revision(
	ctx context.Context, asked *graphenepbv1.RevisionRequest,
) (*graphenepbv1.RevisionResponse, error) {
	return s.up.client.Revision(forwarded(ctx), asked)
}

func (s *Service) Put(
	ctx context.Context, asked *graphenepbv1.PutRequest,
) (*graphenepbv1.PutResponse, error) {
	return s.up.client.Put(forwarded(ctx), asked)
}

func (s *Service) Report(
	ctx context.Context, asked *graphenepbv1.ReportRequest,
) (*graphenepbv1.ReportResponse, error) {
	return s.up.client.Report(forwarded(ctx), asked)
}

func (s *Service) Claim(
	ctx context.Context, asked *graphenepbv1.ClaimRequest,
) (*graphenepbv1.ClaimResponse, error) {
	return s.up.client.Claim(forwarded(ctx), asked)
}

func (s *Service) Release(
	ctx context.Context, asked *graphenepbv1.ReleaseRequest,
) (*graphenepbv1.ReleaseResponse, error) {
	return s.up.client.Release(forwarded(ctx), asked)
}

func (s *Service) Delete(
	ctx context.Context, asked *graphenepbv1.DeleteRequest,
) (*graphenepbv1.DeleteResponse, error) {
	return s.up.client.Delete(forwarded(ctx), asked)
}

func (s *Service) Define(
	ctx context.Context, asked *graphenepbv1.DefineRequest,
) (*graphenepbv1.DefineResponse, error) {
	return s.up.client.Define(forwarded(ctx), asked)
}

func (s *Service) Undefine(
	ctx context.Context, asked *graphenepbv1.UndefineRequest,
) (*graphenepbv1.UndefineResponse, error) {
	return s.up.client.Undefine(forwarded(ctx), asked)
}

func (s *Service) GetDefinition(
	ctx context.Context, asked *graphenepbv1.GetDefinitionRequest,
) (*graphenepbv1.GetDefinitionResponse, error) {
	return s.up.client.GetDefinition(forwarded(ctx), asked)
}

func (s *Service) List(
	asked *graphenepbv1.ListRequest, to grpc.ServerStreamingServer[graphenepbv1.ListResponse],
) error {
	from, err := s.up.client.List(forwarded(to.Context()), asked)
	if err != nil {
		return err
	}

	return relay(from, to)
}

func (s *Service) Watch(
	asked *graphenepbv1.WatchRequest, to grpc.ServerStreamingServer[graphenepbv1.WatchResponse],
) error {
	from, err := s.up.client.Watch(forwarded(to.Context()), asked)
	if err != nil {
		return err
	}

	return relay(from, to)
}

func (s *Service) ListKinds(
	asked *graphenepbv1.ListKindsRequest, to grpc.ServerStreamingServer[graphenepbv1.ListKindsResponse],
) error {
	from, err := s.up.client.ListKinds(forwarded(to.Context()), asked)
	if err != nil {
		return err
	}

	return relay(from, to)
}

func (s *Service) WatchKinds(
	asked *graphenepbv1.WatchKindsRequest, to grpc.ServerStreamingServer[graphenepbv1.WatchKindsResponse],
) error {
	from, err := s.up.client.WatchKinds(forwarded(to.Context()), asked)
	if err != nil {
		return err
	}

	return relay(from, to)
}

// relay pours one stream into the other until it ends.
//
// ONE MESSAGE AT A TIME, on the calling goroutine and no other. A relay
// that buffered would be a relay that lied about when a watch caught up,
// and one that spawned would be a goroutine per stream in a program whose
// whole concurrency is a list at the composition root.
//
// The far side ending is not a failure here: a list that finished is a
// list that finished, and the caller is told the same way.
func relay[T any](from grpc.ServerStreamingClient[T], to grpc.ServerStreamingServer[T]) error {
	for {
		message, err := from.Recv()

		switch {
		case errors.Is(err, io.EOF):
			return nil
		case err != nil:
			return err
		}

		if err := to.Send(message); err != nil {
			return err
		}
	}
}
