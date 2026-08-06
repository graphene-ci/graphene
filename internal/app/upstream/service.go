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
// reached. For the same reason nothing here wraps an error: what the
// caller is told is what the kernel above said, in its words. That is
// written down in .golangci.yaml, where wrapcheck is turned off for this
// file, because it is a decision about the file and not eleven decisions
// about eleven methods.
type Service struct {
	graphenepbv1.UnimplementedKernelServiceServer

	up *Upstream
	// acting names the process this service speaks for, or is empty when
	// it speaks for whoever is connected.
	acting string
}

// out is how a call leaves: as the caller when there is one, and as this
// kernel acting for a process when there is not.
func (s *Service) out(ctx context.Context) context.Context {
	if s.acting == "" {
		return forwarded(ctx)
	}

	return s.up.forProcess(ctx, s.acting)
}

// Serving is what this kernel answers with.
func (u *Upstream) Serving() *Service { return &Service{up: u} }

// ForProcess is what one of this kernel's processes talks to.
//
// The same forwarding, with one difference that is the whole point: the
// call goes up as THIS KERNEL acting for that process, rather than as
// whoever is connected — because whoever is connected is a process, and a
// process has nothing to be.
func (u *Upstream) ForProcess(named string) *Service {
	return &Service{up: u, acting: named}
}

func (s *Service) Get(
	ctx context.Context, asked *graphenepbv1.GetRequest,
) (*graphenepbv1.GetResponse, error) {
	return s.up.client.Get(s.out(ctx), asked)
}

func (s *Service) Holders(
	ctx context.Context, asked *graphenepbv1.HoldersRequest,
) (*graphenepbv1.HoldersResponse, error) {
	return s.up.client.Holders(s.out(ctx), asked)
}

func (s *Service) Revision(
	ctx context.Context, asked *graphenepbv1.RevisionRequest,
) (*graphenepbv1.RevisionResponse, error) {
	return s.up.client.Revision(s.out(ctx), asked)
}

func (s *Service) Put(
	ctx context.Context, asked *graphenepbv1.PutRequest,
) (*graphenepbv1.PutResponse, error) {
	return s.up.client.Put(s.out(ctx), asked)
}

func (s *Service) Report(
	ctx context.Context, asked *graphenepbv1.ReportRequest,
) (*graphenepbv1.ReportResponse, error) {
	return s.up.client.Report(s.out(ctx), asked)
}

func (s *Service) Claim(
	ctx context.Context, asked *graphenepbv1.ClaimRequest,
) (*graphenepbv1.ClaimResponse, error) {
	return s.up.client.Claim(s.out(ctx), asked)
}

func (s *Service) Release(
	ctx context.Context, asked *graphenepbv1.ReleaseRequest,
) (*graphenepbv1.ReleaseResponse, error) {
	return s.up.client.Release(s.out(ctx), asked)
}

func (s *Service) Delete(
	ctx context.Context, asked *graphenepbv1.DeleteRequest,
) (*graphenepbv1.DeleteResponse, error) {
	return s.up.client.Delete(s.out(ctx), asked)
}

func (s *Service) Define(
	ctx context.Context, asked *graphenepbv1.DefineRequest,
) (*graphenepbv1.DefineResponse, error) {
	return s.up.client.Define(s.out(ctx), asked)
}

func (s *Service) Undefine(
	ctx context.Context, asked *graphenepbv1.UndefineRequest,
) (*graphenepbv1.UndefineResponse, error) {
	return s.up.client.Undefine(s.out(ctx), asked)
}

func (s *Service) GetDefinition(
	ctx context.Context, asked *graphenepbv1.GetDefinitionRequest,
) (*graphenepbv1.GetDefinitionResponse, error) {
	return s.up.client.GetDefinition(s.out(ctx), asked)
}

func (s *Service) List(
	asked *graphenepbv1.ListRequest, to grpc.ServerStreamingServer[graphenepbv1.ListResponse],
) error {
	from, err := s.up.client.List(s.out(to.Context()), asked)
	if err != nil {
		return err
	}

	return relay(from, to)
}

func (s *Service) Watch(
	asked *graphenepbv1.WatchRequest, to grpc.ServerStreamingServer[graphenepbv1.WatchResponse],
) error {
	from, err := s.up.client.Watch(s.out(to.Context()), asked)
	if err != nil {
		return err
	}

	return relay(from, to)
}

func (s *Service) ListKinds(
	asked *graphenepbv1.ListKindsRequest, to grpc.ServerStreamingServer[graphenepbv1.ListKindsResponse],
) error {
	from, err := s.up.client.ListKinds(s.out(to.Context()), asked)
	if err != nil {
		return err
	}

	return relay(from, to)
}

func (s *Service) WatchKinds(
	asked *graphenepbv1.WatchKindsRequest, to grpc.ServerStreamingServer[graphenepbv1.WatchKindsResponse],
) error {
	from, err := s.up.client.WatchKinds(s.out(to.Context()), asked)
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
