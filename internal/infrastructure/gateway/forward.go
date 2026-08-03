package gateway

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
)

// The forwarding half: a kernel that holds no truth answers its processes
// by asking the kernel that does.
//
// This is typed delegation rather than a byte proxy, on purpose. The
// kernel does not merely relay these calls — it VOUCHES for them, and a
// relay that could not see what it was passing could not say who it was
// passing it for. It also means the compiler notices when the contract
// grows a method, instead of the method silently not working.

// actingFor is how a kernel says whom it is acting for. Its own token
// signs the request; this only names the process, and the far side
// answers from the store whether such a process is on this kernel.
const actingFor = "graphene-acting-for"

// outgoing builds the metadata for one forwarded call: the caller's
// headers are DROPPED, not merged. A process must not be able to say
// anything about who it is — that is the one claim the kernel makes on
// its behalf, and letting the process contribute to it would make the
// vouch meaningless.
func outgoing(ctx context.Context, process string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(actingFor, process))
}

type forwardResources struct {
	graphenepbv1.UnimplementedResourceServiceServer

	upstream graphenepbv1.ResourceServiceClient
	process  string
}

func (f *forwardResources) Get(ctx context.Context, req *graphenepbv1.GetRequest) (*graphenepbv1.GetResponse, error) {
	return call(outgoing(ctx, f.process), f.upstream.Get, req)
}

func (f *forwardResources) Put(ctx context.Context, req *graphenepbv1.PutRequest) (*graphenepbv1.PutResponse, error) {
	return call(outgoing(ctx, f.process), f.upstream.Put, req)
}

func (f *forwardResources) Delete(
	ctx context.Context, req *graphenepbv1.DeleteRequest,
) (*graphenepbv1.DeleteResponse, error) {
	return call(outgoing(ctx, f.process), f.upstream.Delete, req)
}

func (f *forwardResources) List(
	ctx context.Context, req *graphenepbv1.ListRequest,
) (*graphenepbv1.ListResponse, error) {
	return call(outgoing(ctx, f.process), f.upstream.List, req)
}

func (f *forwardResources) Define(
	ctx context.Context, req *graphenepbv1.DefineRequest,
) (*graphenepbv1.DefineResponse, error) {
	return call(outgoing(ctx, f.process), f.upstream.Define, req)
}

func (f *forwardResources) Undefine(
	ctx context.Context, req *graphenepbv1.UndefineRequest,
) (*graphenepbv1.UndefineResponse, error) {
	return call(outgoing(ctx, f.process), f.upstream.Undefine, req)
}

func (f *forwardResources) GetDefinition(
	ctx context.Context, req *graphenepbv1.GetDefinitionRequest,
) (*graphenepbv1.GetDefinitionResponse, error) {
	return call(outgoing(ctx, f.process), f.upstream.GetDefinition, req)
}

func (f *forwardResources) ListDefinitions(
	ctx context.Context, req *graphenepbv1.ListDefinitionsRequest,
) (*graphenepbv1.ListDefinitionsResponse, error) {
	return call(outgoing(ctx, f.process), f.upstream.ListDefinitions, req)
}

func (f *forwardResources) Watch(
	req *graphenepbv1.WatchRequest, out grpc.ServerStreamingServer[graphenepbv1.WatchEvent],
) error {
	stream, err := f.upstream.Watch(outgoing(out.Context(), f.process), req)
	if err != nil {
		return fmt.Errorf("gateway: watch: %w", err)
	}

	return pipe(stream, out)
}

func (f *forwardResources) WatchDefinitions(
	req *graphenepbv1.WatchDefinitionsRequest,
	out grpc.ServerStreamingServer[graphenepbv1.WatchDefinitionsEvent],
) error {
	stream, err := f.upstream.WatchDefinitions(outgoing(out.Context(), f.process), req)
	if err != nil {
		return fmt.Errorf("gateway: watch definitions: %w", err)
	}

	return pipe(stream, out)
}

type forwardBlobs struct {
	graphenepbv1.UnimplementedBlobServiceServer

	upstream graphenepbv1.BlobServiceClient
	process  string
}

func (f *forwardBlobs) Stat(
	ctx context.Context, req *graphenepbv1.StatRequest,
) (*graphenepbv1.StatResponse, error) {
	return call(outgoing(ctx, f.process), f.upstream.Stat, req)
}

func (f *forwardBlobs) Download(
	req *graphenepbv1.DownloadRequest, out grpc.ServerStreamingServer[graphenepbv1.DownloadResponse],
) error {
	stream, err := f.upstream.Download(outgoing(out.Context(), f.process), req)
	if err != nil {
		return fmt.Errorf("gateway: download: %w", err)
	}

	return pipe(stream, out)
}

// Upload is a client stream in both directions: frames are read from the
// process and written upstream without being held, so a blob as big as
// someone's binary never sits in a kernel's memory.
func (f *forwardBlobs) Upload(in grpc.ClientStreamingServer[graphenepbv1.UploadRequest,
	graphenepbv1.UploadResponse],
) error {
	upstream, err := f.upstream.Upload(outgoing(in.Context(), f.process))
	if err != nil {
		return fmt.Errorf("gateway: upload: %w", err)
	}

	for {
		msg, err := in.Recv()
		if err != nil {
			done, closeErr := upstream.CloseAndRecv()
			if closeErr != nil {
				return fmt.Errorf("gateway: upload commit: %w", closeErr)
			}

			if err := in.SendAndClose(done); err != nil {
				return fmt.Errorf("gateway: upload reply: %w", err)
			}

			return nil
		}

		if err := upstream.Send(msg); err != nil {
			return fmt.Errorf("gateway: upload send: %w", err)
		}
	}
}

// call is the shape every unary forward has: one upstream call, one
// wrapped error.
func call[Req, Resp any](
	ctx context.Context,
	invoke func(context.Context, Req, ...grpc.CallOption) (Resp, error),
	req Req,
) (Resp, error) {
	resp, err := invoke(ctx, req)
	if err != nil {
		var zero Resp

		return zero, fmt.Errorf("gateway: %w", err)
	}

	return resp, nil
}

// pipe copies a server stream through, message for message.
func pipe[T any](from grpc.ServerStreamingClient[T], into grpc.ServerStreamingServer[T]) error {
	for {
		msg, err := from.Recv()
		if err != nil {
			// The upstream ending is how a watch ends; the caller sees the
			// stream close and reopens if it still cares.
			return nil //nolint:nilerr // a closed upstream is not our failure
		}

		if err := into.Send(msg); err != nil {
			return fmt.Errorf("gateway: send: %w", err)
		}
	}
}
