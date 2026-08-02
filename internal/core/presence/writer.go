package presence

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/key"
)

// OverService adapts the resource service a kernel holds in-process, and
// OverClient the far side of a link. Both exist so a control kernel
// registers through exactly the same code a worker uses — a kernel is a
// kernel, wherever its truth happens to live.
func OverService(service graphenepbv1.ResourceServiceServer) Writer {
	return &localWriter{service: service}
}

// OverClient adapts the far side of a link.
func OverClient(client graphenepbv1.ResourceServiceClient) Writer {
	return &clientWriter{client: client}
}

type localWriter struct {
	service graphenepbv1.ResourceServiceServer
}

func (w *localWriter) Get(ctx context.Context, k key.Key) (*graphenepbv1.Resource, error) {
	got, err := w.service.Get(ctx, &graphenepbv1.GetRequest{Key: k.Proto()})

	return unwrapGet(got, err)
}

func (w *localWriter) Put(ctx context.Context, res *graphenepbv1.Resource, expected uint64) error {
	_, err := w.service.Put(ctx, &graphenepbv1.PutRequest{Resource: res, ExpectedRevision: expected})

	return wrapPut(err)
}

type clientWriter struct {
	client graphenepbv1.ResourceServiceClient
}

func (w *clientWriter) Get(ctx context.Context, k key.Key) (*graphenepbv1.Resource, error) {
	got, err := w.client.Get(ctx, &graphenepbv1.GetRequest{Key: k.Proto()})

	return unwrapGet(got, err)
}

func (w *clientWriter) Put(ctx context.Context, res *graphenepbv1.Resource, expected uint64) error {
	_, err := w.client.Put(ctx, &graphenepbv1.PutRequest{Resource: res, ExpectedRevision: expected})

	return wrapPut(err)
}

// unwrapGet turns "not found" — however it arrived — into ErrAbsent, so
// the caller reads the same answer from a local service and from a link.
func unwrapGet(got *graphenepbv1.GetResponse, err error) (*graphenepbv1.Resource, error) {
	if status.Code(err) == codes.NotFound {
		return nil, ErrAbsent
	}

	if err != nil {
		return nil, fmt.Errorf("presence: get: %w", err)
	}

	return got.GetResource(), nil
}

func wrapPut(err error) error {
	if err != nil {
		return fmt.Errorf("presence: put: %w", err)
	}

	return nil
}
