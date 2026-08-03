package controller

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/key"
)

// ErrAbsent — the resource does not exist. Both writers report it the same
// way, so a controller never has to know whether the truth is in this
// process or a link away.
var ErrAbsent = errors.New("controller: resource absent")

// Writer is the write half of what a controller needs; Stream is the read
// half. Together they are the whole of "a controller is a client".
type Writer interface {
	Get(ctx context.Context, k key.Key) (*graphenepbv1.Resource, error)
	Put(ctx context.Context, res *graphenepbv1.Resource, expected uint64) error
}

// OverService adapts the resource service a kernel holds in-process, and
// OverClient the far side of a link. Both exist so the same controller
// code runs wherever it is placed.
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
		return nil, fmt.Errorf("controller: get: %w", err)
	}

	return got.GetResource(), nil
}

func wrapPut(err error) error {
	if err != nil {
		return fmt.Errorf("controller: put: %w", err)
	}

	return nil
}

// Update is the read-modify-write every controller does: read the current
// record, let mutate decide, write it back with CAS — and on a lost race,
// read again. Losing a CAS means someone else moved first, so the decision
// has to be remade against what they wrote, never forced over it.
//
// mutate returns false when there is nothing to do, which is the common
// case: a controller is woken far more often than it has work.
func Update(ctx context.Context, writer Writer, k key.Key,
	mutate func(res *graphenepbv1.Resource) bool,
) error {
	for {
		current, err := writer.Get(ctx, k)
		if err != nil {
			return fmt.Errorf("controller: update %s: %w", k, err)
		}

		if !mutate(current) {
			return nil
		}

		err = writer.Put(ctx, current, current.GetRevision())
		if status.Code(errors.Unwrap(err)) == codes.Aborted {
			continue
		}

		if err != nil {
			return fmt.Errorf("controller: update %s: %w", k, err)
		}

		return nil
	}
}
