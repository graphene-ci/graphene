// Package server holds the transport assembly for a kernel's gRPC
// endpoints: authentication interceptors today; listeners and TLS wiring
// arrive with the command layer.
package server

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/core/auth"
)

const bearerPrefix = "bearer "

// UnaryAuth authenticates every unary call: bearer token → credentials in
// the context. Authorization stays in the service layer (it needs the
// object); this interceptor only answers "who is calling".
func UnaryAuth(source auth.TokenSource) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := authenticate(ctx, source)
		if err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

// StreamAuth is UnaryAuth for server streams.
func StreamAuth(source auth.TokenSource) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := authenticate(stream.Context(), source)
		if err != nil {
			return err
		}

		return handler(srv, &authedStream{ServerStream: stream, ctx: ctx})
	}
}

type authedStream struct {
	grpc.ServerStream

	ctx context.Context //nolint:containedctx // the grpc.ServerStream contract carries its context this way
}

func (s *authedStream) Context() context.Context { return s.ctx }

func authenticate(ctx context.Context, source auth.TokenSource) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}

	values := md.Get("authorization")
	if len(values) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing authorization token")
	}

	raw := values[0]
	if !strings.HasPrefix(strings.ToLower(raw), bearerPrefix) {
		return nil, status.Error(codes.Unauthenticated, "authorization is not a bearer token")
	}

	token := strings.TrimSpace(raw[len(bearerPrefix):])

	creds, ok := source.Lookup(token)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "unknown token")
	}

	return auth.WithCredentials(ctx, creds), nil
}
