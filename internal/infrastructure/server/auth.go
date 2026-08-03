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

const (
	bearerPrefix = "bearer "

	// actingForHeader carries the name of the process a kernel is
	// vouching for. The kernel signs the request with its OWN token; this
	// only says whom it is acting for, and is answered from the store.
	//
	// A forwarding kernel MUST strip whatever the process itself put here
	// before adding its own: otherwise a process names any identity it
	// likes and the vouch means nothing.
	actingForHeader = "graphene-acting-for"
)

// UnaryAuth authenticates every unary call: bearer token → credentials in
// the context. Authorization stays in the service layer (it needs the
// object); this interceptor only answers "who is calling".
//
// vouching may be nil — then no call may act for anyone, which is the
// right answer for a kernel that has no store to check a vouch against.
func UnaryAuth(source auth.TokenSource, vouching auth.Vouching) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := authenticate(ctx, source, vouching)
		if err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

// StreamAuth is UnaryAuth for server streams.
func StreamAuth(source auth.TokenSource, vouching auth.Vouching) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := authenticate(stream.Context(), source, vouching)
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

func authenticate(ctx context.Context, source auth.TokenSource, vouching auth.Vouching) (context.Context, error) {
	incoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}

	values := incoming.Get("authorization")
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

	process := incoming.Get(actingForHeader)
	if len(process) == 0 {
		return auth.WithCredentials(ctx, creds), nil
	}

	vouched, err := vouch(vouching, creds.Principal, process[0])
	if err != nil {
		return nil, err
	}

	return auth.WithCredentials(ctx, vouched), nil
}

// vouch resolves "kernel K is acting for process P" into P's credentials.
// Every failure is a denial, never a fallback to the caller's own
// authority: a vouch that cannot be checked must not quietly become a
// call by the kernel itself, which holds far more.
func vouch(vouching auth.Vouching, caller auth.Principal, process string) (auth.Credentials, error) {
	if vouching == nil {
		return auth.Credentials{}, status.Error(codes.PermissionDenied,
			"this kernel cannot check a vouch")
	}

	// Only a kernel may vouch, and only for processes of its own name.
	// Anyone else asking is trying to become someone else.
	if caller.Kind != auth.PrincipalKernel {
		return auth.Credentials{}, status.Error(codes.PermissionDenied,
			"only a kernel may act for a process")
	}

	creds, ok := vouching.ActingFor(caller.Name, process)
	if !ok {
		return auth.Credentials{}, status.Errorf(codes.PermissionDenied,
			"no process %q on kernel %q", process, caller.Name)
	}

	return creds, nil
}
