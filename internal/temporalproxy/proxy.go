// Package temporalproxy forwards Temporal gRPC traffic to the real
// frontend. Workers and clients dial the graphene server, never Temporal
// itself — the server is the single door, and the stream interceptor has
// already authenticated the caller before the director runs.
package temporalproxy

import (
	"context"

	"github.com/siderolabs/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/auth"
)

// New dials the Temporal frontend and returns the proxy pieces for the
// server's gRPC composition: the codec-forcing option and the unknown
// service handler that carries all Temporal traffic.
func New(temporalHostPort string) (grpc.ServerOption, grpc.ServerOption, func() error, error) {
	backendConn, err := grpc.NewClient(temporalHostPort,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(proxy.Codec())),
	)
	if err != nil {
		return nil, nil, nil, err
	}

	backend := &proxy.SingleBackend{
		GetConn: func(ctx context.Context) (context.Context, *grpc.ClientConn, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			out := md.Copy()
			// The graphene token authenticated the caller at our door; it
			// is not for Temporal's eyes.
			out.Delete("authorization")
			return metadata.NewOutgoingContext(ctx, out), backendConn, nil
		},
	}
	director := func(ctx context.Context, _ string) (proxy.Mode, []proxy.Backend, error) {
		// The interceptor authenticated already; the director only gates
		// WHO may speak Temporal: runs and admins. Agents may not — an
		// agent is a host, not a Temporal client.
		p, ok := auth.FromContext(ctx)
		if !ok || p.Role == auth.RoleAgent {
			return proxy.One2One, nil, status.Error(codes.PermissionDenied, "temporal access requires a run or admin token")
		}
		return proxy.One2One, []proxy.Backend{backend}, nil
	}

	return grpc.ForceServerCodecV2(proxy.Codec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(director)),
		backendConn.Close,
		nil
}
