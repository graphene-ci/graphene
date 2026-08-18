// Package auth validates bearer tokens and attaches the resulting
// principal to the request context. Temporal is visible to nobody: every
// gRPC call — agent session or proxied Temporal traffic — passes here.
package auth

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/config"
	"github.com/graphene-ci/pipeline/pkg/id"
)

// Role is what a principal is allowed to be.
type Role string

// Roles of the installation.
const (
	RoleAgent Role = "agent"
	RoleRun   Role = "run"
	RoleAdmin Role = "admin"
)

// Principal is an authenticated caller.
type Principal struct {
	Role Role
	// AgentId is set for agent principals: the one record the token may
	// embody.
	AgentId id.AgentId
}

// Authenticator checks tokens.
type Authenticator struct {
	tokens []config.Token
}

// New builds an authenticator over the configured token list.
func New(tokens []config.Token) *Authenticator {
	return &Authenticator{tokens: tokens}
}

// Check resolves a bearer token to a principal.
func (a *Authenticator) Check(token string) (Principal, bool) {
	for _, t := range a.tokens {
		if subtle.ConstantTimeCompare([]byte(t.Token), []byte(token)) == 1 {
			return Principal{Role: Role(t.Role), AgentId: id.AgentId(t.AgentId)}, true
		}
	}
	return Principal{}, false
}

type principalKey struct{}

// FromContext returns the principal the interceptor attached.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// WithPrincipal attaches a principal (used by interceptors and tests).
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// BearerFromMD extracts the bearer token from gRPC metadata.
func BearerFromMD(md metadata.MD) string {
	for _, v := range md.Get("authorization") {
		if strings.HasPrefix(v, "Bearer ") {
			return strings.TrimPrefix(v, "Bearer ")
		}
	}
	return ""
}

// healthService is served without a token: Kubernetes and load
// balancers probe it; it exposes nothing but up/down.
const healthService = "/grpc.health.v1.Health/"

// StreamInterceptor authenticates every stream, including the unknown
// services forwarded to the Temporal proxy.
func (a *Authenticator) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if strings.HasPrefix(info.FullMethod, healthService) {
			return handler(srv, ss)
		}
		md, _ := metadata.FromIncomingContext(ss.Context())
		p, ok := a.Check(BearerFromMD(md))
		if !ok {
			return status.Error(codes.Unauthenticated, "invalid token")
		}
		return handler(srv, &wrappedStream{ServerStream: ss, ctx: WithPrincipal(ss.Context(), p)})
	}
}

// UnaryInterceptor authenticates unary calls.
func (a *Authenticator) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if strings.HasPrefix(info.FullMethod, healthService) {
			return handler(ctx, req)
		}
		md, _ := metadata.FromIncomingContext(ctx)
		p, ok := a.Check(BearerFromMD(md))
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "invalid token")
		}
		return handler(WithPrincipal(ctx, p), req)
	}
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }
