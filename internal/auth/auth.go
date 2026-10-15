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
	// RoleBound is a caller whose call was decided by the namespace's
	// bindings — a service account's token, a person's id_token — on one
	// of the gRPC methods open to them. It names no rights of its own:
	// only the handlers of those methods admit it.
	RoleBound Role = "bound"
)

// Principal is an authenticated caller.
type Principal struct {
	Role Role
	// Namespace scopes the caller; "*" is every namespace (admins). It
	// comes from the TOKEN, never from a request: a foreign namespace
	// cannot even be asked for.
	Namespace string
	// AgentId is set for agent principals: the one record the token may
	// embody.
	AgentId id.AgentId
}

// In reports whether the principal may act in the namespace.
func (p Principal) In(namespace string) bool {
	return p.Namespace == "*" || p.Namespace == namespace
}

// Authenticator checks tokens.
type Authenticator struct {
	tokens   []config.Token
	minter   *Minter
	bindings BindingGate
}

// BindingGate decides a gRPC call for a token Check does not know, with
// the namespace's roles and bindings: who the token names (authenticated)
// and whether it may call this method in this namespace (nil when it
// may). An allowed call gets the principal it returns.
type BindingGate interface {
	AuthorizeGRPC(ctx context.Context, token, namespace, fullMethod string) (p Principal, authenticated bool, err error)
}

// WithBindings lets the gRPC door admit the tokens of service accounts
// and people on the methods the gate opens to them.
func (a *Authenticator) WithBindings(g BindingGate) *Authenticator {
	a.bindings = g
	return a
}

// resolve is the gRPC door's answer: a configured or minted token by
// Check, any other by the binding gate.
func (a *Authenticator) resolve(ctx context.Context, fullMethod string) (Principal, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	token := BearerFromMD(md)
	if p, ok := a.Check(token); ok {
		return p, nil
	}
	if a.bindings == nil || token == "" {
		return Principal{}, status.Error(codes.Unauthenticated, "invalid token")
	}
	namespace := "default"
	if hdr := md.Get(namespaceHeader); len(hdr) > 0 && hdr[0] != "" {
		namespace = hdr[0]
	}
	p, authenticated, err := a.bindings.AuthorizeGRPC(ctx, token, namespace, fullMethod)
	switch {
	case !authenticated:
		return Principal{}, status.Error(codes.Unauthenticated, "invalid token")
	case err != nil:
		return Principal{}, status.Error(codes.PermissionDenied, err.Error())
	}
	return p, nil
}

// namespaceHeader is the namespace a call names (services.NamespaceHeader).
const namespaceHeader = "x-graphene-namespace"

// New builds an authenticator over the configured token list.
func New(tokens []config.Token) *Authenticator {
	return &Authenticator{tokens: tokens}
}

// WithMinter teaches the authenticator the installation's short-lived
// tokens: a run or an agent carries one instead of a shared secret
// from the configuration.
func (a *Authenticator) WithMinter(m *Minter) *Authenticator {
	a.minter = m
	return a
}

// Check resolves a bearer token to a principal: a configured one
// first, then a minted one. A minted token becomes the SAME principal
// shape, so everything downstream — the worker plane, the Temporal
// proxy — keeps working without knowing which contour issued it.
func (a *Authenticator) Check(token string) (Principal, bool) {
	for _, t := range a.tokens {
		if subtle.ConstantTimeCompare([]byte(t.Token), []byte(token)) == 1 {
			return Principal{Role: Role(t.Role), Namespace: t.Namespace, AgentId: id.AgentId(t.AgentId)}, true
		}
	}
	if a.minter != nil {
		if identity, role, ok := a.minter.Verify(token); ok {
			p := Principal{Role: Role(role), Namespace: identity.Namespace}
			// "sa:agent/edge-1" is the one agent record this token may
			// embody — the same scope the configured agent tokens have.
			if agentId, ok := strings.CutPrefix(identity.Subject.Name, "agent/"); ok {
				p.AgentId = id.AgentId(agentId)
			}
			return p, true
		}
	}
	return Principal{}, false
}

// Identity resolves a bearer token to an authorization identity, for
// the callers that decide with roles rather than principals.
func (a *Authenticator) Identity(token string) (Identity, bool) {
	if a.minter == nil {
		return Identity{}, false
	}
	identity, role, ok := a.minter.Verify(token)
	if !ok {
		return Identity{}, false
	}
	return Identity{Identity: identity, BoundRole: role}, true
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
		p, err := a.resolve(ss.Context(), info.FullMethod)
		if err != nil {
			return err
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
		p, err := a.resolve(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(WithPrincipal(ctx, p), req)
	}
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }
