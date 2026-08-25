package services

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	connectcors "connectrpc.com/cors"
	"github.com/go-chi/cors"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/pkg/proto/management/v1/managementv1connect"
)

// MountConnect mounts the management plane onto the door's HTTP mux.
// The generated connect handlers serve the connect, gRPC, and gRPC-Web
// protocols on one implementation — no per-protocol adapters. Auth is
// the same bearer space; the middleware also carries the namespace
// header into the gRPC metadata the services read.
func MountConnect(mux *http.ServeMux, m *Management, o *Observe, authn *auth.Authenticator) {
	opts := connect.WithInterceptors(statusCodes{})
	mount := func(pattern string, handler http.Handler) {
		mux.Handle(pattern, withCORS(withBearer(handler, authn, m)))
	}
	mount(managementv1connect.NewRunsAPIHandler(m, opts))
	mount(managementv1connect.NewResourcesAPIHandler(m, opts))
	mount(managementv1connect.NewNamespacesAPIHandler(m, opts))
	mount(managementv1connect.NewSecretsAPIHandler(m, opts))
	mount(managementv1connect.NewVarsAPIHandler(m, opts))
	mount(managementv1connect.NewRevisionsAPIHandler(m, opts))
	mount(managementv1connect.NewWorkspacesAPIHandler(m, opts))
	mount(managementv1connect.NewRbacAPIHandler(m, opts))
	mount(managementv1connect.NewObserveAPIHandler(o, opts))
}

// withCORS lets a browser UI hosted on another origin call the
// management plane. Auth is a bearer header and cookies are never
// used (AllowCredentials stays false), so allowing every origin grants
// nothing by itself — each request still needs a valid token. The
// header lists come from connectrpc.com/cors and cover all three
// protocols (connect, gRPC, gRPC-Web); preflights are answered before
// auth (browsers send them without the Authorization header).
func withCORS(next http.Handler) http.Handler {
	return cors.Handler(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: connectcors.AllowedMethods(),
		AllowedHeaders: append(connectcors.AllowedHeaders(), "Authorization", NamespaceHeader),
		ExposedHeaders: connectcors.ExposedHeaders(),
		MaxAge:         7200,
	})(next)
}

// withBearer authenticates every request and mirrors the namespace
// header into gRPC metadata. Four contours answer, in order of cost:
// a configured token, a minted one (a run, an agent), a service
// account's issued token, and finally an identity provider's id_token.
func withBearer(next http.Handler, authn *auth.Authenticator, m *Management) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		ctx := r.Context()
		namespace := r.Header.Get(NamespaceHeader)

		switch identity, ok := m.authenticate(ctx, token, namespace); {
		case ok:
			ctx = auth.WithIdentity(ctx, identity)
		default:
			p, ok := authn.Check(token)
			if !ok {
				http.Error(w, "invalid token", http.StatusUnauthorized)
				return
			}
			ctx = auth.WithPrincipal(ctx, p)
		}
		if namespace != "" {
			ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(NamespaceHeader, namespace))
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// statusCodes translates the shared helpers' gRPC statuses into the
// matching connect codes (numerically identical spaces), so browsers
// see invalid_argument — not unknown.
type statusCodes struct{}

func (statusCodes) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		res, err := next(ctx, req)
		if err != nil {
			if s, ok := status.FromError(err); ok {
				return nil, connect.NewError(connect.Code(s.Code()), errors.New(s.Message()))
			}
		}
		return res, err
	}
}

func (statusCodes) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (statusCodes) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}
