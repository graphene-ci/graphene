package services

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/auth"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
	"github.com/graphene-ci/graphene/pkg/proto/management/v1/managementv1connect"
)

// MountConnect mounts the management plane for browsers onto the door's
// HTTP mux: ConnectRPC serves the connect, gRPC-web, and JSON protocols
// under the services' own path prefixes. Auth is the same bearer space;
// the middleware also carries the namespace header into the gRPC
// metadata the services read.
func MountConnect(mux *http.ServeMux, m *Management, authn *auth.Authenticator) {
	mount := func(pattern string, handler http.Handler) {
		mux.Handle(pattern, withBearer(handler, authn))
	}
	mount(managementv1connect.NewRunsAPIHandler(runsConnect{m}))
	mount(managementv1connect.NewResourcesAPIHandler(resourcesConnect{m}))
	mount(managementv1connect.NewNamespacesAPIHandler(namespacesConnect{m}))
	mount(managementv1connect.NewSecretsAPIHandler(secretsConnect{m}))
}

// withBearer authenticates every request and mirrors the namespace
// header into gRPC metadata.
func withBearer(next http.Handler, authn *auth.Authenticator) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		p, ok := authn.Check(token)
		if !ok {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}
		ctx := auth.WithPrincipal(r.Context(), p)
		if ns := r.Header.Get(NamespaceHeader); ns != "" {
			ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(NamespaceHeader, ns))
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// The adapters below are mechanical: same impl, connect envelopes.

func adapt[Req, Res any](ctx context.Context, req *connect.Request[Req], fn func(context.Context, *Req) (*Res, error)) (*connect.Response[Res], error) {
	res, err := fn(ctx, req.Msg)
	if err != nil {
		return nil, asConnectError(err)
	}
	return connect.NewResponse(res), nil
}

// asConnectError translates the shared impl's gRPC status into the
// matching connect code, so browsers see invalid_argument — not
// unknown.
func asConnectError(err error) error {
	s, ok := status.FromError(err)
	if !ok {
		return err
	}
	return connect.NewError(connect.Code(s.Code()), errors.New(s.Message()))
}

type runsConnect struct{ m *Management }

func (a runsConnect) StartRun(ctx context.Context, req *connect.Request[managementv1.StartRunRequest]) (*connect.Response[managementv1.StartRunResponse], error) {
	return adapt(ctx, req, a.m.StartRun)
}

func (a runsConnect) GetRun(ctx context.Context, req *connect.Request[managementv1.GetRunRequest]) (*connect.Response[managementv1.GetRunResponse], error) {
	return adapt(ctx, req, a.m.GetRun)
}

func (a runsConnect) RunResult(ctx context.Context, req *connect.Request[managementv1.RunResultRequest]) (*connect.Response[managementv1.RunResultResponse], error) {
	return adapt(ctx, req, a.m.RunResult)
}

func (a runsConnect) CancelRun(ctx context.Context, req *connect.Request[managementv1.CancelRunRequest]) (*connect.Response[managementv1.CancelRunResponse], error) {
	return adapt(ctx, req, a.m.CancelRun)
}

func (a runsConnect) ListRuns(ctx context.Context, req *connect.Request[managementv1.ListRunsRequest]) (*connect.Response[managementv1.ListRunsResponse], error) {
	return adapt(ctx, req, a.m.ListRuns)
}

type resourcesConnect struct{ m *Management }

func (a resourcesConnect) List(ctx context.Context, req *connect.Request[managementv1.ListRequest]) (*connect.Response[managementv1.ListResponse], error) {
	return adapt(ctx, req, a.m.List)
}

func (a resourcesConnect) Get(ctx context.Context, req *connect.Request[managementv1.GetRequest]) (*connect.Response[managementv1.GetResponse], error) {
	return adapt(ctx, req, a.m.Get)
}

func (a resourcesConnect) Tree(ctx context.Context, req *connect.Request[managementv1.TreeRequest]) (*connect.Response[managementv1.TreeResponse], error) {
	return adapt(ctx, req, a.m.Tree)
}

func (a resourcesConnect) Delete(ctx context.Context, req *connect.Request[managementv1.DeleteRequest]) (*connect.Response[managementv1.DeleteResponse], error) {
	return adapt(ctx, req, a.m.Delete)
}

func (a resourcesConnect) Transfer(ctx context.Context, req *connect.Request[managementv1.TransferRequest]) (*connect.Response[managementv1.TransferResponse], error) {
	return adapt(ctx, req, a.m.Transfer)
}

func (a resourcesConnect) Invoke(ctx context.Context, req *connect.Request[managementv1.InvokeRequest]) (*connect.Response[managementv1.InvokeResponse], error) {
	return adapt(ctx, req, a.m.Invoke)
}

type namespacesConnect struct{ m *Management }

func (a namespacesConnect) CreateNamespace(ctx context.Context, req *connect.Request[managementv1.CreateNamespaceRequest]) (*connect.Response[managementv1.CreateNamespaceResponse], error) {
	return adapt(ctx, req, a.m.CreateNamespace)
}

func (a namespacesConnect) ListNamespaces(ctx context.Context, req *connect.Request[managementv1.ListNamespacesRequest]) (*connect.Response[managementv1.ListNamespacesResponse], error) {
	return adapt(ctx, req, a.m.ListNamespaces)
}

type secretsConnect struct{ m *Management }

func (a secretsConnect) SetSecret(ctx context.Context, req *connect.Request[managementv1.SetSecretRequest]) (*connect.Response[managementv1.SetSecretResponse], error) {
	return adapt(ctx, req, a.m.SetSecret)
}

func (a secretsConnect) DeleteSecret(ctx context.Context, req *connect.Request[managementv1.DeleteSecretRequest]) (*connect.Response[managementv1.DeleteSecretResponse], error) {
	return adapt(ctx, req, a.m.DeleteSecret)
}

func (a secretsConnect) ListSecrets(ctx context.Context, req *connect.Request[managementv1.ListSecretsRequest]) (*connect.Response[managementv1.ListSecretsResponse], error) {
	return adapt(ctx, req, a.m.ListSecrets)
}
