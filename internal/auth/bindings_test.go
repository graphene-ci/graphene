package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/config"
)

type fakeBindings struct{ namespace, method string }

func (f *fakeBindings) AuthorizeGRPC(_ context.Context, token, namespace, method string) (Principal, bool, error) {
	if token != "sa-token" {
		return Principal{}, false, nil
	}
	f.namespace, f.method = namespace, method
	if method != "/graphene.workerplane.v1.ManifestAPI/PublishManifest" {
		return Principal{}, true, errors.New(method + " is not open to a binding")
	}
	return Principal{Role: RoleBound, Namespace: namespace}, true, nil
}

// The native door: configured and minted tokens as before; any other
// token goes to the binding gate with the method and the namespace the
// call names — Unauthenticated when nobody issued it, PermissionDenied
// when its bindings do not open the method.
func TestUnaryInterceptorFallsBackToBindings(t *testing.T) {
	gate := &fakeBindings{}
	a := New([]config.Token{{Token: "admin-token", Role: "admin", Namespace: "*"}}).WithBindings(gate)
	ic := a.UnaryInterceptor()
	call := func(token, ns, method string) (Principal, error) {
		md := metadata.Pairs("authorization", "Bearer "+token)
		if ns != "" {
			md.Set("x-graphene-namespace", ns)
		}
		var got Principal
		_, err := ic(metadata.NewIncomingContext(context.Background(), md), nil, &grpc.UnaryServerInfo{FullMethod: method},
			func(ctx context.Context, _ any) (any, error) { got, _ = FromContext(ctx); return nil, nil })
		return got, err
	}

	p, err := call("admin-token", "", "/x/Any")
	require.NoError(t, err)
	require.Equal(t, RoleAdmin, p.Role)
	require.Empty(t, gate.method, "a known token never reaches the gate")

	p, err = call("sa-token", "t-arenadata-rnd", "/graphene.workerplane.v1.ManifestAPI/PublishManifest")
	require.NoError(t, err)
	require.Equal(t, Principal{Role: RoleBound, Namespace: "t-arenadata-rnd"}, p)
	require.Equal(t, "t-arenadata-rnd", gate.namespace)

	_, err = call("sa-token", "", "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution")
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Equal(t, "default", gate.namespace, "no header: the default namespace")

	_, err = call("garbage", "x", "/graphene.workerplane.v1.ManifestAPI/PublishManifest")
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	// Without a gate the door is what it was.
	_, err = New(nil).UnaryInterceptor()(metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer sa-token")),
		nil, &grpc.UnaryServerInfo{FullMethod: "/m"}, func(context.Context, any) (any, error) { return nil, nil })
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}
