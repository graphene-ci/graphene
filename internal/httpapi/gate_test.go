package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/authz"
	"github.com/graphene-ci/graphene/internal/config"
)

// fakeGate knows one service account token, bound to get pipelines and
// create revisions in "tenant-a" only.
type fakeGate struct {
	namespace string
	asks      []Ask
}

func (g *fakeGate) AuthorizeHTTP(_ context.Context, token, namespace string, asks []Ask) (bool, error) {
	if token != "sa-token" {
		return false, nil
	}
	g.namespace, g.asks = namespace, asks
	if len(asks) == 0 {
		return true, nil
	}
	if namespace != "tenant-a" {
		return true, errors.New("sa:stroppy may not in " + namespace)
	}
	for _, a := range asks {
		if a == (Ask{authz.VerbGet, authz.KindPipeline}) || a == (Ask{authz.VerbCreate, authz.KindRevision}) {
			return true, nil
		}
	}
	return true, errors.New("not bound")
}

// A service account's token reaches the registry through its bindings:
// the namespace is the repository's first segment, reading asks for the
// pipeline, writing for a revision. A token nobody knows gets the docker
// challenge; a known one without the binding gets 403, not 401.
func TestRegistryDoorAdmitsBindings(t *testing.T) {
	gate := &fakeGate{}
	reached := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { reached++; w.WriteHeader(http.StatusOK) })
	d := Deps{Auth: auth.New([]config.Token{{Token: "admin-token", Role: "admin", Namespace: "*"}}), Gate: gate}
	h := d.requireRole(upstream, registryRule, auth.RoleAgent, auth.RoleRun, auth.RoleAdmin)

	do := func(method, path, user, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, nil)
		if user != "" {
			r.SetBasicAuth(user, token)
		} else if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	// The configured token keeps working, gate untouched.
	require.Equal(t, http.StatusOK, do(http.MethodPut, "/v2/anything/x/manifests/1", "", "admin-token").Code)
	require.Empty(t, gate.asks)

	// docker login: the ping needs only someone; Basic carries the token.
	require.Equal(t, http.StatusOK, do(http.MethodGet, "/v2/", "token", "sa-token").Code)

	w := do(http.MethodHead, "/v2/tenant-a/stroppy-quotas/manifests/81f8", "token", "sa-token")
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "tenant-a", gate.namespace)
	require.Equal(t, []Ask{{authz.VerbGet, authz.KindPipeline}}, gate.asks)

	w = do(http.MethodPatch, "/v2/tenant-a/stroppy-quotas/blobs/uploads/u1", "", "sa-token")
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, []Ask{{authz.VerbCreate, authz.KindRevision}, {authz.VerbUpdate, authz.KindPipeline}}, gate.asks)

	// Another namespace's images: known caller, no binding — 403.
	w = do(http.MethodGet, "/v2/tenant-b/x/manifests/1", "", "sa-token")
	require.Equal(t, http.StatusForbidden, w.Code)
	require.Contains(t, w.Body.String(), "tenant-b")

	// The catalog spans namespaces: not for a binding.
	require.Equal(t, http.StatusForbidden, do(http.MethodGet, "/v2/_catalog", "", "sa-token").Code)
	require.Equal(t, http.StatusForbidden, do(http.MethodGet, "/v2/tenant-a", "", "sa-token").Code)

	// Nobody: the challenge that makes a docker daemon send credentials.
	w = do(http.MethodGet, "/v2/tenant-a/x/manifests/1", "", "garbage")
	require.Equal(t, http.StatusUnauthorized, w.Code)
	require.Equal(t, `Basic realm="graphene"`, w.Header().Get("WWW-Authenticate"))
	require.Equal(t, http.StatusUnauthorized, do(http.MethodGet, "/v2/", "", "").Code)

	require.Equal(t, 4, reached, "admin, ping, read, write")
}

// A route without a rule stays with the configured and minted tokens.
func TestRuleLessRouteRefusesBindings(t *testing.T) {
	d := Deps{Auth: auth.New(nil), Gate: &fakeGate{}}
	h := d.requireRole(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("reached") }), nil, auth.RoleRun)
	r := httptest.NewRequest(http.MethodPost, "/api/v1/write", nil)
	r.Header.Set("Authorization", "Bearer sa-token")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusUnauthorized, w.Code)
}

// The agent binary is read under the namespace the call names.
func TestAgentBinaryRule(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/agent/binary", nil)
	ns, asks, ok := agentBinaryRule(r)
	require.True(t, ok)
	require.Equal(t, "default", ns)
	require.Equal(t, []Ask{{authz.VerbGet, authz.KindAgent}}, asks)
	r.Header.Set(NamespaceHeader, "tenant-a")
	ns, _, _ = agentBinaryRule(r)
	require.Equal(t, "tenant-a", ns)
}
