package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/config"
)

// The process's profiles are an administrator's: a run's or an agent's
// token is refused, a token nobody knows is challenged.
func TestProfilesAreForAdministrators(t *testing.T) {
	h := New(Deps{Auth: auth.New([]config.Token{
		{Token: "admin-token", Role: "admin", Namespace: "*"},
		{Token: "run-token", Role: "run", Namespace: "default"},
		{Token: "agent-token", Role: "agent", Namespace: "default"},
	})})
	get := func(token string) int {
		r := httptest.NewRequest(http.MethodGet, "/debug/pprof/goroutine?debug=1", nil)
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	require.Equal(t, http.StatusOK, get("admin-token"))
	require.Equal(t, http.StatusForbidden, get("run-token"))
	require.Equal(t, http.StatusForbidden, get("agent-token"))
	require.Equal(t, http.StatusUnauthorized, get(""))
}
