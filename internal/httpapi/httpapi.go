// Package httpapi is the plain-HTTP side of the door: the health probes
// (token-free — balancers and kubelets call them) and the docker
// registry proxy for agents. The business planes are gRPC (the door)
// and ConnectRPC (the browser port).
package httpapi

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/gopherex/xlog"

	"github.com/graphene-ci/graphene/internal/auth"
)

// Deps is everything the HTTP door needs.
type Deps struct {
	Auth             *auth.Authenticator
	RegistryUpstream string
	// Health serves /healthz/liveness and /healthz/readiness.
	Health http.Handler
	Log    *xlog.Logger
}

// New builds the HTTP handler.
func New(deps Deps) http.Handler {
	mux := http.NewServeMux()
	// Liveness alias for the historical path; the real probes live under
	// /healthz/...
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	if deps.Health != nil {
		mux.Handle("/healthz/", deps.Health)
	}
	if deps.RegistryUpstream != "" {
		proxy, err := registryProxy(deps.RegistryUpstream)
		if err != nil {
			deps.Log.Error("registry proxy disabled", xlog.Err(err))
		} else {
			// Agents pull worker images through here — the only registry
			// they know.
			mux.Handle("/v2/", deps.requireRole(proxy, auth.RoleAgent, auth.RoleRun, auth.RoleAdmin))
		}
	}
	return mux
}

func (d Deps) requireRole(next http.Handler, roles ...auth.Role) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		// Docker daemons speak Basic after a challenge — the token rides
		// as the password, the username is ignored.
		if _, basic, ok := r.BasicAuth(); ok {
			token = basic
		}
		p, ok := d.Auth.Check(token)
		if !ok {
			// The challenge makes docker daemons retry WITH credentials.
			w.Header().Set("WWW-Authenticate", `Basic realm="graphene"`)
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}
		for _, role := range roles {
			if p.Role == role {
				next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
				return
			}
		}
		http.Error(w, fmt.Sprintf("role %s may not call this", p.Role), http.StatusForbidden)
	})
}

func registryProxy(upstream string) (http.Handler, error) {
	target, err := url.Parse(upstream)
	if err != nil {
		return nil, fmt.Errorf("registry upstream: %w", err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	director := proxy.Director
	proxy.Director = func(r *http.Request) {
		director(r)
		// The graphene token authenticated the puller at our door; the
		// upstream registry has its own auth (none in the dev contour).
		r.Header.Del("Authorization")
	}
	return proxy, nil
}

// ensure errors import stays if future paths need it
