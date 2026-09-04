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
	"os"
	"strings"

	"github.com/gopherex/xlog"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/nsbundle"
	"github.com/graphene-ci/graphene/internal/secrets"
)

// agentBinaryPath is where the image carries the agent binary.
const agentBinaryPath = "/agent/graphene-agent"

// Deps is everything the HTTP door needs.
type Deps struct {
	Auth             *auth.Authenticator
	RegistryUpstream string
	// MetricsUpstream is the metrics store's base URL; when set, the door
	// accepts Prometheus remote_write at /api/v1/write (authenticated)
	// and forwards it there. This is how an agent-side vmagent ships a
	// machine's scraped metrics into the installation's obs.
	MetricsUpstream string
	// Health serves /healthz/liveness and /healthz/readiness.
	Health http.Handler
	// Bundles and Secrets serve the webhook door (/hooks/...); nil
	// disables it.
	Bundles *nsbundle.Manager
	Secrets *secrets.Namespaced
	Log     *xlog.Logger
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
	// The agent binary machines install: the ssh script and user-data
	// download it from the same door they will dial.
	if _, err := os.Stat(agentBinaryPath); err == nil {
		mux.Handle("GET /agent/binary", deps.requireRole(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.ServeFile(w, r, agentBinaryPath)
			}), auth.RoleAgent, auth.RoleRun, auth.RoleAdmin))
	}
	if deps.Bundles != nil && deps.Secrets != nil {
		// The webhook door: authenticated by the trigger's own secret,
		// not the installation's tokens.
		mux.HandleFunc("POST /hooks/{ns}/{pipeline}/{trigger}", deps.hooks)
	}
	if deps.MetricsUpstream != "" {
		proxy, err := remoteWriteProxy(deps.MetricsUpstream)
		if err != nil {
			deps.Log.Error("remote_write receiver disabled", xlog.Err(err))
		} else {
			// A machine's vmagent ships scraped metrics here with its run
			// token; the door forwards them to the metrics store. The
			// series' own labels (vmagent external_labels — graphene entity/
			// run) carry the correlation.
			mux.Handle("POST /api/v1/write", deps.requireRole(proxy, auth.RoleRun, auth.RoleAdmin))
		}
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

// remoteWriteProxy forwards Prometheus remote_write to the metrics
// store's /api/v1/write. The graphene token authenticated the sender at
// our door; the store (internal to the compose network) has its own
// auth (none in the dev contour), so the header is dropped.
func remoteWriteProxy(upstream string) (http.Handler, error) {
	target, err := url.Parse(upstream)
	if err != nil {
		return nil, fmt.Errorf("metrics upstream: %w", err)
	}
	proxy := &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) {
		r.SetURL(target)
		r.Out.URL.Path = "/api/v1/write"
		r.Out.Header.Del("Authorization")
	}}
	return proxy, nil
}

func registryProxy(upstream string) (http.Handler, error) {
	target, err := url.Parse(upstream)
	if err != nil {
		return nil, fmt.Errorf("registry upstream: %w", err)
	}
	proxy := &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) {
		r.SetURL(target)
		// The graphene token authenticated the puller at our door; the
		// upstream registry has its own auth (none in the dev contour).
		r.Out.Header.Del("Authorization")
	}}
	return proxy, nil
}

// ensure errors import stays if future paths need it
