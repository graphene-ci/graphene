// Package httpapi is the plain-HTTP side of the door: the health probes
// (token-free — balancers and kubelets call them) and the docker
// registry proxy for agents. The business planes are gRPC (the door)
// and ConnectRPC (the browser port).
package httpapi

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/http/pprof"
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
	// Gate decides for the tokens Auth does not know (a service
	// account's, a person's) with the namespace's roles and bindings; nil
	// leaves the door to the configured and minted tokens alone.
	Gate Gate
	Log  *xlog.Logger
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
			}), agentBinaryRule, auth.RoleAgent, auth.RoleRun, auth.RoleAdmin))
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
			// Only the configured and minted tokens: the door does not
			// stamp the namespace on remote_write series, so a binding
			// would let its holder write any tenant's labels.
			mux.Handle("POST /api/v1/write", deps.requireRole(proxy, nil, auth.RoleRun, auth.RoleAdmin))
		}
	}
	if deps.RegistryUpstream != "" {
		proxy, err := registryProxy(deps.RegistryUpstream)
		if err != nil {
			deps.Log.Error("registry proxy disabled", xlog.Err(err))
		} else {
			// Agents pull worker images through here — the only registry
			// they know.
			mux.Handle("/v2/", deps.requireRole(proxy, registryRule, auth.RoleAgent, auth.RoleRun, auth.RoleAdmin))
		}
	}
	// The process's own profiles: a goroutine dump taken while the door
	// stalls says where it waits. A profile spans every tenant (a heap
	// dump holds their payloads), so only an installation-wide
	// administrator gets one — an admin of one namespace does not.
	profiles := http.NewServeMux()
	profiles.HandleFunc("/debug/pprof/", pprof.Index)
	profiles.HandleFunc("/debug/pprof/profile", pprof.Profile)
	profiles.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	profiles.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/", deps.requireRole(installationWide(profiles), nil, auth.RoleAdmin))
	return mux
}

// installationWide admits only a principal whose namespace is "*".
func installationWide(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p, ok := auth.FromContext(r.Context()); !ok || p.Namespace != "*" {
			http.Error(w, "an installation-wide administrator only", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requireRole admits a configured or minted token by its role; any other
// token — a service account's, a person's — is decided by the Gate with
// the route's rule, against the namespace's bindings. A route without a
// rule is for configured and minted tokens only.
func (d Deps) requireRole(next http.Handler, decide rule, roles ...auth.Role) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		// Docker daemons speak Basic after a challenge — the token rides
		// as the password, the username is ignored.
		if _, basic, ok := r.BasicAuth(); ok {
			token = basic
		}
		p, ok := d.Auth.Check(token)
		if !ok {
			d.gated(w, r, next, token, decide)
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

// gated answers a token the static authenticator does not know.
func (d Deps) gated(w http.ResponseWriter, r *http.Request, next http.Handler, token string, decide rule) {
	unknown := func() {
		// The challenge makes docker daemons retry WITH credentials.
		w.Header().Set("WWW-Authenticate", `Basic realm="graphene"`)
		http.Error(w, "invalid token", http.StatusUnauthorized)
	}
	if d.Gate == nil || decide == nil || token == "" {
		unknown()
		return
	}
	namespace, asks, ok := decide(r)
	authenticated, err := d.Gate.AuthorizeHTTP(r.Context(), token, namespace, asks)
	switch {
	case !authenticated:
		unknown()
	case !ok:
		http.Error(w, "this path is not open to a binding", http.StatusForbidden)
	case err != nil:
		http.Error(w, err.Error(), http.StatusForbidden)
	default:
		next.ServeHTTP(w, r)
	}
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
	}, ModifyResponse: func(res *http.Response) error {
		// Upload locations belong to the public door. Return a relative
		// reference so clients also work through TLS ingress and port forwards.
		location := res.Header.Get("Location")
		u, err := url.Parse(location)
		if err == nil && u.Host == target.Host {
			u.Scheme, u.Host = "", ""
			res.Header.Set("Location", u.String())
		}
		return nil
	}}
	return proxy, nil
}

// ensure errors import stays if future paths need it
