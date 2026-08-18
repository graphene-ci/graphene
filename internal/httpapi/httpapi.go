// Package httpapi is the HTTP side of the single door: the runs API for
// CLI/UI and the docker registry proxy for agents. Bearer tokens — the
// same credential space as the gRPC door.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gopherex/xlog"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/secrets"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// CapabilityPublisher writes a capability onto an agent's record.
type CapabilityPublisher interface {
	PublishCapability(ctx context.Context, agentId id.AgentId, capability pipeline.Capability) error
}

// RunLauncher starts the managed run container for a run.
type RunLauncher interface {
	Start(ctx context.Context, runId id.RunId, image string) error
}

// Deps is everything the HTTP API needs.
type Deps struct {
	Auth             *auth.Authenticator
	Temporal         client.Client
	RegistryUpstream string
	Secrets          secrets.Store
	BlobDir          string
	Capabilities     CapabilityPublisher
	Launcher         RunLauncher
	// Health serves the outside probes (/healthz/liveness, readiness).
	Health http.Handler
	Log    *xlog.Logger
}

// StartRunRequest asks the server to start a pipeline run. The worker —
// managed container or inplace binary — serves the run's queue; the
// server only starts the workflow.
type StartRunRequest struct {
	RunId id.RunId `json:"runId"`
	// Pipeline is the workflow type on the wire — the pipeline id.
	Pipeline string `json:"pipeline"`
	// Params is the typed params value of the pipeline, as JSON.
	Params json.RawMessage `json:"params"`
	// Image, when set, makes the run MANAGED: the server launches the
	// worker container itself; empty means an inplace worker serves the
	// queue.
	Image string `json:"image,omitempty"`
}

// StartRunResponse reports the started workflow.
type StartRunResponse struct {
	WorkflowId    string `json:"workflowId"`
	TemporalRunId string `json:"temporalRunId"`
}

// RunStatusResponse is the run's current status.
type RunStatusResponse struct {
	Status string `json:"status"`
}

// New builds the HTTP handler.
func New(deps Deps) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/runs", deps.requireRole(deps.startRun, auth.RoleRun, auth.RoleAdmin))
	mux.HandleFunc("GET /api/v1/runs/{runId}", deps.requireRole(deps.runStatus, auth.RoleRun, auth.RoleAdmin))
	// Liveness alias for the historical path; the real probes live under
	// /healthz/... — no token: balancers and kubelets call these.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	if deps.Health != nil {
		mux.Handle("/healthz/", deps.Health)
	}
	// Secrets resolve at the point of use, by name, with the worker's
	// token; the value goes out exactly once and never into records.
	mux.HandleFunc("GET /api/v1/secrets/{name}", deps.requireRole(deps.getSecret, auth.RoleRun, auth.RoleAdmin))
	// Blobs are content-addressed: PUT is idempotent by construction.
	mux.HandleFunc("PUT /api/v1/blobs/{location...}", deps.requireRole(deps.putBlob, auth.RoleRun, auth.RoleAdmin))
	mux.HandleFunc("GET /api/v1/blobs/{location...}", deps.requireRole(deps.getBlob, auth.RoleRun, auth.RoleAdmin))
	// Capabilities are published from wherever the installation happened.
	mux.HandleFunc("PUT /api/v1/agents/{agentId}/capabilities/{name}", deps.requireRole(deps.putCapability, auth.RoleRun, auth.RoleAdmin))
	if deps.RegistryUpstream != "" {
		proxy, err := registryProxy(deps.RegistryUpstream)
		if err != nil {
			deps.Log.Error("registry proxy disabled", xlog.Err(err))
		} else {
			// Agents pull worker images through here — the only registry
			// they know.
			mux.Handle("/v2/", deps.requireRoleHandler(proxy, auth.RoleAgent, auth.RoleRun, auth.RoleAdmin))
		}
	}
	return mux
}

func (d Deps) startRun(w http.ResponseWriter, r *http.Request) {
	var req StartRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	if err := req.RunId.Validate(); err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	if req.Pipeline == "" {
		httpError(w, http.StatusBadRequest, errors.New("pipeline is required"))
		return
	}
	opts := client.StartWorkflowOptions{
		ID:        "run/" + string(req.RunId),
		TaskQueue: wire.RunQueue(req.RunId),
		// A run id names ONE run: starting it twice attaches, never forks.
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}
	var args []any
	if len(req.Params) > 0 {
		args = append(args, req.Params)
	}
	run, err := d.Temporal.ExecuteWorkflow(r.Context(), opts, req.Pipeline, args...)
	if err != nil {
		httpError(w, http.StatusBadGateway, err)
		return
	}
	if req.Image != "" {
		if d.Launcher == nil {
			httpError(w, http.StatusNotImplemented, errors.New("managed runs are not enabled"))
			return
		}
		if err := d.Launcher.Start(r.Context(), req.RunId, req.Image); err != nil {
			httpError(w, http.StatusBadGateway, err)
			return
		}
	}
	d.Log.Info("run started", xlog.Any("run", req.RunId), xlog.String("pipeline", req.Pipeline), xlog.Bool("managed", req.Image != ""))
	writeJSON(w, StartRunResponse{WorkflowId: run.GetID(), TemporalRunId: run.GetRunID()})
}

func (d Deps) runStatus(w http.ResponseWriter, r *http.Request) {
	runId, err := id.ParseRunId(r.PathValue("runId"))
	if err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	desc, err := d.Temporal.DescribeWorkflowExecution(r.Context(), "run/"+string(runId), "")
	if err != nil {
		httpError(w, http.StatusNotFound, err)
		return
	}
	status := desc.GetWorkflowExecutionInfo().GetStatus().String()
	writeJSON(w, RunStatusResponse{Status: status})
}

func (d Deps) requireRole(next http.HandlerFunc, roles ...auth.Role) http.HandlerFunc {
	return d.requireRoleHandler(next, roles...).ServeHTTP
}

func (d Deps) requireRoleHandler(next http.Handler, roles ...auth.Role) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		p, ok := d.Auth.Check(token)
		if !ok {
			httpError(w, http.StatusUnauthorized, errors.New("invalid token"))
			return
		}
		for _, role := range roles {
			if p.Role == role {
				next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
				return
			}
		}
		httpError(w, http.StatusForbidden, fmt.Errorf("role %s may not call this", p.Role))
	})
}

func (d Deps) getSecret(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	value, err := d.Secrets.Get(id.SecretId(name))
	if err != nil {
		httpError(w, http.StatusNotFound, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte(value)) //nolint:gosec // plain-text secret value to an authenticated worker, not HTML
}

func (d Deps) putBlob(w http.ResponseWriter, r *http.Request) {
	location := path.Clean(r.PathValue("location"))
	if location == "." || !filepath.IsLocal(location) {
		httpError(w, http.StatusBadRequest, errors.New("bad blob location"))
		return
	}
	target := filepath.Join(d.BlobDir, filepath.FromSlash(location))
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		httpError(w, http.StatusInternalServerError, err)
		return
	}
	f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640) //nolint:gosec // confined above
	if err != nil {
		httpError(w, http.StatusInternalServerError, err)
		return
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(f, r.Body); err != nil {
		httpError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (d Deps) getBlob(w http.ResponseWriter, r *http.Request) {
	location := path.Clean(r.PathValue("location"))
	if location == "." || !filepath.IsLocal(location) {
		httpError(w, http.StatusBadRequest, errors.New("bad blob location"))
		return
	}
	http.ServeFile(w, r, filepath.Join(d.BlobDir, filepath.FromSlash(location)))
}

func (d Deps) putCapability(w http.ResponseWriter, r *http.Request) {
	agentId, err := id.ParseAgentId(r.PathValue("agentId"))
	if err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	var capability pipeline.Capability
	if err := json.NewDecoder(r.Body).Decode(&capability); err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	if capability.Name == "" || capability.Name != r.PathValue("name") {
		httpError(w, http.StatusBadRequest, errors.New("capability name mismatch"))
		return
	}
	if err := d.Capabilities.PublishCapability(r.Context(), agentId, capability); err != nil {
		httpError(w, http.StatusBadGateway, err)
		return
	}
	w.WriteHeader(http.StatusOK)
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

func httpError(w http.ResponseWriter, code int, err error) {
	http.Error(w, err.Error(), code)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
