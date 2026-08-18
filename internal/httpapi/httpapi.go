// Package httpapi is the HTTP side of the single door: the runs API for
// CLI/UI and the docker registry proxy for agents. Bearer tokens — the
// same credential space as the gRPC door.
package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Deps is everything the HTTP API needs.
type Deps struct {
	Auth             *auth.Authenticator
	Temporal         client.Client
	RegistryUpstream string
	Log              *slog.Logger
}

// StartRunRequest asks the server to start a pipeline run. The worker —
// managed container or inplace binary — serves the run's queue; the
// server only starts the workflow.
type StartRunRequest struct {
	RunId id.RunId `json:"runId"`
	// Workflow is the registered workflow type name of the pipeline (the
	// Go function name until the manifest mechanism lands).
	Workflow string `json:"workflow"`
	// Params is the typed params value of the pipeline, as JSON.
	Params json.RawMessage `json:"params"`
}

// StartRunResponse reports the started workflow.
type StartRunResponse struct {
	WorkflowId string `json:"workflowId"`
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
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	if deps.RegistryUpstream != "" {
		proxy, err := registryProxy(deps.RegistryUpstream)
		if err != nil {
			deps.Log.Error("registry proxy disabled", "error", err)
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
	if req.Workflow == "" {
		httpError(w, http.StatusBadRequest, errors.New("workflow is required"))
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
	run, err := d.Temporal.ExecuteWorkflow(r.Context(), opts, req.Workflow, args...)
	if err != nil {
		httpError(w, http.StatusBadGateway, err)
		return
	}
	d.Log.Info("run started", "run", req.RunId, "workflow", req.Workflow)
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
