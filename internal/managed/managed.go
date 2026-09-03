// Package managed is the server-side execution path: the run worker as a
// docker container next to the server. A run's container lives until the
// run is over AND the last resource served by the run's queue is gone —
// the run worker keeps serving the entities it created (their records
// live on its queue) even after the workflow returns.
package managed

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/gopherex/xlog"
	"io"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/registry"
	dockerclient "github.com/docker/docker/client"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/graphene-ci/graphene/internal/probes"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Runner launches and reaps managed run containers of ONE namespace.
type Runner struct {
	namespace string
	docker    *dockerclient.Client
	temporal  client.Client
	log       *xlog.Logger

	// wiring handed to every run container.
	externalGRPC string
	runToken     string

	// sink receives tailed container output; nil disables tailing.
	sink LogSink

	mu    sync.Mutex
	runs  map[id.RunId]string           // run -> container id
	tails map[string]context.CancelFunc // container id -> tail stop
}

// New builds the runner over the host's docker daemon; an installation
// without docker serves inplace runs only (Start returns the error).
func New(namespace string, temporal client.Client, externalGRPC, runToken string, sink LogSink, log *xlog.Logger) *Runner {
	docker, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		log.Warn("managed contour disabled: no docker", xlog.Err(err))
		docker = nil
	}
	return &Runner{
		namespace:    namespace,
		docker:       docker,
		temporal:     temporal,
		log:          log,
		externalGRPC: externalGRPC,
		runToken:     runToken,
		sink:         sink,
		runs:         map[id.RunId]string{},
		tails:        map[string]context.CancelFunc{},
	}
}

// Ping reports the docker daemon's reachability for the health probes;
// probes.ErrDisabled when the managed contour is off.
func (r *Runner) Ping(ctx context.Context) error {
	if r.docker == nil {
		return probes.ErrDisabled
	}
	_, err := r.docker.Ping(ctx)
	return err
}

// Start launches the run worker container for a run.
// Start launches the run's orchestrator container. runToken is the
// run's OWN token — minted for this run, scoped to it, and dying with
// it; the installation-wide token is only the fallback of a server
// without a signing key.
func (r *Runner) Start(ctx context.Context, runId id.RunId, imageRef, runToken string) error {
	if runToken == "" {
		runToken = r.runToken
	}
	if r.docker == nil {
		return fmt.Errorf("managed runs need docker on the server host")
	}
	r.mu.Lock()
	_, exists := r.runs[runId]
	r.mu.Unlock()
	if exists {
		return nil
	}
	// The image lives behind our own /v2 door: the daemon authenticates
	// with the run token as the Basic password (the door challenges,
	// the daemon retries with credentials).
	authJSON, _ := json.Marshal(registry.AuthConfig{Username: "token", Password: runToken}) //nolint:gosec // registry credentials travel to the local docker daemon only
	pullOpts := image.PullOptions{RegistryAuth: base64.URLEncoding.EncodeToString(authJSON)}
	if pull, err := r.docker.ImagePull(ctx, imageRef, pullOpts); err == nil {
		_, _ = io.Copy(io.Discard, pull)
		_ = pull.Close()
	} // a local image is fine — pull is best-effort
	env := []string{
		wire.EnvRole + "=run",
		wire.EnvAddress + "=" + r.externalGRPC,
		wire.EnvNamespace + "=" + r.namespace,
		wire.EnvRunId + "=" + string(runId),
		wire.EnvToken + "=" + runToken,
		wire.EnvImage + "=" + imageRef,
		// TODO(tls): drop once the door serves TLS.
		wire.EnvInsecure + "=1",
	}
	created, err := r.docker.ContainerCreate(ctx,
		&container.Config{
			Image: imageRef,
			Env:   env,
			// The labels ARE the tracking: the reaper lists them from the
			// daemon, so run containers survive server restarts without
			// becoming orphans.
			Labels: map[string]string{
				labelNamespace: r.namespace,
				labelRun:       string(runId),
			},
		},
		&container.HostConfig{
			NetworkMode:   "host",
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyUnlessStopped},
		}, nil, nil, "graphene-run-"+sanitize(r.namespace)+"-"+sanitize(string(runId)))
	if err != nil {
		return fmt.Errorf("create run container: %w", err)
	}
	if err := r.docker.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("start run container: %w", err)
	}
	r.mu.Lock()
	r.runs[runId] = created.ID
	r.mu.Unlock()
	// The orchestrator's stdout is dimension 3 of the run — tail it
	// into the telemetry plane. Detached from the caller's ctx: the
	// tail lives with the container, not with the StartRun call.
	r.startTail(context.WithoutCancel(ctx), created.ID, runId)
	r.log.Info("managed run container started", xlog.Any("run", runId), xlog.String("image", imageRef))
	return nil
}

// Container labels the reaper tracks runs by.
const (
	labelNamespace = "graphene.io/namespace"
	labelRun       = "graphene.io/run"
)

// Ensure is the resurrection half of the lifecycle: live records on a
// run's queue with no serving container get their worker back. An
// existing container is adopted — running gets its tail reattached,
// stopped is started; a missing one is created the same way Start
// creates it. Idempotent, cheap when the worker is already alive.
func (r *Runner) Ensure(ctx context.Context, runId id.RunId, imageRef, runToken string) (revived bool, err error) {
	if r.docker == nil {
		return false, fmt.Errorf("managed runs need docker on the server host")
	}
	list, err := r.docker.ContainerList(ctx, container.ListOptions{
		All: true,
		Filters: filters.NewArgs(
			filters.Arg("label", labelNamespace+"="+r.namespace),
			filters.Arg("label", labelRun+"="+string(runId)),
		),
	})
	if err != nil {
		return false, fmt.Errorf("ensure run container: %w", err)
	}
	if len(list) > 0 {
		c := list[0]
		if c.State == "running" {
			r.adopt(ctx, c.ID, runId)
			return false, nil
		}
		if err := r.docker.ContainerStart(ctx, c.ID, container.StartOptions{}); err != nil {
			return false, fmt.Errorf("restart run container: %w", err)
		}
		r.adopt(ctx, c.ID, runId)
		r.log.Info("managed run container resurrected", xlog.Any("run", runId))
		return true, nil
	}
	// No container at all — the create path is Start's.
	r.mu.Lock()
	delete(r.runs, runId) // stale memory must not short-circuit Start
	r.mu.Unlock()
	if err := r.Start(ctx, runId, imageRef, runToken); err != nil {
		return false, err
	}
	r.log.Info("managed run container resurrected", xlog.Any("run", runId))
	return true, nil
}

// adopt takes an already-existing container under management: tracked
// for the reaper, its stdout tailed. startTail is a no-op for a
// container already tailed.
func (r *Runner) adopt(ctx context.Context, containerId string, runId id.RunId) {
	r.mu.Lock()
	r.runs[runId] = containerId
	r.mu.Unlock()
	r.startTail(context.WithoutCancel(ctx), containerId, runId)
}

// Reap stops the containers of runs that are OVER: the run workflow is
// closed and nothing runs on the run's queue any more. Called on a
// tick. The candidates come from the DAEMON by label, not from process
// memory — a server restart leaves no orphans.
func (r *Runner) Reap(ctx context.Context) {
	if r.docker == nil {
		return
	}
	list, err := r.docker.ContainerList(ctx, container.ListOptions{
		All: true,
		Filters: filters.NewArgs(
			filters.Arg("label", labelNamespace+"="+r.namespace),
		),
	})
	if err != nil {
		r.log.Error("reap: list run containers", xlog.Err(err))
		return
	}
	for _, c := range list {
		runId := id.RunId(c.Labels[labelRun])
		if runId == "" {
			continue
		}
		over, err := r.runOver(ctx, runId)
		if err != nil || !over {
			// A living container the runner does not tail yet (found
			// after a server restart) gets its tail reattached here.
			if c.State == "running" {
				r.startTail(context.WithoutCancel(ctx), c.ID, runId)
			}
			continue
		}
		r.stopTail(c.ID)
		if err := r.docker.ContainerRemove(ctx, c.ID, container.RemoveOptions{Force: true}); err != nil {
			r.log.Error("reap run container", xlog.Any("run", runId), xlog.Err(err))
			continue
		}
		r.mu.Lock()
		delete(r.runs, runId)
		r.mu.Unlock()
		r.log.Info("managed run container reaped", xlog.Any("run", runId))
	}
}

// runOver: the run workflow is closed AND no workflow is running on the
// run's task queue — the run worker serves nothing any more.
func (r *Runner) runOver(ctx context.Context, runId id.RunId) (bool, error) {
	desc, err := r.temporal.DescribeWorkflowExecution(ctx, "run/"+string(runId), "")
	if err == nil && desc.GetWorkflowExecutionInfo().GetStatus() == enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return false, nil
	}
	query := fmt.Sprintf("TaskQueue = '%s' AND ExecutionStatus = 'Running'", wire.RunQueue(runId))
	resp, err := r.temporal.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{Query: query})
	if err != nil {
		return false, err
	}
	return resp.GetCount() == 0, nil
}

// Tick runs the reaper until ctx ends.
func (r *Runner) Tick(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.Reap(ctx)
		}
	}
}

func sanitize(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.', r == '_':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}
