// Package managed is the server-side execution path: the run worker as a
// docker container next to the server. A run's container lives until the
// run is over AND the last resource served by the run's queue is gone —
// the run worker keeps serving the entities it created (their records
// live on its queue) even after the workflow returns.
package managed

import (
	"context"
	"fmt"
	"github.com/gopherex/xlog"
	"io"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
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

	mu   sync.Mutex
	runs map[id.RunId]string // run -> container id
}

// New builds the runner over the host's docker daemon; an installation
// without docker serves inplace runs only (Start returns the error).
func New(namespace string, temporal client.Client, externalGRPC, runToken string, log *xlog.Logger) *Runner {
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
		runs:         map[id.RunId]string{},
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
func (r *Runner) Start(ctx context.Context, runId id.RunId, imageRef string) error {
	if r.docker == nil {
		return fmt.Errorf("managed runs need docker on the server host")
	}
	r.mu.Lock()
	_, exists := r.runs[runId]
	r.mu.Unlock()
	if exists {
		return nil
	}
	if pull, err := r.docker.ImagePull(ctx, imageRef, image.PullOptions{}); err == nil {
		_, _ = io.Copy(io.Discard, pull)
		_ = pull.Close()
	} // a local image is fine — pull is best-effort
	env := []string{
		wire.EnvRole + "=run",
		wire.EnvAddress + "=" + r.externalGRPC,
		wire.EnvNamespace + "=" + r.namespace,
		wire.EnvRunId + "=" + string(runId),
		wire.EnvToken + "=" + r.runToken,
		wire.EnvImage + "=" + imageRef,
		// TODO(tls): drop once the door serves TLS.
		wire.EnvInsecure + "=1",
	}
	created, err := r.docker.ContainerCreate(ctx,
		&container.Config{Image: imageRef, Env: env},
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
	r.log.Info("managed run container started", xlog.Any("run", runId), xlog.String("image", imageRef))
	return nil
}

// Reap stops the containers of runs that are OVER: the run workflow is
// closed and nothing runs on the run's queue any more. Called on a tick.
func (r *Runner) Reap(ctx context.Context) {
	r.mu.Lock()
	snapshot := make(map[id.RunId]string, len(r.runs))
	for k, v := range r.runs {
		snapshot[k] = v
	}
	r.mu.Unlock()
	for runId, containerId := range snapshot {
		over, err := r.runOver(ctx, runId)
		if err != nil || !over {
			continue
		}
		if err := r.docker.ContainerRemove(ctx, containerId, container.RemoveOptions{Force: true}); err != nil {
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
