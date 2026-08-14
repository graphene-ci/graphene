package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"

	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
	"github.com/graphene-ci/graphene/sdk/pipeline"
)

// ServeRequest is one pipeline worker, run where the person is.
type ServeRequest struct {
	Kube      client.Client
	Namespace string
	// Pipeline names whose latest revision to serve. Revision pins one.
	Pipeline string
	Revision string
	// Dir holds the pipeline's code.
	Dir string
	// Where the worker connects and where machines will fetch the agent.
	Address      string
	Temporal     string
	Control      string
	AgentAddress string
	Out          io.Writer
}

// Serve runs the pipeline's worker HERE — on the machine of whoever typed
// the command, not in the control plane.
//
// This is the whole point and it is worth saying plainly: a pipeline is
// arbitrary code written by anybody, and the control plane must not run it.
// It runs where its author is, and the control plane only ever hands it
// work and takes back results.
//
// The cost is honest and belongs in the open: while nothing serves a
// revision, runs of it do not move. Their history is intact and they
// continue when a worker returns — but "the run goes on while everyone
// sleeps" stops being true for steps the pipeline computes itself. Steps on
// machines are unaffected: agents run those.
func Serve(ctx context.Context, req ServeRequest) error {
	revision, err := revisionOf(ctx, req)
	if err != nil {
		return err
	}

	// Каталог называет тот, кто набрал команду, у себя на машине и своим
	// же кодом. Проверять здесь нечего: это и есть работа команды.
	//nolint:gosec // запустить названный каталог — это и есть смысл serve
	cmd := exec.CommandContext(ctx, "go", "run", req.Dir)
	cmd.Stdout = req.Out
	cmd.Stderr = req.Out
	cmd.Env = append(os.Environ(),
		pipeline.EnvAddress+"="+req.Temporal,
		pipeline.EnvNamespace+"="+req.Address,
		pipeline.EnvQueue+"="+revision.Spec.Queue,
		pipeline.EnvRevision+"="+revision.Name,
		pipeline.EnvRecords+"="+req.Namespace,
		pipeline.EnvControl+"="+req.Control,
		pipeline.EnvAgentAddress+"="+req.AgentAddress,
	)

	fmt.Fprintf(req.Out, "ревизия %s, очередь %s\n", revision.Name, revision.Spec.Queue)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("воркер пайплайна остановился: %w", err)
	}

	return nil
}

// revisionOf finds which revision this worker answers for.
func revisionOf(ctx context.Context, req ServeRequest) (*v1.PipelineRevision, error) {
	name := req.Revision

	if name == "" {
		var found v1.Pipeline

		key := client.ObjectKey{Namespace: req.Namespace, Name: req.Pipeline}
		if err := req.Kube.Get(ctx, key, &found); err != nil {
			return nil, fmt.Errorf("пайплайн не читается: %w", err)
		}

		name = found.Status.LatestRevision
		if name == "" {
			return nil, fmt.Errorf("%w: %s", ErrNoRevision, req.Pipeline)
		}
	}

	var revision v1.PipelineRevision

	key := client.ObjectKey{Namespace: req.Namespace, Name: name}
	if err := req.Kube.Get(ctx, key, &revision); err != nil {
		return nil, fmt.Errorf("ревизия не читается: %w", err)
	}

	return &revision, nil
}
