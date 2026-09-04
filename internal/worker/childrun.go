package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"go.temporal.io/sdk/activity"

	syslabels "github.com/graphene-ci/graphene/internal/labels"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// maxChildRunDepth bounds how deep run-owns-run may nest — an
// anti-fork-bomb guard so a pipeline that starts children which start
// children cannot recurse without limit.
const maxChildRunDepth = 5

// startChildRun starts (or attaches to) a CHILD run under the calling run.
// The parent is the activity's OWN workflow id — provably the run that
// asked — never trusted from the request; the child's EntityOwner is set to
// it, which puts the child in the parent's tree and under its cancel
// cascade. The child runs its OWN pipeline's active image (not the
// parent's). Idempotent by child run id: a parent replay re-attaches to the
// live child instead of forking a second.
func (s *Worker) startChildRun(ctx context.Context, req wire.StartChildRunRequest) (string, error) {
	if s.startRun == nil {
		return "", fmt.Errorf("run starter is not wired yet")
	}
	parent := activity.GetInfo(ctx).WorkflowExecution.ID
	if !strings.HasPrefix(parent, "run/") {
		return "", fmt.Errorf("a child run may only be started by a run, not by %q", parent)
	}
	if req.RunId == "" || req.Pipeline == "" {
		return "", fmt.Errorf("child run needs a run id and a pipeline")
	}
	// Anti-fork-bomb: refuse to nest runs beyond the depth limit.
	if depth, err := s.runDepth(ctx, parent); err != nil {
		return "", fmt.Errorf("child run depth: %w", err)
	} else if depth >= maxChildRunDepth {
		return "", fmt.Errorf("child run would nest to depth %d, over the limit of %d", depth+1, maxChildRunDepth)
	}
	// The child runs its OWN pipeline's ACTIVE image — the version the
	// server resynced, not the parent's revision.
	st, err := s.GetPipeline(ctx, req.Pipeline)
	if err != nil {
		return "", fmt.Errorf("child pipeline %s: %w", req.Pipeline, err)
	}
	if st.Image == "" {
		return "", fmt.Errorf("child pipeline %s has no active image — materialize and activate a revision first", req.Pipeline)
	}
	if err := s.startRun(ctx, req.RunId, req.Pipeline, req.Params, st.Image, req.Labels, syslabels.TriggerChild, parent); err != nil {
		return "", err
	}
	return req.RunId, nil
}

// awaitChildRun blocks until the named child run reaches a terminal state
// and returns its typed result as JSON. A failed or cancelled child comes
// back as an error (the parent handle's Ready then surfaces it). Heartbeats
// while waiting, so a worker restart re-attaches to the still-running child
// instead of the whole await failing. The caller must OWN the child.
func (s *Worker) awaitChildRun(ctx context.Context, req wire.AwaitChildRunRequest) (json.RawMessage, error) {
	if req.RunId == "" {
		return nil, fmt.Errorf("await needs a child run id")
	}
	childWorkflowId := "run/" + req.RunId
	parent := activity.GetInfo(ctx).WorkflowExecution.ID
	owner, err := s.ownerOf(ctx, childWorkflowId)
	if err != nil {
		return nil, fmt.Errorf("child %s: %w", req.RunId, err)
	}
	if owner != parent {
		return nil, fmt.Errorf("run %q may not await %q (owned by %q)", parent, childWorkflowId, owner)
	}
	// Wait for the child to close (heartbeating inside), then read its
	// result — GetWorkflow surfaces a failed/cancelled child as an error.
	if err := awaitClosed(ctx, s.deps.Client, childWorkflowId); err != nil {
		return nil, err
	}
	var out json.RawMessage
	if err := s.deps.Client.GetWorkflow(ctx, childWorkflowId, "").Get(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// runDepth counts how many run ancestors a run has by climbing the
// EntityOwner chain: a top-level run (owned by its pipeline) is depth 0.
func (s *Worker) runDepth(ctx context.Context, runWorkflowId string) (int, error) {
	depth := 0
	cur := runWorkflowId
	for i := 0; i <= maxChildRunDepth+1; i++ {
		owner, err := s.ownerOf(ctx, cur)
		if err != nil {
			return depth, err
		}
		if !strings.HasPrefix(owner, "run/") {
			return depth, nil // owner is a pipeline (or none): the top of the run chain
		}
		depth++
		cur = owner
	}
	return depth, nil
}
