package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	syslabels "github.com/graphene-ci/graphene/internal/labels"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// maxChildRunDepth bounds how deep run-owns-run may nest — an
// anti-fork-bomb guard so a pipeline that starts children which start
// children cannot recurse without limit.
const maxChildRunDepth = 5

// childPoll is how often awaitChildRun re-checks a running child (each
// tick also heartbeats, so a worker restart re-attaches instead of the
// await failing).
const childPoll = 2 * time.Second

// terminal classifies an error as NON-RETRYABLE: a child's own outcome and
// a caller's own mistake are permanent, so the await/start activity must
// fail ONCE and hand the outcome to the parent — never retry forever. The
// whole point is the invariant "no wait is ever infinite": without this a
// failed/cancelled child (whose GetWorkflow error is retryable by default)
// would loop until the activity's own timeout, pinning the fan-out
// semaphore and starving siblings.
func terminal(kind, msg string, cause error) error {
	return temporal.NewNonRetryableApplicationError(msg, kind, cause)
}

// permanentStart reports whether a start error is the caller's fault (bad
// params, unknown pipeline, policy) — retrying it never helps — versus a
// transient infrastructure error that should retry.
func permanentStart(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.NotFound,
		codes.AlreadyExists, codes.PermissionDenied, codes.Unauthenticated:
		return true
	default:
		return false
	}
}

// startChildRun starts (or attaches to) a CHILD run under the calling run.
// The parent is the activity's OWN workflow id — provably the run that
// asked — never trusted from the request; the child's EntityOwner is set to
// it, which puts the child in the parent's tree and under its cancel
// cascade. The child runs its OWN pipeline's active image (not the
// parent's). Idempotent by child run id: a parent replay re-attaches to the
// live child instead of forking a second.
//
// Every rejection that retrying cannot fix is returned NON-RETRYABLE, so a
// bad child declaration fails the handle at once instead of looping.
func (s *Worker) startChildRun(ctx context.Context, req wire.StartChildRunRequest) (string, error) {
	if s.startRun == nil {
		return "", fmt.Errorf("run starter is not wired yet") // infra: retryable (wiring races startup)
	}
	parent := activity.GetInfo(ctx).WorkflowExecution.ID
	if !strings.HasPrefix(parent, "run/") {
		return "", terminal("NotARun", fmt.Sprintf("a child run may only be started by a run, not by %q", parent), nil)
	}
	if req.RunId == "" || req.Pipeline == "" {
		return "", terminal("InvalidChildRun", "child run needs a run id and a pipeline", nil)
	}
	// Anti-fork-bomb: refuse to nest runs beyond the depth limit.
	depth, err := s.runDepth(ctx, parent)
	if err != nil {
		return "", err // reading the owner chain is infra: retryable
	}
	if depth >= maxChildRunDepth {
		return "", terminal("ChildDepthExceeded",
			fmt.Sprintf("child run would nest to depth %d, over the limit of %d", depth+1, maxChildRunDepth), nil)
	}
	// The child runs its OWN pipeline's ACTIVE image — the version the
	// server resynced, not the parent's revision.
	st, err := s.GetPipeline(ctx, req.Pipeline)
	if err != nil {
		return "", err // describe is infra: retryable
	}
	if st.Image == "" {
		return "", terminal("ChildNoImage",
			fmt.Sprintf("child pipeline %s has no active image — materialize and activate a revision first", req.Pipeline), nil)
	}
	if err := s.startRun(ctx, req.RunId, req.Pipeline, req.Params, st.Image, req.Labels, syslabels.TriggerChild, parent); err != nil {
		if permanentStart(err) {
			return "", terminal("ChildStartRejected", fmt.Sprintf("start child %s: %v", req.RunId, err), err)
		}
		return "", err // transient: retryable
	}
	return req.RunId, nil
}

// awaitChildRun blocks until the named child run reaches a terminal state
// and returns its typed result as JSON. It CLASSIFIES the outcome so the
// activity never retries a settled child: a failed/cancelled/terminated
// child, or a gone one, comes back as a NON-RETRYABLE error the parent's
// handle surfaces; only genuine infrastructure faults (a transient describe
// error) stay retryable. Heartbeats while the child runs, so a worker
// restart re-attaches instead of the await failing. The caller must OWN the
// child.
func (s *Worker) awaitChildRun(ctx context.Context, req wire.AwaitChildRunRequest) (json.RawMessage, error) {
	if req.RunId == "" {
		return nil, terminal("InvalidAwait", "await needs a child run id", nil)
	}
	childWorkflowId := "run/" + req.RunId
	parent := activity.GetInfo(ctx).WorkflowExecution.ID
	owner, err := s.ownerOf(ctx, childWorkflowId)
	if err != nil {
		if alreadyGone(err) {
			return nil, terminal("ChildGone", fmt.Sprintf("child %s is gone", req.RunId), err)
		}
		return nil, err // transient describe: retryable
	}
	if owner != parent {
		return nil, terminal("NotOwner",
			fmt.Sprintf("run %q may not await %q (owned by %q)", parent, childWorkflowId, owner), nil)
	}
	for {
		desc, err := s.deps.Client.DescribeWorkflowExecution(ctx, childWorkflowId, "")
		if err != nil {
			if alreadyGone(err) {
				// Closed and already swept: terminal, no result to read.
				return nil, terminal("ChildGone", fmt.Sprintf("child %s is gone", req.RunId), err)
			}
			return nil, err // transient: retryable
		}
		switch desc.GetWorkflowExecutionInfo().GetStatus() {
		case enums.WORKFLOW_EXECUTION_STATUS_RUNNING:
			activity.RecordHeartbeat(ctx, "awaiting child "+childWorkflowId)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(childPoll):
			}
			continue
		case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:
			var out json.RawMessage
			if err := s.deps.Client.GetWorkflow(ctx, childWorkflowId, "").Get(ctx, &out); err != nil {
				return nil, err // completed but result momentarily unreadable: retryable
			}
			return out, nil
		default: // CANCELED, FAILED, TERMINATED, TIMED_OUT, CONTINUED_AS_NEW
			return nil, childTerminalError(desc.GetWorkflowExecutionInfo().GetStatus(), req.RunId)
		}
	}
}

// childTerminalError maps a child's non-Completed terminal status to the
// NON-RETRYABLE error the parent's handle receives. Pure, so the "a failed
// child fails the wait in one attempt" invariant is unit-tested without a
// Temporal environment.
func childTerminalError(st enums.WorkflowExecutionStatus, runId string) error {
	if st == enums.WORKFLOW_EXECUTION_STATUS_CANCELED {
		return terminal("ChildCancelled", "child run "+runId+" was cancelled", nil)
	}
	return terminal("ChildFailed", fmt.Sprintf("child run %s ended %s", runId, st), nil)
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
