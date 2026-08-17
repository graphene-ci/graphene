// Package pipeline is the library a pipeline author writes against. A
// pipeline is an ordinary Temporal workflow; this package adds what plain
// Temporal does not have: running functions on machines (OnAgent),
// one-shot actions with "at most once" semantics (Action), and reference
// types for secrets and blobs (see pkg/ref).
//
// The same user binary serves every execution site: the run worker
// (managed container or inplace process) and the per-(machine × run)
// container hosted by the agent. Dispatch is plain Temporal — the function
// must be a named, registered function, not a closure (closures do not
// serialize; enforced by lint).
package pipeline

import (
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/graphene/pkg/id"
	"github.com/graphene-ci/graphene/pkg/ref"
	"github.com/graphene-ci/graphene/pkg/wire"
)

// Re-exported reference types: what travels instead of values.
type (
	// SecretRef names a secret; the value is resolved at the point of use.
	SecretRef = ref.SecretRef
	// BlobRef points at bytes in external storage.
	BlobRef = ref.BlobRef
)

// RunId derives the current run id from the workflow ID.
func RunId(ctx workflow.Context) id.RunId {
	return id.RunId(workflow.GetInfo(ctx).WorkflowExecution.ID)
}

// OnAgentOptions tune a machine-bound call.
type OnAgentOptions struct {
	// Timeout bounds a single execution (start-to-close). Zero means 10m.
	Timeout time.Duration
	// HeartbeatTimeout distinguishes "still running" from "died". Zero
	// disables heartbeating.
	HeartbeatTimeout time.Duration
}

func (o *OnAgentOptions) defaults() {
	if o.Timeout == 0 {
		o.Timeout = 10 * time.Minute
	}
}

// OnAgent executes a registered function on the machine, inside the
// per-(machine × run) container hosted by the agent. It is an ordinary
// Temporal activity on the machine's queue: at-least-once, retried by
// policy — use it for converging operations that are idempotent by
// construction. fn must be a named registered function of the same
// binary.
func OnAgent(
	ctx workflow.Context,
	machineId id.MachineId,
	opts OnAgentOptions,
	fn any,
	args ...any,
) workflow.Future {
	opts.defaults()
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           wire.MachineRunQueue(machineId, RunId(ctx)),
		StartToCloseTimeout: opts.Timeout,
		HeartbeatTimeout:    opts.HeartbeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	})
	return workflow.ExecuteActivity(actx, fn, args...)
}

// ErrUnknown reports that a one-shot action was dispatched but its outcome
// could not be established: it may or may not have executed. There is no
// silent retry — the caller decides by policy (retry under a NEW action,
// ask a human, fail the run).
var ErrUnknown = errors.New("action outcome unknown")

// Action executes a registered function on the machine AT MOST ONCE:
// MaximumAttempts=1, no retries. A timeout or worker loss surfaces as
// ErrUnknown (wrapped), never as a re-execution. Use for one-shot work —
// a performance test, a migration; use OnAgent for converging work.
func Action(ctx workflow.Context, machineId id.MachineId, opts OnAgentOptions, fn any, args ...any) workflow.Future {
	opts.defaults()
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           wire.MachineRunQueue(machineId, RunId(ctx)),
		StartToCloseTimeout: opts.Timeout,
		HeartbeatTimeout:    opts.HeartbeatTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	f := workflow.ExecuteActivity(actx, fn, args...)
	return unknownClassifier{f}
}

// unknownClassifier wraps an at-most-once activity future, translating
// undeterminable outcomes (timeouts) into ErrUnknown.
type unknownClassifier struct {
	workflow.Future
}

// Get resolves the underlying future, classifying timeout-shaped failures
// as ErrUnknown.
func (u unknownClassifier) Get(ctx workflow.Context, valuePtr any) error {
	err := u.Future.Get(ctx, valuePtr)
	if err == nil {
		return nil
	}
	var timeout *temporal.TimeoutError
	if errors.As(err, &timeout) {
		return errors.Join(ErrUnknown, err)
	}
	return err
}
