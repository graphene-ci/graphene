package pipeline

import (
	"context"
	"time"

	"go.temporal.io/sdk/workflow"
)

// computeTimeout bounds one computation. Generous, because the whole point
// is that this is where the heavy thing is allowed to happen — but bounded,
// because an unbounded computation is indistinguishable from a hang.
const computeTimeout = 30 * time.Minute

// Do computes something in ordinary Go and remembers the answer.
//
// This is the only legitimate door for computation in a pipeline, and it
// exists because without it a person has nowhere to put one. Everything
// else in this package asks somebody else to do the work: Apply asks the
// cluster, On(...).Command asks a machine. Do is where the pipeline's own
// author gets to write a loop.
//
// It does NOT run in the workflow. Workflow code is replayed from history
// on every recovery and runs cooperatively on one logical thread — a heavy
// loop there times out its workflow task, is retried, and recomputes on
// every retry, hanging the pipeline on itself. Do runs beside it and its
// RESULT goes into the history, so a replay reads the answer instead of
// computing it again.
//
// Same bargain as a command: the function runs AT LEAST once. Make it
// idempotent, or do not put anything in it that must happen exactly once.
func Do[T any](run Run, memo string, fn func() (T, error)) T {
	ctx := workflow.WithLocalActivityOptions(run.s.ctx, workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: computeTimeout,
	})

	var out T

	future := workflow.ExecuteLocalActivity(ctx, func(context.Context) (T, error) {
		return fn()
	})

	if err := future.Get(ctx, &out); err != nil {
		run.raise("вычисление "+memo, err)
	}

	return out
}
