package worker

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/graphene/pkg/agent"
)

// registerTimeout bounds one write of a machine's record. It is a couple of
// writes to the API server; a minute is already generous.
const registerTimeout = time.Minute

// RegisterMachine is how an agent gets into the cluster.
//
// It is a workflow rather than a bare activity because an activity has to be
// scheduled by something, and the agent is not a workflow — it is a worker.
// So the agent starts this, and this schedules the write onto our own queue,
// where the permissions to do it live.
//
// It is deliberately tiny and it completes at once: every heartbeat is one
// of these. Temporal already knows who polls a task queue, and asking IT
// would remove this whole path — worth doing when readiness and selection
// are looked at properly, which is M5.
func RegisterMachine(ctx workflow.Context, req agent.RegisterInput) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           agent.SystemQueue,
		StartToCloseTimeout: registerTimeout,
	})

	if err := workflow.ExecuteActivity(ctx, agent.ActivityRegister, req).Get(ctx, nil); err != nil {
		return fmt.Errorf("машина не записалась: %w", err)
	}

	return nil
}
