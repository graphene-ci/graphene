package agent

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/graphene-ci/graphene/sdk/agent"
)

// beatEvery is how often the agent says it is still here.
//
// A third of the lease: two marks may be lost to a hiccup before anything
// is declared silent. Marking exactly at the lease would make every late
// packet look like a dead machine.
const beatEvery = time.Duration(agent.LeaseSeconds/3) * time.Second

// Registration is the agent introducing itself and keeping that true.
type Registration struct {
	Temporal  client.Client
	Machine   string
	Namespace string
	Queue     string
	Facts     map[string]string
}

// Beat records the machine once.
//
// It goes through Temporal rather than through the API server: the agent
// has no access to the cluster and will not get any. A token that let a
// machine write records would be a key to the cluster handed out with
// every installation we ever perform.
func (r Registration) Beat(ctx context.Context) error {
	options := client.StartWorkflowOptions{
		ID:        "register-" + r.Machine,
		TaskQueue: agent.SystemQueue,
		// Each beat is its own short workflow, so a new one starting
		// while the previous is finished is exactly what should happen.
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	input := agent.RegisterInput{
		Machine:   r.Machine,
		Namespace: r.Namespace,
		Queue:     r.Queue,
		Facts:     r.Facts,
	}

	handle, err := r.Temporal.ExecuteWorkflow(ctx, options, agent.WorkflowRegister, input)
	if err != nil {
		return fmt.Errorf("машина не представилась: %w", err)
	}

	if err := handle.Get(ctx, nil); err != nil {
		return fmt.Errorf("запись машины не прошла: %w", err)
	}

	return nil
}

// Keep beats until the context is done.
//
// A failed beat is not fatal. The machine goes not-ready after the lease
// runs out, and the agent keeps trying — a control plane that was briefly
// away should find its fleet where it left it.
func (r Registration) Keep(ctx context.Context, report func(error)) {
	for {
		if err := r.Beat(ctx); err != nil && report != nil {
			report(err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(beatEvery):
		}
	}
}
