package pipeline

import (
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/graphene/sdk/agent"
)

// Match describes the machines a pipeline wants, by what they have rather
// than by what they are called.
type Match = agent.Match

// Claim takes machines out of the pool for this run.
//
// It asks by description — "linux, has docker, tolerates being dedicated"
// — because naming a machine means knowing the fleet, and the point of a
// fleet is that nobody has to.
//
// Waiting is free and needs no code: when the pool is short, the activity
// fails and Temporal retries it with backoff, so a run that wants three
// machines simply waits until three exist. That is the whole of the
// queueing, and there is no scheduler anywhere.
//
// What comes back is targets, the same kind of thing pipeline.Install
// returns: a queue to put steps into. A claimed machine already has an
// agent, so there is nothing to install and nothing to wait for.
func Claim(run Run, memo string, count int, match Match) []Target {
	in := agent.ClaimInput{
		Owner: run.s.owner,
		Memo:  memo,
		Count: count,
		Match: match,
	}

	var out agent.ClaimOutput
	if err := workflow.ExecuteActivity(run.s.ctx, agent.ActivityClaim, in).Get(run.s.ctx, &out); err != nil {
		run.raise("захват "+memo, err)
	}

	targets := make([]Target, 0, len(out.Machines))
	for _, machine := range out.Machines {
		// Очередь машины — это очередь её установки, а имя установки и
		// есть имя машины: агент представился под ним. Ставить шаг
		// можно сразу.
		targets = append(targets, Target{installation: machine})
	}

	return targets
}
