package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"os"

	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/graphene/pkg/agent"
)

// EnvControl is where a machine fetches the agent binary from. The
// operator sets it on the pipeline's worker; the worker passes it into
// every install script it produces.
const EnvControl = "GRAPHENE_CONTROL"

// Target is a machine this run will have an agent on.
//
// It exists from the moment it is named, before any machine does. Its queue
// follows from its name, and a step put into that queue simply waits there
// until an agent comes up to read it — the queue IS the waiting. That is
// why nothing in a pipeline ever asks whether the agent has arrived.
type Target struct {
	installation string
	install      agent.Install
}

// Queue is where steps for this target go.
func (t Target) Queue() string { return agent.InstallationQueue(t.installation) }

// Script is what puts the agent on a machine that already exists.
func (t Target) Script() string { return t.install.Script() }

// CloudInit is the same thing for a machine that does not exist yet — it
// goes into the provider's own field, whatever that provider calls it.
func (t Target) CloudInit() string { return t.install.CloudInit() }

// Install names a machine this run will put an agent on.
//
// The installation's name carries the run's, so two runs never share a
// queue: a step of one must never reach the agent of the other, and
// reinstalling on the same iron must give a new queue rather than inherit
// a zombie's.
func Install(run Run, name string) Target {
	installation := run.s.owner.Name + "-" + name

	return Target{
		installation: installation,
		install: agent.Install{
			Control:   os.Getenv(EnvControl),
			Address:   os.Getenv(EnvAddress),
			Namespace: os.Getenv(EnvNamespace),
			Records:   run.s.owner.Namespace,
			Machine:   installation,
			Token:     token(run, installation),
		},
	}
}

// token proves an installation was asked for.
//
// Derived rather than random, because the script must be a pure function of
// the installation: a random token would make two identical asks produce
// different bytes, and anything carrying the script — a VM's user-data —
// would stop being idempotent.
//
// See agent.Install.Token for the hole this leaves and when it closes.
func token(run Run, installation string) string {
	sum := sha256.Sum256([]byte(run.s.owner.UID + "/" + installation))

	return hex.EncodeToString(sum[:])
}

// Step is a machine to run something on.
type Step struct {
	run    Run
	target Target
}

// On points at the machine the next step runs on.
func On(run Run, target Target) Step {
	return Step{run: run, target: target}
}

// Command runs one command on the machine and returns what it said.
//
// A non-zero exit code comes back as a value rather than as the end of the
// pipeline: the command ran and said no, and whether that ends the run is
// the pipeline's decision, not ours.
func (s Step) Command(memo string, req agent.ExecInput) agent.ExecOutput {
	ctx := workflow.WithTaskQueue(s.run.s.ctx, s.target.Queue())

	var out agent.ExecOutput
	if err := workflow.ExecuteActivity(ctx, agent.ActivityExec, req).Get(ctx, &out); err != nil {
		fail("шаг "+memo, err)
	}

	return out
}

// Facts asks the machine to look at itself again.
//
// This is how a capability appears: a wrapper installs docker and then asks
// for this, and the fact becomes true because the machine says so — not
// because the wrapper claimed it.
func (s Step) Facts() map[string]string {
	ctx := workflow.WithTaskQueue(s.run.s.ctx, s.target.Queue())

	var out agent.FactsOutput
	if err := workflow.ExecuteActivity(ctx, agent.ActivityFacts).Get(ctx, &out); err != nil {
		fail("факты "+s.target.installation, err)
	}

	return out.Facts
}
