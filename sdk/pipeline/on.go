package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"os"

	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/graphene/sdk/agent"
)

// Where a MACHINE reaches us. The operator sets both on the pipeline's
// worker, and the worker puts them into every install script it produces.
//
// They are separate from EnvAddress on purpose. A pipeline's worker runs
// inside the cluster and talks to Temporal by its service name; an agent
// runs on a machine somewhere else, and a service name means nothing to
// it. One address for two sides would be wrong for one of them.
const (
	// EnvControl is where a machine fetches the agent binary from.
	EnvControl = "GRAPHENE_CONTROL"
	// EnvAgentAddress is the Temporal frontend as a machine can reach it.
	EnvAgentAddress = "GRAPHENE_AGENT_TEMPORAL"
	// EnvAgentTraces is the OTLP receiver as a machine can reach it. The
	// trace is one per run, so what happened on the machine has to land
	// in the same place as everything else that run did.
	EnvAgentTraces = "GRAPHENE_AGENT_OTLP"
	// EnvTraces is the OTLP receiver as this worker reaches it, and what
	// a machine falls back to when nobody said otherwise.
	EnvTraces = "OTEL_EXPORTER_OTLP_ENDPOINT"
)

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
			Address:   agentAddress(),
			Namespace: os.Getenv(EnvNamespace),
			Records:   run.s.owner.Namespace,
			Machine:   installation,
			Traces:    agentTraces(),
			Token:     token(run, installation),
		},
	}
}

// agentTraces is the receiver as a machine sees it, falling back to how
// this worker sees it — right when both are the same host.
func agentTraces() string {
	if value := os.Getenv(EnvAgentTraces); value != "" {
		return value
	}

	return os.Getenv(EnvTraces)
}

// agentAddress is Temporal as a machine sees it, falling back to how this
// worker sees it — which is right when both are the same host, and that is
// exactly the case of a machine running beside the cluster.
func agentAddress() string {
	if value := os.Getenv(EnvAgentAddress); value != "" {
		return value
	}

	return os.Getenv(EnvAddress)
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
		s.run.raise("шаг "+memo, err)
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
		s.run.raise("факты "+s.target.installation, err)
	}

	return out.Facts
}
