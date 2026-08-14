package agent

import (
	"time"
	"unicode/utf8"
)

// What an agent can be asked to do. Five primitives were named in FORM;
// these are the two M2 needs, and the others arrive with the thing that
// needs them rather than before it.
const (
	// ActivityExec runs a command on the machine.
	ActivityExec = "graphene.Exec"
	// ActivityFacts re-reads what the machine has.
	ActivityFacts = "graphene.Facts"
	// ActivityRegister records the machine in the cluster.
	//
	// It runs on the SYSTEM queue, not on the agent's: the agent has no
	// access to the cluster and will not get any. A token that let a
	// machine write records would be a key to the cluster handed to every
	// machine we ever install on.
	ActivityRegister = "graphene.Register"
)

// WorkflowRegister is what an agent starts to get itself into the cluster.
// A workflow rather than an activity because an activity has to be
// scheduled by something, and an agent is a worker, not a workflow.
const WorkflowRegister = "graphene.RegisterMachine"

// queuePrefix keeps our task queues apart from anything else in the same
// Temporal namespace.
const queuePrefix = "graphene-machine-"

// InstallationQueue is the Temporal task queue of one installation of the
// agent.
//
// It belongs to the INSTALLATION, not to the machine. Reinstalling the
// agent on the same hardware must produce a different queue: otherwise a
// step scheduled into the previous installation's queue would reach a
// zombie, while the new agent sat waiting for work that went elsewhere.
//
// It is a pure function of the installation's name, which is what lets a
// pipeline schedule a step before the machine exists — the queue is known
// the moment the name is chosen, and the queue itself is the waiting.
func InstallationQueue(installation string) string {
	return queuePrefix + installation
}

// WorkflowRegisterRevision is what a pipeline's worker starts when it comes
// up, to say which kinds its pipeline applies.
//
// The same shape as the agent's registration and for the same reason: a
// pipeline's worker has no access to the cluster — the import guard makes
// sure of it — so what it knows travels through Temporal to a worker of
// ours that does.
const WorkflowRegisterRevision = "graphene.RegisterRevision"

// ActivityRegisterRevision records what a revision needs.
const ActivityRegisterRevision = "graphene.RecordRequirements"

// Kind names one kind a pipeline applies.
type Kind struct {
	Group   string `json:"group"`
	Version string `json:"version"`
	Kind    string `json:"kind"`
}

// RegisterRevisionInput is a revision's worker saying what it will need.
type RegisterRevisionInput struct {
	Revision  string `json:"revision"`
	Namespace string `json:"namespace"`
	// Requires is every kind the pipeline could apply. It comes from the
	// scheme the pipeline handed to Serve, which is the only place that
	// knows — the kinds live in the pipeline's own imports.
	// +optional
	Requires []Kind `json:"requires,omitempty"`
}

// LeaseSeconds is how long an agent's mark is good for. It lives in the
// contract because both sides need the same number: the agent decides how
// often to mark, the operator decides when silence has gone on too long.
const LeaseSeconds = 40

// MaxOutputBytes is how much of a step's output travels back.
//
// Output goes into Temporal's history and stays there for the life of the
// run. Real output is an artifact and belongs in storage — that is M6. What
// comes back here is the tail, which is what a person reads first when
// something fails, and Truncated says plainly that there was more.
const MaxOutputBytes = 64 << 10

// DefaultExecTimeout bounds a command that did not say how long it needs.
const DefaultExecTimeout = 10 * time.Minute

// ExecInput is one command on one machine.
type ExecInput struct {
	// Argv runs the program directly, without a shell. Preferred: nothing
	// is re-parsed, so a value with a space in it stays one value.
	// +optional
	Argv []string `json:"argv,omitempty"`

	// Script is fed to a shell. For the cases where the shell IS the
	// point — a pipeline of commands, a heredoc, a conditional.
	// +optional
	Script string `json:"script,omitempty"`

	// +optional
	Env map[string]string `json:"env,omitempty"`
	// +optional
	Dir string `json:"dir,omitempty"`
	// Timeout bounds this command. Zero means DefaultExecTimeout.
	// +optional
	Timeout time.Duration `json:"timeout,omitempty"`
}

// ExecOutput is what came back.
type ExecOutput struct {
	// Code is the process's exit code. A non-zero code is not an error of
	// the activity: the command ran and said no, which is an answer the
	// pipeline is allowed to look at.
	Code int `json:"code"`

	Stdout string `json:"stdout,omitempty"`
	Stderr string `json:"stderr,omitempty"`

	// Truncated says the output was longer than MaxOutputBytes and what
	// came back is its tail.
	Truncated bool `json:"truncated,omitempty"`
}

// FactsOutput is what the machine turned out to have.
type FactsOutput struct {
	Facts map[string]string `json:"facts,omitempty"`
}

// RegisterInput is an agent introducing itself.
type RegisterInput struct {
	// Machine is what this machine is called in the cluster.
	Machine string `json:"machine"`
	// Namespace is where the record goes.
	Namespace string `json:"namespace"`
	// Queue is this installation's task queue.
	Queue string `json:"queue"`
	// Facts are what the agent found when it started.
	// +optional
	Facts map[string]string `json:"facts,omitempty"`
}

// Tail keeps the last limit bytes of s and says whether anything was cut.
//
// The tail rather than the head: when a command fails, what says why is at
// the end. A head would reliably return the part nobody needs.
func Tail(text string, limit int) (string, bool) {
	if len(text) <= limit {
		return text, false
	}

	cut := text[len(text)-limit:]

	// Не разрезаем символ пополам: обрезанный по байтам UTF-8 даёт
	// мусор в первом же символе, и человек видит его раньше всего
	// остального. Если целого символа не осталось вовсе — не осталось и
	// ничего, что стоит показывать.
	for i := range len(cut) {
		if utf8.RuneStart(cut[i]) {
			return cut[i:], true
		}
	}

	return "", true
}
