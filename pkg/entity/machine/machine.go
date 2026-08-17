// Package machine defines the Machine system entity: one durable record
// per machine, living as an entity workflow on the graphene server worker.
//
// A machine is either CREATED in a cloud (graphene owns it and can tear
// it down) or RECOGNIZED over ssh (graphene does not own it and is
// physically unable to destroy it — deletion of the record leaves the
// machine untouched). Readiness means the agent has connected in both
// cases.
package machine

import (
	"errors"
	"fmt"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/graphene/pkg/id"
	"github.com/graphene-ci/graphene/pkg/ref"
)

// Kind is the entity kind name; workflow IDs are "machine/{machine-id}".
const Kind = entity.KindName("machine")

// CloudSource asks graphene to create the machine in a cloud. The record
// owns it: deleting the record destroys the machine.
type CloudSource struct {
	Provider string `json:"provider"`
	// Params are provider-specific creation parameters, interpreted by the
	// provider implementation behind Ops.
	Params map[string]string `json:"params,omitempty"`
}

// SSHSource asks graphene to recognize an existing machine over ssh. The
// record does not own it: nothing is created and nothing will ever be
// destroyed.
type SSHSource struct {
	Host   string        `json:"host"`
	Port   int           `json:"port,omitempty"`
	User   string        `json:"user"`
	KeyRef ref.SecretRef `json:"keyRef"`
}

// Spec is the desired state of a machine record. Exactly one source must
// be set.
type Spec struct {
	Cloud *CloudSource `json:"cloud,omitempty"`
	SSH   *SSHSource   `json:"ssh,omitempty"`

	Owner  ref.OwnerRef      `json:"owner,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
}

// Validate checks the spec structurally (deterministic; used as the entity
// update validator).
func (s Spec) Validate() error {
	if (s.Cloud == nil) == (s.SSH == nil) {
		return errors.New("exactly one of cloud or ssh must be set")
	}
	if s.SSH != nil && (s.SSH.Host == "" || s.SSH.User == "") {
		return errors.New("ssh source requires host and user")
	}
	if s.Cloud != nil && s.Cloud.Provider == "" {
		return errors.New("cloud source requires provider")
	}
	if s.Owner != "" {
		return s.Owner.Validate()
	}
	return nil
}

// Owned reports whether graphene owns the machine (may destroy it).
func (s Spec) Owned() bool { return s.Cloud != nil }

// State is the observed state of the machine.
type State struct {
	Addresses      []string  `json:"addresses,omitempty"`
	AgentConnected bool      `json:"agentConnected"`
	ConnectedAt    time.Time `json:"connectedAt,omitempty"`
	// FactsDigest references the machine facts blob; the facts themselves
	// live outside the record.
	FactsDigest string `json:"factsDigest,omitempty"`
	// Cloud is a teardown copy of the cloud source, set at creation: the
	// finalizer sees only State (temporal-entity limitation, candidate for
	// a chassis change), and destroy needs the provider parameters.
	Cloud *CloudSource `json:"cloud,omitempty"`
}

// Ops is the side-effect boundary of the machine entity: everything that
// touches clouds or the agent registry goes through it. Implementations
// live in the server; all methods must be idempotent (safe to retry).
type Ops interface {
	// CreateCloud creates (or finds by machine id) the machine in the
	// cloud and returns its addresses.
	CreateCloud(machineId id.MachineId, src CloudSource) ([]string, error)
	// DestroyCloud destroys the machine; not-found is not an error.
	DestroyCloud(machineId id.MachineId, src CloudSource) error
	// AgentStatus reports whether the agent of the machine is currently
	// connected and, if so, the digest of its reported facts.
	AgentStatus(machineId id.MachineId) (connected bool, factsDigest string, err error)
}

// Activity names (registered by the server against its Ops).
const (
	CreateCloudActivity  = "machine.create-cloud"
	DestroyCloudActivity = "machine.destroy-cloud"
	AgentStatusActivity  = "machine.agent-status"
)

// Options tune the machine entity definition.
type Options struct {
	// ConnectTimeout bounds waiting for the agent during creation.
	ConnectTimeout time.Duration
	// ReconcileEvery is the health-check period.
	ReconcileEvery time.Duration
	// PollInterval is the agent-status polling period during creation.
	PollInterval time.Duration
}

func (o *Options) defaults() {
	if o.ConnectTimeout == 0 {
		o.ConnectTimeout = 10 * time.Minute
	}
	if o.ReconcileEvery == 0 {
		o.ReconcileEvery = 30 * time.Second
	}
	if o.PollInterval == 0 {
		o.PollInterval = 5 * time.Second
	}
}

// Definition builds the machine entity definition. The server registers it
// on its worker together with activities implementing Ops.
func Definition(opts Options) *entdefine.Definition[Spec, State] {
	opts.defaults()
	return entdefine.New[Spec, State](Kind,
		entdefine.WithInit[Spec, State](func(ctx workflow.Context, spec Spec) (State, error) {
			return initMachine(ctx, opts, spec)
		}),
		entdefine.WithFinalize[Spec, State](finalizeMachine),
		entdefine.WithReconcileEvery[Spec, State](opts.ReconcileEvery, reconcileMachine),
		entdefine.WithSearchAttributes[Spec, State](true),
	)
}

func activityCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	})
}

// machineId derives the machine id from the entity workflow ID
// ("machine/{id}").
func machineId(ctx workflow.Context) id.MachineId {
	full := workflow.GetInfo(ctx).WorkflowExecution.ID
	prefix := string(Kind) + "/"
	if len(full) > len(prefix) {
		return id.MachineId(full[len(prefix):])
	}
	return id.MachineId(full)
}

func initMachine(ctx workflow.Context, opts Options, spec Spec) (State, error) {
	var st State
	mid := machineId(ctx)
	actx := activityCtx(ctx)

	if spec.Cloud != nil {
		if err := workflow.ExecuteActivity(actx, CreateCloudActivity, mid, *spec.Cloud).Get(ctx, &st.Addresses); err != nil {
			return st, fmt.Errorf("create in cloud: %w", err)
		}
		st.Cloud = spec.Cloud
	}

	// Readiness for both sources: the agent has connected.
	deadline := workflow.Now(ctx).Add(opts.ConnectTimeout)
	for workflow.Now(ctx).Before(deadline) {
		var status struct {
			Connected   bool   `json:"connected"`
			FactsDigest string `json:"factsDigest"`
		}
		if err := workflow.ExecuteActivity(actx, AgentStatusActivity, mid).Get(ctx, &status); err != nil {
			return st, fmt.Errorf("agent status: %w", err)
		}
		if status.Connected {
			st.AgentConnected = true
			st.ConnectedAt = workflow.Now(ctx)
			st.FactsDigest = status.FactsDigest
			return st, nil
		}
		if err := workflow.Sleep(ctx, opts.PollInterval); err != nil {
			return st, err
		}
	}
	return st, fmt.Errorf("agent did not connect within %s", opts.ConnectTimeout)
}

func reconcileMachine(ctx workflow.Context, ec *entdefine.Ctx[Spec, State]) error {
	if ec.Phase() != entity.PhaseReady {
		return nil
	}
	mid := machineId(ctx)
	var status struct {
		Connected   bool   `json:"connected"`
		FactsDigest string `json:"factsDigest"`
	}
	if err := workflow.ExecuteActivity(activityCtx(ctx), AgentStatusActivity, mid).Get(ctx, &status); err != nil {
		return err
	}
	st := ec.State()
	st.AgentConnected = status.Connected
	if status.Connected {
		st.FactsDigest = status.FactsDigest
	}
	return nil
}

func finalizeMachine(ctx workflow.Context, st *State) error {
	// Recognized machines are not ours to destroy: no cloud source in
	// state — deleting the record leaves the machine untouched.
	if st.Cloud == nil {
		return nil
	}
	return workflow.ExecuteActivity(activityCtx(ctx), DestroyCloudActivity, machineId(ctx), *st.Cloud).Get(ctx, nil)
}
