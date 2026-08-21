// Package triggerflow is a TRIGGER as an entity: the record of one
// declared way a pipeline's runs start without a human — a cron
// schedule ticking its own timer, or a webhook fired through the door.
// The record is owned by its pipeline (cascade removes it), carries its
// own history of firings, and pauses/resumes by command. It never
// decides whether a run actually starts: every firing goes to the
// pipeline record's fire command — the single arbiter of the
// concurrency policy.
package triggerflow

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"github.com/robfig/cron/v3"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Kind is the entity kind; the id is "{pipelineId}.{name}".
const Kind entity.KindName = "trigger"

// Id renders a trigger's resource id.
func Id(pipelineId, name string) entity.ResourceID {
	return entity.ResourceID(pipelineId + "." + name)
}

// Spec is the declaration, straight from the manifest.
type Spec struct {
	PipelineId string `json:"pipelineId"`
	// Kind: "cron" | "webhook".
	Kind string `json:"kind"`
	Name string `json:"name"`
	// Spec is the cron expression (cron only). Five-field cron plus
	// the @every/@hourly descriptors.
	Spec string `json:"spec,omitempty"`
	// SecretName authenticates the webhook (HMAC or bearer).
	SecretName string `json:"secretName,omitempty"`
	// Params is the fixed typed-params JSON firings start runs with.
	Params json.RawMessage `json:"params,omitempty"`
}

// State is the trigger's life.
type State struct {
	Paused    bool       `json:"paused,omitempty"`
	LastFired *time.Time `json:"lastFired,omitempty"`
	Firings   int        `json:"firings,omitempty"`
	// LastError keeps the most recent delivery failure for describe.
	LastError string `json:"lastError,omitempty"`
	// Owned: the record belongs to its pipeline — the cascade removes
	// triggers with it, and the publish reconcile finds them by owner.
	ownership.State
}

// PauseCmd stops firings without removing the record.
type PauseCmd struct{}

// Name is the command's wire identity.
func (PauseCmd) Name() entity.CommandName { return "pause" }

// Result binds the response type.
func (PauseCmd) Result() StateRes { return StateRes{} }

// ResumeCmd re-enables firings.
type ResumeCmd struct{}

// Name is the command's wire identity.
func (ResumeCmd) Name() entity.CommandName { return "resume" }

// Result binds the response type.
func (ResumeCmd) Result() StateRes { return StateRes{} }

// HookCmd is one webhook delivery, sent by the door after it verified
// the signature. Event is the request body.
type HookCmd struct {
	Event json.RawMessage `json:"event,omitempty"`
}

// Name is the command's wire identity.
func (HookCmd) Name() entity.CommandName { return "hook" }

// Result binds the response type.
func (HookCmd) Result() StateRes { return StateRes{} }

// StateRes reports the trigger's state after a command.
type StateRes struct {
	Paused  bool `json:"paused"`
	Firings int  `json:"firings"`
}

// FireRequest is what a firing sends to the arbiter activity: the
// pipeline record decides, this record only reports.
type FireRequest struct {
	PipelineId string          `json:"pipelineId"`
	Trigger    string          `json:"trigger"`
	Params     json.RawMessage `json:"params,omitempty"`
	Event      json.RawMessage `json:"event,omitempty"`
}

// FireActivity is the server-side arbiter call: it lands the fire
// command on the pipeline record.
const FireActivity = "server.trigger.fire"

// New builds the trigger definition. tick bounds cron latency.
func New(tick time.Duration) *entdefine.Definition[Spec, State] {
	def := entdefine.New[Spec, State](Kind,
		entdefine.WithSearchAttributes[Spec, State](true),
		entdefine.WithInit[Spec, State](func(ctx workflow.Context, spec Spec) (State, error) {
			if spec.Kind == "cron" {
				if _, err := cronNext(spec.Spec, time.Time{}); err != nil {
					return State{}, err
				}
			}
			// Creation is firing zero: a fresh cron waits for its NEXT
			// slot instead of firing immediately.
			now := workflow.Now(ctx)
			st := State{LastFired: &now}
			ownership.Init(ctx, &st.State, ref.OwnerRef("pipeline/"+spec.PipelineId))
			return st, nil
		}),
		entdefine.WithReconcileEvery[Spec, State](tick, cronTick),
	)
	ownership.Register(def, func(s *State) *ownership.State { return &s.State })
	entdefine.Handle(def, func(_ workflow.Context, ec *entdefine.Ctx[Spec, State], _ PauseCmd) (StateRes, error) {
		st := ec.State()
		st.Paused = true
		return StateRes{Paused: true, Firings: st.Firings}, nil
	})
	entdefine.Handle(def, func(_ workflow.Context, ec *entdefine.Ctx[Spec, State], _ ResumeCmd) (StateRes, error) {
		st := ec.State()
		st.Paused = false
		return StateRes{Paused: false, Firings: st.Firings}, nil
	})
	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[Spec, State], cmd HookCmd) (StateRes, error) {
		st := ec.State()
		if st.Paused {
			return StateRes{Paused: true, Firings: st.Firings}, nil
		}
		fire(ctx, ec.Spec(), st, cmd.Event)
		return StateRes{Paused: st.Paused, Firings: st.Firings}, nil
	})
	return def
}

// cronTick fires a due schedule. The record's own timer: no server
// loop, the firing is an event of THIS history.
func cronTick(ctx workflow.Context, ec *entdefine.Ctx[Spec, State]) error {
	spec := ec.Spec()
	if spec.Kind != "cron" {
		return nil
	}
	st := ec.State()
	if st.Paused {
		return nil
	}
	last := workflow.Now(ctx)
	if st.LastFired != nil {
		last = *st.LastFired
	}
	next, err := cronNext(spec.Spec, last)
	if err != nil {
		return err
	}
	if workflow.Now(ctx).Before(next) {
		return nil
	}
	fire(ctx, spec, st, nil)
	return nil
}

// fire delivers one firing to the arbiter and records the outcome on
// this record.
func fire(ctx workflow.Context, spec Spec, st *State, event json.RawMessage) {
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		TaskQueue:           wire.ServerQueue,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	err := workflow.ExecuteActivity(actx, FireActivity, FireRequest{
		PipelineId: spec.PipelineId,
		Trigger:    spec.Name,
		Params:     spec.Params,
		Event:      event,
	}).Get(ctx, nil)
	now := workflow.Now(ctx)
	st.LastFired = &now
	st.Firings++
	if err != nil {
		st.LastError = err.Error()
		return
	}
	st.LastError = ""
}

// cronNext parses the spec and returns the next firing after t.
func cronNext(spec string, t time.Time) (time.Time, error) {
	sched, err := cron.ParseStandard(spec)
	if err != nil {
		return time.Time{}, fmt.Errorf("cron spec %q: %w", spec, err)
	}
	return sched.Next(t), nil
}
