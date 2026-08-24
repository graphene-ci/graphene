// Package standflow is the Stand as an ENTITY: the permanent owner
// every pipeline has, holding what runs hand over. The TTL is the
// stand's OWN timer — acceptance, extension, expiry, and the cascade
// are lived in its history, not scanned from visibility by a server
// loop. Lazy: the first transfer creates it.
package standflow

import (
	"strings"
	"fmt"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Kind is the entity kind.
const Kind entity.KindName = "stand"

// CascadeActivity deletes one held subtree — served by the graphene
// server worker (the stand itself runs there too).
const CascadeActivity = "server.stand.cascade"

// Spec is the stand's identity.
type Spec struct {
	PipelineId string `json:"pipelineId"`
}

// Holding is one resource the stand keeps.
type Holding struct {
	// KeepUntil bounds the stay; nil means until an explicit release.
	KeepUntil *time.Time `json:"keepUntil,omitempty"`
	// From records who handed it over.
	From string `json:"from,omitempty"`
}

// State is the stand's holdings.
type State struct {
	Holdings map[string]Holding `json:"holdings,omitempty"`
}

// AcceptCmd registers a handover (the transfer flow calls it right
// after transfer-owner lands on the resource).
type AcceptCmd struct {
	Ref  ref.OwnerRef  `json:"ref"`
	Keep time.Duration `json:"keep,omitempty"`
	From string        `json:"from,omitempty"`
}

// Name is the command's wire identity.
func (AcceptCmd) Name() entity.CommandName { return "accept" }

// Result binds the response type.
func (AcceptCmd) Result() HoldingsRes { return HoldingsRes{} }

// Validate rejects a malformed ref.
func (c AcceptCmd) Validate() error { return c.Ref.Validate() }

// ExtendCmd moves a holding's deadline (or removes it with Keep=0);
// empty Ref extends everything.
type ExtendCmd struct {
	Ref  ref.OwnerRef  `json:"ref,omitempty"`
	Keep time.Duration `json:"keep"`
}

// Name is the command's wire identity.
func (ExtendCmd) Name() entity.CommandName { return "extend" }

// Result binds the response type.
func (ExtendCmd) Result() HoldingsRes { return HoldingsRes{} }

// ReleaseCmd tears a holding down NOW (cascade included); empty Ref
// releases everything.
type ReleaseCmd struct {
	Ref ref.OwnerRef `json:"ref,omitempty"`
}

// Name is the command's wire identity.
func (ReleaseCmd) Name() entity.CommandName { return "release" }

// Result binds the response type.
func (ReleaseCmd) Result() HoldingsRes { return HoldingsRes{} }

// HoldingsRes reports the holdings after a command.
type HoldingsRes struct {
	Holdings map[string]Holding `json:"holdings,omitempty"`
}

// New builds the stand definition. tick is how often expiry is checked.
func New(tick time.Duration) *entdefine.Definition[Spec, State] {
	def := entdefine.New[Spec, State](Kind,
		entdefine.WithSearchAttributes[Spec, State](true),
		entdefine.WithInit[Spec, State](func(workflow.Context, Spec) (State, error) {
			return State{Holdings: map[string]Holding{}}, nil
		}),
		entdefine.WithReconcileEvery[Spec, State](tick, expireTick),
		// Deleting the stand releases everything it still holds.
		entdefine.WithFinalize[Spec, State](func(ctx workflow.Context, st *State) error {
			for held := range st.Holdings {
				if err := cascade(ctx, held); err != nil {
					return err
				}
				delete(st.Holdings, held)
			}
			return nil
		}),
	)

	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[Spec, State], cmd AcceptCmd) (HoldingsRes, error) {
		st := ec.State()
		if st.Holdings == nil {
			st.Holdings = map[string]Holding{}
		}
		h := Holding{From: cmd.From}
		if cmd.Keep > 0 {
			deadline := workflow.Now(ctx).Add(cmd.Keep)
			h.KeepUntil = &deadline
		}
		st.Holdings[string(cmd.Ref)] = h
		return HoldingsRes{Holdings: st.Holdings}, nil
	})

	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[Spec, State], cmd ExtendCmd) (HoldingsRes, error) {
		st := ec.State()
		extend := func(key string, h Holding) {
			if cmd.Keep > 0 {
				deadline := workflow.Now(ctx).Add(cmd.Keep)
				h.KeepUntil = &deadline
			} else {
				h.KeepUntil = nil
			}
			st.Holdings[key] = h
		}
		if cmd.Ref != "" {
			h, held := st.Holdings[string(cmd.Ref)]
			if !held {
				return HoldingsRes{}, fmt.Errorf("stand does not hold %s", cmd.Ref)
			}
			extend(string(cmd.Ref), h)
		} else {
			for key, h := range st.Holdings {
				extend(key, h)
			}
		}
		return HoldingsRes{Holdings: st.Holdings}, nil
	})

	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[Spec, State], cmd ReleaseCmd) (HoldingsRes, error) {
		st := ec.State()
		targets := []string{}
		if cmd.Ref != "" {
			if _, held := st.Holdings[string(cmd.Ref)]; !held {
				return HoldingsRes{}, fmt.Errorf("stand does not hold %s", cmd.Ref)
			}
			targets = append(targets, string(cmd.Ref))
		} else {
			for key := range st.Holdings {
				targets = append(targets, key)
			}
		}
		for _, held := range targets {
			if err := cascade(ctx, held); err != nil {
				return HoldingsRes{}, err
			}
			delete(st.Holdings, held)
		}
		return HoldingsRes{Holdings: st.Holdings}, nil
	})

	return def
}

// expireTick tears down what outstayed its keep — the stand's own
// clock, not a server scan. A holding whose ORIGIN run still runs is
// left alone until the run ends: ToStand happens mid-workflow, and a
// short keep must never tear the tree from under the run that handed
// it over.
func expireTick(ctx workflow.Context, ec *entdefine.Ctx[Spec, State]) error {
	st := ec.State()
	now := workflow.Now(ctx)
	for held, h := range st.Holdings {
		if h.KeepUntil == nil || h.KeepUntil.After(now) {
			continue
		}
		if strings.HasPrefix(h.From, "run/") {
			var active bool
			if err := workflow.ExecuteActivity(serverActx(ctx), RunActiveActivity, h.From).Get(ctx, &active); err != nil {
				return err
			}
			if active {
				workflow.GetLogger(ctx).Info("stand TTL expired, awaiting the origin run",
					"resource", held, "run", h.From)
				continue
			}
		}
		workflow.GetLogger(ctx).Info("stand TTL expired", "resource", held)
		if err := cascade(ctx, held); err != nil {
			return err
		}
		delete(st.Holdings, held)
	}
	return nil
}

// RunActiveActivity reports whether a workflow (a run) is still
// running — served by the graphene worker.
const RunActiveActivity = "stand.run-active"

// cascade runs the server-side teardown of one held subtree.
func cascade(ctx workflow.Context, held string) error {
	return workflow.ExecuteActivity(serverActx(ctx), CascadeActivity, held).Get(ctx, nil)
}

func serverActx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           wire.ServerQueue,
		StartToCloseTimeout: 10 * time.Minute,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	})
}
