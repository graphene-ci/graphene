// Package standflow is the Stand as an ENTITY: an owner holding what runs
// hand over, so machines and resources outlive the runs that use them.
// Every pipeline has its own stand (stand/<pipelineId>), created lazily by
// its first transfer; a NAMED stand (stand/<name>) is applied like any
// record, has its own owner, and only receives transfers once it exists.
// TTLs are the stand's OWN timers — acceptance, extension, expiry, and the
// cascade are lived in its history, not scanned from visibility by a
// server loop.
package standflow

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Kind is the entity kind.
const Kind entity.KindName = "stand"

// CascadeActivity deletes one held subtree — served by the graphene
// server worker (the stand itself runs there too).
const CascadeActivity = "server.stand.cascade"

// ExpireActivity deletes the calling stand: its TTL or idle timeout ran
// out — served by the graphene server worker.
const ExpireActivity = "server.stand.expire"

// Spec is the stand's identity.
type Spec struct {
	// PipelineId makes this a pipeline's OWN stand. The pipeline IS the
	// project — source, arbiter and active version in one record — so its
	// stand is its child, named after it.
	PipelineId string `json:"pipelineId,omitempty"`
	// Owner owns a NAMED stand: a pipeline ("pipeline/<id>") or nobody —
	// the namespace itself. Not set together with PipelineId.
	Owner ref.OwnerRef `json:"owner,omitempty"`
	// TTL ends the stand (and everything it holds) after this long, e.g.
	// "168h"; empty — the stand lives until deleted.
	TTL string `json:"ttl,omitempty"`
	// IdleTimeout ends the stand after this long without use, e.g. "72h";
	// empty — never idle out.
	IdleTimeout string `json:"idleTimeout,omitempty"`
}

// Validate checks the declaration before the record starts.
func (s Spec) Validate() error {
	if s.PipelineId != "" && s.Owner != "" {
		return errors.New("stand: a pipeline's own stand takes no separate owner")
	}
	if s.Owner != "" {
		if err := s.Owner.Validate(); err != nil {
			return fmt.Errorf("stand owner: %w", err)
		}
	}
	if _, err := parseLifetime("ttl", s.TTL); err != nil {
		return err
	}
	_, err := parseLifetime("idleTimeout", s.IdleTimeout)
	return err
}

// parseLifetime reads a duration field: empty is zero (none), anything
// else must be positive.
func parseLifetime(field, v string) (time.Duration, error) {
	if v == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("stand %s %q: %w", field, v, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("stand %s %q: must be positive", field, v)
	}
	return d, nil
}

// Holding is one resource the stand keeps.
type Holding struct {
	// KeepUntil bounds the stay; nil means until an explicit release.
	KeepUntil *time.Time `json:"keepUntil,omitempty"`
	// From records who handed it over.
	From string `json:"from,omitempty"`
}

// State is the stand's holdings and lifetime.
type State struct {
	ownership.State
	Holdings map[string]Holding `json:"holdings,omitempty"`
	// ExpiresAt ends the stand; nil — no TTL.
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
	// IdleTimeout ends a stand unused for that long; zero — never.
	IdleTimeout time.Duration `json:"idleTimeout,omitempty"`
	// LastActive is the last time anything used the stand.
	LastActive time.Time `json:"lastActive,omitempty"`
	// Expiring is set once the stand asked for its own deletion.
	Expiring bool `json:"expiring,omitempty"`
	// Lease is the run using the stand now; nil — free.
	Lease *Lease `json:"lease,omitempty"`
}

// Lease is one run's exclusive use of a stand.
type Lease struct {
	// Run is the run id ("run/<run>" is its workflow).
	Run string    `json:"run"`
	At  time.Time `json:"at"`
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
func (AcceptCmd) Result() CommandRes { return CommandRes{} }

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
func (ExtendCmd) Result() CommandRes { return CommandRes{} }

// ReleaseCmd tears a holding down NOW (cascade included); empty Ref
// releases everything.
type ReleaseCmd struct {
	Ref ref.OwnerRef `json:"ref,omitempty"`
}

// Name is the command's wire identity.
func (ReleaseCmd) Name() entity.CommandName { return "release" }

// Result binds the response type.
func (ReleaseCmd) Result() CommandRes { return CommandRes{} }

// SetTTLCmd replaces the stand's TTL, counted from now; empty removes it.
type SetTTLCmd struct {
	TTL string `json:"ttl,omitempty"`
}

// Name is the command's wire identity.
func (SetTTLCmd) Name() entity.CommandName { return "set-ttl" }

// Result binds the response type.
func (SetTTLCmd) Result() CommandRes { return CommandRes{} }

// Validate rejects a malformed duration before anything runs.
func (c SetTTLCmd) Validate() error {
	_, err := parseLifetime("ttl", c.TTL)
	return err
}

// SetIdleCmd replaces the stand's idle timeout; empty removes it.
type SetIdleCmd struct {
	IdleTimeout string `json:"idleTimeout,omitempty"`
}

// Name is the command's wire identity.
func (SetIdleCmd) Name() entity.CommandName { return "set-idle" }

// Result binds the response type.
func (SetIdleCmd) Result() CommandRes { return CommandRes{} }

// Validate rejects a malformed duration before anything runs.
func (c SetIdleCmd) Validate() error {
	_, err := parseLifetime("idleTimeout", c.IdleTimeout)
	return err
}

// Refusal codes: the stable error types a stand answers with. A queue in
// front of the stand tells them apart as wait reasons.
const (
	// CodeBusy — another run holds the lease.
	CodeBusy = "busy"
	// CodeDirty — a machine on the stand has leaks its removals left.
	CodeDirty = "dirty"
	// CodeLeased — the stand is in use; holdings cannot leave it.
	CodeLeased = "stand_leased"
	// CodeNotHolder — the lease belongs to another run.
	CodeNotHolder = "not_holder"
)

func refusal(code, format string, args ...any) error {
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf(format, args...), code, nil)
}

// DirtyActivity lists the agents on the calling stand that have leaks —
// served by the graphene server worker.
const DirtyActivity = "server.stand.dirty"

// RunStateActivity reports a run's state: "running", "closed", or
// "unknown" (never started) — served by the graphene server worker.
const RunStateActivity = "server.run.state"

// Run states RunStateActivity answers.
const (
	RunRunning = "running"
	RunClosed  = "closed"
	RunUnknown = "unknown"
)

// LeaseGrace is how long a lease waits for a run that has not started:
// a queue acquires before it starts the run.
const LeaseGrace = 15 * time.Minute

// AcquireCmd takes the stand for one run: exclusive, refused while
// another run holds it (busy) or a machine on it is dirty. The run may
// acquire before it starts; the lease ends with the run.
type AcquireCmd struct {
	Run string `json:"run"`
}

// Name is the command's wire identity.
func (AcquireCmd) Name() entity.CommandName { return "acquire" }

// Result binds the response type.
func (AcquireCmd) Result() LeaseRes { return LeaseRes{} }

// Validate rejects a lease for nobody.
func (c AcquireCmd) Validate() error {
	if c.Run == "" || strings.Contains(c.Run, "/") {
		return fmt.Errorf("acquire needs a run id, got %q", c.Run)
	}
	return nil
}

// ReleaseLeaseCmd gives the stand back before the run ends.
type ReleaseLeaseCmd struct {
	Run string `json:"run"`
}

// Name is the command's wire identity.
func (ReleaseLeaseCmd) Name() entity.CommandName { return "release-lease" }

// Result binds the response type.
func (ReleaseLeaseCmd) Result() LeaseRes { return LeaseRes{} }

// Validate rejects a release for nobody.
func (c ReleaseLeaseCmd) Validate() error {
	if c.Run == "" {
		return errors.New("release-lease needs the run that holds the lease")
	}
	return nil
}

// LeaseRes names the run holding the stand after the command; "" — free.
type LeaseRes struct {
	Holder string `json:"holder,omitempty"`
}

// CommandRes reports the number of holdings after a command. The entity
// deduplicates the last 100 responses across Continue-as-New: returning the
// entire holdings map here would copy a growing state into every cache entry.
// Read the holdings through the record describe query instead.
type CommandRes struct {
	Count int `json:"count"`
}

// New builds the stand definition. tick is how often expiry is checked.
func New(tick time.Duration) *entdefine.Definition[Spec, State] {
	def := entdefine.New[Spec, State](Kind,
		entdefine.WithSearchAttributes[Spec, State](true),
		entdefine.WithInit[Spec, State](func(ctx workflow.Context, spec Spec) (State, error) {
			st := State{Holdings: map[string]Holding{}}
			owner := spec.Owner
			if spec.PipelineId != "" {
				owner = ref.OwnerRef("pipeline/" + spec.PipelineId)
			}
			ownership.Init(ctx, &st.State, owner)
			// Validated on apply; a pipeline's own stand has neither.
			ttl, _ := parseLifetime("ttl", spec.TTL)
			idle, _ := parseLifetime("idleTimeout", spec.IdleTimeout)
			now := workflow.Now(ctx)
			if ttl > 0 {
				deadline := now.Add(ttl)
				st.ExpiresAt = &deadline
			}
			st.IdleTimeout = idle
			st.LastActive = now
			return st, nil
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
	ownership.Register(def, func(st *State) *ownership.State { return &st.State })

	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[Spec, State], cmd AcceptCmd) (CommandRes, error) {
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
		st.LastActive = workflow.Now(ctx)
		return CommandRes{Count: len(st.Holdings)}, nil
	})

	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[Spec, State], cmd ExtendCmd) (CommandRes, error) {
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
				return CommandRes{}, fmt.Errorf("stand does not hold %s", cmd.Ref)
			}
			extend(string(cmd.Ref), h)
		} else {
			for key, h := range st.Holdings {
				extend(key, h)
			}
		}
		st.LastActive = workflow.Now(ctx)
		return CommandRes{Count: len(st.Holdings)}, nil
	})

	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[Spec, State], cmd ReleaseCmd) (CommandRes, error) {
		st := ec.State()
		// A leased stand's machines are in use: nothing leaves it until
		// the run is done.
		if st.Lease != nil {
			return CommandRes{}, refusal(CodeLeased, "stand is leased by run %s", st.Lease.Run)
		}
		targets := []string{}
		if cmd.Ref != "" {
			if _, held := st.Holdings[string(cmd.Ref)]; !held {
				return CommandRes{}, fmt.Errorf("stand does not hold %s", cmd.Ref)
			}
			targets = append(targets, string(cmd.Ref))
		} else {
			for key := range st.Holdings {
				targets = append(targets, key)
			}
		}
		for _, held := range targets {
			if err := cascade(ctx, held); err != nil {
				return CommandRes{}, err
			}
			delete(st.Holdings, held)
		}
		st.LastActive = workflow.Now(ctx)
		return CommandRes{Count: len(st.Holdings)}, nil
	})

	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[Spec, State], cmd SetTTLCmd) (CommandRes, error) {
		st := ec.State()
		ttl, _ := parseLifetime("ttl", cmd.TTL)
		st.ExpiresAt = nil
		if ttl > 0 {
			deadline := workflow.Now(ctx).Add(ttl)
			st.ExpiresAt = &deadline
		}
		return CommandRes{Count: len(st.Holdings)}, nil
	})

	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[Spec, State], cmd SetIdleCmd) (CommandRes, error) {
		st := ec.State()
		st.IdleTimeout, _ = parseLifetime("idleTimeout", cmd.IdleTimeout)
		st.LastActive = workflow.Now(ctx)
		return CommandRes{Count: len(st.Holdings)}, nil
	})

	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[Spec, State], cmd AcquireCmd) (LeaseRes, error) {
		st := ec.State()
		if st.Lease != nil {
			if st.Lease.Run == cmd.Run {
				return LeaseRes{Holder: cmd.Run}, nil
			}
			return LeaseRes{Holder: st.Lease.Run}, refusal(CodeBusy, "stand is leased by run %s", st.Lease.Run)
		}
		if st.Expiring {
			return LeaseRes{}, refusal(CodeLeased, "stand is being deleted")
		}
		var dirty []string
		if err := workflow.ExecuteActivity(serverActx(ctx), DirtyActivity).Get(ctx, &dirty); err != nil {
			return LeaseRes{}, err
		}
		if len(dirty) > 0 {
			return LeaseRes{}, refusal(CodeDirty, "machines with leaks: %s", strings.Join(dirty, ", "))
		}
		now := workflow.Now(ctx)
		st.Lease = &Lease{Run: cmd.Run, At: now}
		st.LastActive = now
		return LeaseRes{Holder: cmd.Run}, nil
	})

	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[Spec, State], cmd ReleaseLeaseCmd) (LeaseRes, error) {
		st := ec.State()
		if st.Lease == nil {
			return LeaseRes{}, nil
		}
		if st.Lease.Run != cmd.Run {
			return LeaseRes{Holder: st.Lease.Run}, refusal(CodeNotHolder, "stand is leased by run %s", st.Lease.Run)
		}
		st.Lease = nil
		st.LastActive = workflow.Now(ctx)
		return LeaseRes{}, nil
	})

	return def
}

// leaseVersion gates the lease checks in the reconcile tick.
const leaseVersion = "stand-lease"

// leaseHolds reports whether the lease still stands: its run is running,
// or has not started yet and is within LeaseGrace.
func leaseHolds(ctx workflow.Context, l *Lease, now time.Time) (bool, error) {
	var state string
	if err := workflow.ExecuteActivity(serverActx(ctx), RunStateActivity, l.Run).Get(ctx, &state); err != nil {
		return false, err
	}
	switch state {
	case RunRunning:
		return true, nil
	case RunUnknown:
		return now.Sub(l.At) < LeaseGrace, nil
	default:
		return false, nil
	}
}

// lifetimeVersion gates the stand-level TTL and idle timeout: histories
// written before them replay without the new commands.
const lifetimeVersion = "stand-lifetime"

// outlived reports whether the stand's own TTL or idle timeout ran out.
func outlived(st *State, now time.Time) bool {
	if st.ExpiresAt != nil && !now.Before(*st.ExpiresAt) {
		return true
	}
	return st.IdleTimeout > 0 && now.Sub(st.LastActive) >= st.IdleTimeout
}

// expireTick tears down what outstayed its keep — the stand's own
// clock, not a server scan. A holding whose ORIGIN run still runs is
// left alone until the run ends: ToStand happens mid-workflow, and a
// short keep must never tear the tree from under the run that handed
// it over.
func expireTick(ctx workflow.Context, ec *entdefine.Ctx[Spec, State]) error {
	st := ec.State()
	now := workflow.Now(ctx)
	if workflow.GetVersion(ctx, leaseVersion, workflow.DefaultVersion, 1) != workflow.DefaultVersion && st.Lease != nil {
		held, err := leaseHolds(ctx, st.Lease, now)
		if err != nil {
			return err
		}
		if held {
			// In use: nothing expires under a running run, and use is
			// not idleness.
			st.LastActive = now
			return nil
		}
		workflow.GetLogger(ctx).Info("stand lease ended with its run", "run", st.Lease.Run)
		st.Lease = nil
		st.LastActive = now
	}
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
	if workflow.GetVersion(ctx, lifetimeVersion, workflow.DefaultVersion, 1) == workflow.DefaultVersion {
		return nil
	}
	if st.Expiring || !outlived(st, now) {
		return nil
	}
	// The stand asks for its own deletion; the delete runs the finalize,
	// which releases everything it still holds.
	workflow.GetLogger(ctx).Info("stand outlived its TTL or idle timeout")
	if err := workflow.ExecuteActivity(serverActx(ctx), ExpireActivity).Get(ctx, nil); err != nil {
		return err
	}
	st.Expiring = true
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
