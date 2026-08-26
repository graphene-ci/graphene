// Package nsflow holds the record of a NAMESPACE — the isolation unit
// itself, declared like everything else. The records live in the
// default namespace: a container cannot hold its own declaration, and
// the default one exists on every installation.
package nsflow

import (
	"fmt"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Kind is the entity kind name; the record id is the namespace name.
const Kind = entity.KindName("namespace")

// Spec is what a namespace IS to the installation.
type Spec struct {
	// RetentionDays bounds how long closed workflows are kept; 0 takes
	// the installation's default.
	RetentionDays int32 `json:"retentionDays,omitempty"`
	// Description is for humans reading a list of namespaces.
	Description string `json:"description,omitempty"`
}

// Validate refuses a retention the durable core would reject.
func (s Spec) Validate() error {
	if s.RetentionDays < 0 {
		return fmt.Errorf("retention cannot be negative")
	}
	return nil
}

// State is the namespace as it stands.
type State struct {
	ownership.State
	// RetentionDays is what was registered.
	RetentionDays int32 `json:"retentionDays,omitempty"`
	// CreatedAt is when the namespace was registered (workflow time).
	CreatedAt string `json:"createdAt,omitempty"`
}

// EnsureActivity registers the namespace in the durable core and
// starts its worker: (EnsureReq). Idempotent — an existing namespace
// is adopted, not refused.
const EnsureActivity = "server.namespace.ensure"

// RetireActivity stops serving a deleted namespace: (RetireReq). The
// namespace's own records are NOT destroyed — they age out under the
// retention they were registered with, the way a closed workflow does.
const RetireActivity = "server.namespace.retire"

// EnsureReq asks for one namespace to exist.
type EnsureReq struct {
	Name          string `json:"name"`
	RetentionDays int32  `json:"retentionDays,omitempty"`
}

// RetireReq asks for one namespace to stop being served.
type RetireReq struct {
	Name string `json:"name"`
}

// New builds the namespace definition.
func New() *entdefine.Definition[Spec, State] {
	def := entdefine.New[Spec, State](Kind,
		entdefine.WithSearchAttributes[Spec, State](true),
		entdefine.WithInit[Spec, State](func(ctx workflow.Context, spec Spec) (State, error) {
			var st State
			if err := spec.Validate(); err != nil {
				return st, temporal.NewNonRetryableApplicationError(err.Error(), "BadSpec", err)
			}
			ownership.Init(ctx, &st.State, "")
			name := nameOf(ctx)
			if err := workflow.ExecuteActivity(actx(ctx), EnsureActivity,
				EnsureReq{Name: name, RetentionDays: spec.RetentionDays}).Get(ctx, nil); err != nil {
				return st, err
			}
			st.RetentionDays = spec.RetentionDays
			st.CreatedAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
			return st, nil
		}),
		entdefine.WithFinalize[Spec, State](func(ctx workflow.Context, _ *State) error {
			return workflow.ExecuteActivity(actx(ctx), RetireActivity,
				RetireReq{Name: nameOf(ctx)}).Get(ctx, nil)
		}),
	)
	ownership.Register(def, func(st *State) *ownership.State { return &st.State })
	return def
}

// nameOf recovers the namespace name from the record's workflow id.
func nameOf(ctx workflow.Context) string {
	id := workflow.GetInfo(ctx).WorkflowExecution.ID
	prefix := string(Kind) + "/"
	if len(id) > len(prefix) && id[:len(prefix)] == prefix {
		return id[len(prefix):]
	}
	return id
}

// actx bounds the registration: creating a namespace is a handful of
// core calls, and a name that cannot be registered must fail loudly.
func actx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
		TaskQueue:           wire.ServerQueue,
	})
}
