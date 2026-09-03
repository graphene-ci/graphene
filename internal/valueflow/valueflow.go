// Package valueflow holds the records of the two value planes: the
// VARIABLE, whose value is ordinary configuration, and the SECRET,
// whose value never enters a history. Both are ordinary records — they
// are listed, owned, audited and deleted like everything else; the
// secret's value simply lives beside its record, in the sealed store,
// and only its rotations are recorded.
package valueflow

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

// The kinds of the value contour.
const (
	VarKind    = entity.KindName("var")
	SecretKind = entity.KindName("secret")
)

// --- var ---

// VarSpec is what a variable IS. The name is the record id, so the
// spec carries only the value it starts with.
type VarSpec struct {
	Value string `json:"value,omitempty"`
}

// VarState is the variable as it stands: its value is readable by
// design — that is the whole difference from a secret.
type VarState struct {
	ownership.State
	Value     string `json:"value"`
	UpdatedAt string `json:"updatedAt,omitempty"`
}

// SetVarCmd replaces a variable's value.
type SetVarCmd struct {
	Value string `json:"value"`
}

// Name is the command's wire identity.
func (SetVarCmd) Name() entity.CommandName { return "set" }

// Result binds the response type.
func (SetVarCmd) Result() VarRes { return VarRes{} }

// VarRes reports the variable after a command.
type VarRes struct {
	Value string `json:"value"`
}

// NewVar builds the variable definition.
func NewVar() *entdefine.Definition[VarSpec, VarState] {
	def := entdefine.New[VarSpec, VarState](VarKind,
		entdefine.WithSearchAttributes[VarSpec, VarState](true),
		entdefine.WithInit[VarSpec, VarState](func(ctx workflow.Context, spec VarSpec) (VarState, error) {
			var st VarState
			ownership.Init(ctx, &st.State, "")
			st.Value = spec.Value
			st.UpdatedAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
			return st, nil
		}),
	)
	ownership.Register(def, func(st *VarState) *ownership.State { return &st.State })
	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[VarSpec, VarState], cmd SetVarCmd) (VarRes, error) {
		st := ec.State()
		st.Value = cmd.Value
		st.UpdatedAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
		return VarRes{Value: st.Value}, nil
	})
	return def
}

// --- secret ---

// SecretSpec is what a secret IS to the system: a NAME with a value
// kept elsewhere. Nothing here carries the value — not on creation
// either, because a spec is replayed forever.
type SecretSpec struct {
	// Description is for humans reading a list of secrets.
	Description string `json:"description,omitempty"`
}

// SecretState records the value's LIFE, never the value: how many
// times it was written, and when it last was.
type SecretState struct {
	ownership.State
	// Version counts writes; 0 means the name exists with no value yet.
	Version int32 `json:"version"`
	// UpdatedAt is when the value was last written (workflow time).
	UpdatedAt string `json:"updatedAt,omitempty"`
}

// RotateCmd records that the value behind this name was written. The
// door writes the value into the sealed store FIRST and then lands
// this; the command carries no value, so no history ever holds one.
type RotateCmd struct{}

// Name is the command's wire identity.
func (RotateCmd) Name() entity.CommandName { return "rotate" }

// Result binds the response type.
func (RotateCmd) Result() SecretRes { return SecretRes{} }

// SecretRes reports the secret after a rotation.
type SecretRes struct {
	Version int32 `json:"version"`
}

// ForgetActivity erases the value behind a deleted secret record:
// (ForgetReq). Served by the graphene worker over the sealed store.
const ForgetActivity = "server.secret.forget"

// ForgetReq names the value to erase.
type ForgetReq struct {
	Name string `json:"name"`
}

// NewSecret builds the secret definition.
func NewSecret() *entdefine.Definition[SecretSpec, SecretState] {
	def := entdefine.New[SecretSpec, SecretState](SecretKind,
		entdefine.WithSearchAttributes[SecretSpec, SecretState](true),
		entdefine.WithInit[SecretSpec, SecretState](func(ctx workflow.Context, _ SecretSpec) (SecretState, error) {
			var st SecretState
			ownership.Init(ctx, &st.State, "")
			return st, nil
		}),
		// The record IS the name's existence: deleting it takes the
		// value with it, or a forgotten value would outlive every trace
		// of who put it there.
		entdefine.WithFinalize[SecretSpec, SecretState](func(ctx workflow.Context, _ *SecretState) error {
			return workflow.ExecuteActivity(actx(ctx), ForgetActivity,
				ForgetReq{Name: secretName(ctx)}).Get(ctx, nil)
		}),
	)
	ownership.Register(def, func(st *SecretState) *ownership.State { return &st.State })
	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[SecretSpec, SecretState], _ RotateCmd) (SecretRes, error) {
		st := ec.State()
		st.Version++
		st.UpdatedAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
		return SecretRes{Version: st.Version}, nil
	})
	return def
}

// secretName recovers the secret's name from its workflow id.
func secretName(ctx workflow.Context) string {
	id := workflow.GetInfo(ctx).WorkflowExecution.ID
	return trimKind(id, string(SecretKind))
}

func trimKind(id, kind string) string {
	if len(id) > len(kind)+1 && id[:len(kind)+1] == kind+"/" {
		return id[len(kind)+1:]
	}
	return id
}

// actx bounds the value store's activities: they are local writes, so
// a failure that is not transient must not retry forever.
func actx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
		TaskQueue:           wire.ServerQueue,
	})
}

// Validate refuses a variable with no value.
func (c SetVarCmd) Validate() error {
	if c.Value == "" {
		return fmt.Errorf("a variable needs a value")
	}
	return nil
}
