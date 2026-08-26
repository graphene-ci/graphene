// Package kindflow holds the record of a KIND — the installation's
// dictionary as records, not as a hand-kept list. Every kind the
// system serves has one: the system's own kinds are declared by the
// server when a namespace starts, and the kinds a pipeline BRINGS
// (docker, k8s resources — types whose definitions live in the user's
// binary) are reconciled into the dictionary when a revision is
// activated, exactly like triggers are.
//
// A kind record is a record like any other: listed by `get kind`,
// read by `get kind/docker`, audited through its own history. Its
// controller keeps it honest — a brought kind whose pipelines are gone
// and whose records have died removes itself.
package kindflow

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Kind is the entity kind name; the record id is the described kind's
// own name ("kind/docker" describes the kind "docker"). The dictionary
// contains itself: "kind/kind" is declared like the rest.
const Kind = entity.KindName("kind")

// Origins of a kind.
const (
	// OriginSystem is a kind the server itself serves.
	OriginSystem = "system"
	// OriginBrought is a kind whose definition lives in a pipeline's
	// binary: the server can list and command its records, but only a
	// run's own worker can execute them.
	OriginBrought = "brought"
)

// Spec is what a kind record IS: where the kind comes from.
type Spec struct {
	Origin string `json:"origin"`
}

// Validate refuses an unknown origin.
func (s Spec) Validate() error {
	switch s.Origin {
	case OriginSystem, OriginBrought:
		return nil
	}
	return fmt.Errorf("origin %q: want %s or %s", s.Origin, OriginSystem, OriginBrought)
}

// Command is one command of the described kind, with the schema of its
// payload when the schema is known. A brought kind's own commands are
// unknown to the server until the SDK exports them; the chassis
// commands are known for everyone.
type Command struct {
	Name          string          `json:"name"`
	PayloadSchema json.RawMessage `json:"payloadSchema,omitempty"`
}

// State is the dictionary entry as it stands.
type State struct {
	ownership.State
	Origin string `json:"origin"`
	// Declarable says whether a caller may create the kind directly.
	Declarable  bool   `json:"declarable"`
	Description string `json:"description,omitempty"`
	// SpecSchema is the declaration's schema (schemapb protojson);
	// empty when the server does not know it.
	SpecSchema json.RawMessage `json:"specSchema,omitempty"`
	Commands   []Command       `json:"commands,omitempty"`
	// Dimensions this kind ANSWERS: which of the five observation
	// surfaces a UI should offer for it.
	Dimensions []string `json:"dimensions,omitempty"`
	// BroughtBy names the pipelines whose ACTIVE version brings this
	// kind; system kinds keep it empty.
	BroughtBy []string `json:"broughtBy,omitempty"`
	// Records is the live record count of this kind, from the last
	// audit tick.
	Records   int    `json:"records"`
	UpdatedAt string `json:"updatedAt,omitempty"`
}

// DeclareCmd is the server (re)describing a SYSTEM kind — sent at
// namespace start, so a server upgrade refreshes the dictionary the
// same way a role re-declaration refreshes its rules.
type DeclareCmd struct {
	Declarable  bool            `json:"declarable"`
	Description string          `json:"description,omitempty"`
	SpecSchema  json.RawMessage `json:"specSchema,omitempty"`
	Commands    []Command       `json:"commands,omitempty"`
	Dimensions  []string        `json:"dimensions,omitempty"`
}

// Name is the command's wire identity.
func (DeclareCmd) Name() entity.CommandName { return "declare" }

// Result binds the response type.
func (DeclareCmd) Result() Res { return Res{} }

// BringCmd is a pipeline's activation bringing this kind: the fast
// path into the dictionary. The audit tick would find it anyway; this
// makes it appear the moment the manifest does.
type BringCmd struct {
	PipelineId string `json:"pipelineId"`
	// Commands are the kind's own commands as the manifest declares
	// them.
	Commands []Command `json:"commands,omitempty"`
	// The rest is the SDK's full description (RecordKindInfo); empty
	// fields leave the entry as it stands, so a library that only
	// names its kind does not blank a richer entry.
	Description string          `json:"description,omitempty"`
	SpecSchema  json.RawMessage `json:"specSchema,omitempty"`
	Dimensions  []string        `json:"dimensions,omitempty"`
}

// Name is the command's wire identity.
func (BringCmd) Name() entity.CommandName { return "bring" }

// Result binds the response type.
func (BringCmd) Result() Res { return Res{} }

// Validate refuses an anonymous bring.
func (c BringCmd) Validate() error {
	if c.PipelineId == "" {
		return fmt.Errorf("a bring names the pipeline")
	}
	return nil
}

// Res reports the entry after a command.
type Res struct {
	Origin    string   `json:"origin"`
	BroughtBy []string `json:"broughtBy,omitempty"`
}

// AuditActivity keeps a brought entry honest: (AuditReq) -> AuditRes.
// Served by the graphene worker — counting records and reading
// pipeline manifests are side effects.
const AuditActivity = "kind.audit"

// RetireActivity asks the server to delete this kind record —
// (kindName). A workflow cannot remove itself; the server signals it
// the way any deletion is signalled.
const RetireActivity = "kind.retire"

// AuditReq asks who still brings a kind and how many records live.
type AuditReq struct {
	KindName string `json:"kindName"`
	// BroughtBy is the current claim, to be verified pipeline by
	// pipeline.
	BroughtBy []string `json:"broughtBy,omitempty"`
}

// AuditRes is the verified truth.
type AuditRes struct {
	// BroughtBy is the pruned list: pipelines that exist and whose
	// active manifest still names the kind.
	BroughtBy []string `json:"broughtBy,omitempty"`
	Records   int      `json:"records"`
}

// auditEvery is the controller's tick.
const auditEvery = time.Minute

// New builds the kind-record definition.
func New() *entdefine.Definition[Spec, State] {
	def := entdefine.New[Spec, State](Kind,
		entdefine.WithSearchAttributes[Spec, State](true),
		entdefine.WithInit[Spec, State](func(ctx workflow.Context, spec Spec) (State, error) {
			var st State
			if err := spec.Validate(); err != nil {
				return st, temporal.NewNonRetryableApplicationError(err.Error(), "BadSpec", err)
			}
			ownership.Init(ctx, &st.State, "")
			st.Origin = spec.Origin
			st.UpdatedAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
			return st, nil
		}),
		entdefine.WithReconcileEvery[Spec, State](auditEvery, audit),
	)
	ownership.Register(def, func(st *State) *ownership.State { return &st.State })

	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[Spec, State], cmd DeclareCmd) (Res, error) {
		st := ec.State()
		st.Declarable, st.Description = cmd.Declarable, cmd.Description
		st.SpecSchema, st.Commands = cmd.SpecSchema, cmd.Commands
		st.Dimensions = cmd.Dimensions
		st.UpdatedAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
		return Res{Origin: st.Origin, BroughtBy: st.BroughtBy}, nil
	})

	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[Spec, State], cmd BringCmd) (Res, error) {
		st := ec.State()
		found := false
		for _, p := range st.BroughtBy {
			if p == cmd.PipelineId {
				found = true
				break
			}
		}
		if !found {
			st.BroughtBy = append(st.BroughtBy, cmd.PipelineId)
		}
		if len(cmd.Commands) > 0 {
			st.Commands = cmd.Commands
		}
		if cmd.Description != "" {
			st.Description = cmd.Description
		}
		if len(cmd.SpecSchema) > 0 {
			st.SpecSchema = cmd.SpecSchema
		}
		if len(cmd.Dimensions) > 0 {
			st.Dimensions = cmd.Dimensions
		}
		st.UpdatedAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
		return Res{Origin: st.Origin, BroughtBy: st.BroughtBy}, nil
	})
	return def
}

// audit is the controller: refresh the record count, prune the
// bringers, and remove the entry when nothing justifies it any more.
func audit(ctx workflow.Context, ec *entdefine.Ctx[Spec, State]) error {
	st := ec.State()
	name := describedKind(ctx)
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           wire.ServerQueue,
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	var res AuditRes
	if err := workflow.ExecuteActivity(actx, AuditActivity,
		AuditReq{KindName: name, BroughtBy: st.BroughtBy}).Get(ctx, &res); err != nil {
		// The audit is a health tick: a failed one leaves the entry as
		// it stands rather than killing the record.
		return nil //nolint:nilerr // deliberate: keep the last known state
	}
	st.Records = res.Records
	if st.Origin == OriginBrought {
		st.BroughtBy = res.BroughtBy
		// Nobody brings it, nothing of it lives: the dictionary entry
		// has no subject left and removes itself.
		if len(st.BroughtBy) == 0 && st.Records == 0 {
			return workflow.ExecuteActivity(actx, RetireActivity, name).Get(ctx, nil)
		}
	}
	return nil
}

// describedKind is the kind this record speaks about — its own id.
func describedKind(ctx workflow.Context) string {
	id := workflow.GetInfo(ctx).WorkflowExecution.ID
	prefix := string(Kind) + "/"
	if len(id) > len(prefix) && id[:len(prefix)] == prefix {
		return id[len(prefix):]
	}
	return id
}
