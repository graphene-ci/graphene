// Package revisionflow holds the temporal flow of a PIPELINE REVISION:
// one immutable materialized build of a source tree. The build IS the
// record's Init — so it survives a server restart, deduplicates by
// source digest through the ordinary create-or-attach, cancels by
// deletion, and keeps its log where every record keeps telemetry.
package revisionflow

import (
	"fmt"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Kind is the entity kind name; workflow IDs are
// "revision/{pipelineId}.{revisionId}".
const Kind = entity.KindName("revision")

// Id renders the record id of one revision.
func Id(pipelineId, revisionId string) entity.ResourceID {
	return entity.ResourceID(pipelineId + "." + revisionId)
}

// Spec is what a revision IS: the source it was built from and the
// runtime that built it. Immutable — a different source is a different
// revision.
type Spec struct {
	PipelineId string `json:"pipelineId"`
	// SourceLocation names the uploaded tree (tar.gz) in the blob store.
	SourceLocation string `json:"sourceLocation"`
	// SourceDigest pins the tree's content.
	SourceDigest string `json:"sourceDigest"`
	// Runtime names the toolchain image that builds it; empty takes the
	// installation's default.
	Runtime string `json:"runtime,omitempty"`
}

// State is what materialization produced.
type State struct {
	ownership.State
	// Image is the worker OCI reference (content-tagged).
	Image string `json:"image,omitempty"`
	// ManifestLocation names the manifest blob — what Plan renders.
	ManifestLocation string `json:"manifestLocation,omitempty"`
	// LogLocation names the build log blob (diagnostics).
	LogLocation string `json:"logLocation,omitempty"`
	// Error carries the failure of a build that did not produce an
	// image; the record stays for its diagnostics.
	Error string `json:"error,omitempty"`
	// CreatedAt is when the build started — workflow time, replay-safe.
	CreatedAt string `json:"createdAt,omitempty"`
}

// MaterializeActivity builds one revision — served by the graphene
// worker over its Materializer.
const MaterializeActivity = "revision.materialize"

// MaterializeReq asks for one build.
type MaterializeReq struct {
	PipelineId     string `json:"pipelineId"`
	RevisionId     string `json:"revisionId"`
	SourceLocation string `json:"sourceLocation"`
	Runtime        string `json:"runtime,omitempty"`
}

// MaterializeRes is what the build produced.
type MaterializeRes struct {
	Image            string `json:"image"`
	ManifestLocation string `json:"manifestLocation"`
	LogLocation      string `json:"logLocation,omitempty"`
}

// buildTimeout bounds one materialization; a toolchain that hangs must
// not hold the record forever.
const buildTimeout = 30 * time.Minute

// New builds the revision definition.
func New() *entdefine.Definition[Spec, State] {
	def := entdefine.New[Spec, State](Kind,
		entdefine.WithSearchAttributes[Spec, State](true),
		entdefine.WithInit[Spec, State](initRevision),
	)
	ownership.Register(def, func(st *State) *ownership.State { return &st.State })
	return def
}

func initRevision(ctx workflow.Context, spec Spec) (State, error) {
	var st State
	if spec.PipelineId == "" || spec.SourceLocation == "" {
		return st, fmt.Errorf("revision needs a pipeline and a source")
	}
	// The revision belongs to its pipeline: deleting the pipeline takes
	// its builds with it.
	ownership.Init(ctx, &st.State, ref.OwnerRef("pipeline/"+spec.PipelineId))
	st.CreatedAt = workflow.Now(ctx).UTC().Format(time.RFC3339)

	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           wire.ServerQueue,
		StartToCloseTimeout: buildTimeout,
		HeartbeatTimeout:    2 * time.Minute,
		// A build is deterministic: a failing source fails again. One
		// retry covers an infrastructure blip, no more.
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 2},
	})
	var res MaterializeRes
	err := workflow.ExecuteActivity(actx, MaterializeActivity, MaterializeReq{
		PipelineId:     spec.PipelineId,
		RevisionId:     revisionIdOf(ctx, spec.PipelineId),
		SourceLocation: spec.SourceLocation,
		Runtime:        spec.Runtime,
	}).Get(ctx, &res)
	if err != nil {
		// The record survives its failure: the build log and the error
		// are the diagnostics a developer opens.
		st.Error = err.Error()
		return st, err
	}
	st.Image = res.Image
	st.ManifestLocation = res.ManifestLocation
	st.LogLocation = res.LogLocation
	return st, nil
}

// revisionIdOf recovers the revision half of the record id.
func revisionIdOf(ctx workflow.Context, pipelineId string) string {
	full := workflow.GetInfo(ctx).WorkflowExecution.ID
	prefix := string(Kind) + "/" + pipelineId + "."
	if len(full) > len(prefix) {
		return full[len(prefix):]
	}
	return full
}
