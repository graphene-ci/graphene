// Package sourceflow holds the SOURCE a pipeline is built from: a
// gitsource — a checkout of a ref. Its files are read-only, and it
// moves only by fetching the ref again. Editing it would create local
// changes on top of a commit — a tree to keep somewhere, diff against
// upstream and merge on the next sync. That is version control, and
// graphene is not one.
//
// A source lives UNDER a pipeline. The pipeline keeps what a source has
// no business owning: the triggers, the stand, the runs, and the
// version its automatic starts use.
package sourceflow

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

// GitKind is the one source kind: a checkout of a ref.
const GitKind = entity.KindName("gitsource")

// GitSpec is what a Git source IS: a repository and a ref inside it.
type GitSpec struct {
	// PipelineId is the project this source belongs to.
	PipelineId string `json:"pipelineId"`
	Url        string `json:"url"`
	// Ref is a branch, tag or commit; empty takes the default branch.
	Ref string `json:"ref,omitempty"`
	// Subdir is the pipeline's root inside a monorepo.
	Subdir string `json:"subdir,omitempty"`
	// CredentialRef names the secret holding the token or key. Only the
	// NAME travels; the value resolves at the moment of the clone.
	CredentialRef string `json:"credentialRef,omitempty"`
	// Runtime is the toolchain this code is built with ("go"); the
	// toolchain follows the CODE, so it is declared here and not on the
	// pipeline.
	Runtime string `json:"runtime,omitempty"`
}

// Validate refuses a source that names no repository.
func (s GitSpec) Validate() error {
	if s.Url == "" {
		return fmt.Errorf("a git source needs a url")
	}
	if s.PipelineId == "" {
		return fmt.Errorf("a source belongs to a pipeline: name it")
	}
	return nil
}

// GitState is the checkout as it stands.
type GitState struct {
	ownership.State
	// TreeLocation names the checkout (tar.gz) in the blob store.
	TreeLocation string `json:"treeLocation,omitempty"`
	TreeDigest   string `json:"treeDigest,omitempty"`
	// Commit is the resolved commit the tree came from.
	Commit string `json:"commit,omitempty"`
	// Generation counts fetches.
	Generation uint64 `json:"generation,omitempty"`
	UpdatedAt  string `json:"updatedAt,omitempty"`
}

// SyncCmd fetches the ref again. It carries nothing: what to fetch is
// the spec's business, and a Git source has no other way to move.
type SyncCmd struct{}

// Name is the command's wire identity.
func (SyncCmd) Name() entity.CommandName { return "sync" }

// Result binds the response type.
func (SyncCmd) Result() GitRes { return GitRes{} }

// GitRes reports the checkout after a fetch.
type GitRes struct {
	TreeLocation string `json:"treeLocation,omitempty"`
	TreeDigest   string `json:"treeDigest,omitempty"`
	Commit       string `json:"commit,omitempty"`
	Generation   uint64 `json:"generation,omitempty"`
}

// FetchActivity resolves a Git source into a checkout — served by the
// graphene worker (a clone in an ephemeral container).
const FetchActivity = "source.git.fetch"

// FetchReq asks for one checkout.
type FetchReq struct {
	SourceId string  `json:"sourceId"`
	Spec     GitSpec `json:"spec"`
}

// FetchRes is the resolved checkout.
type FetchRes struct {
	TreeLocation string `json:"treeLocation"`
	TreeDigest   string `json:"treeDigest"`
	Commit       string `json:"commit,omitempty"`
}

// fetchTimeout bounds one clone.
const fetchTimeout = 15 * time.Minute

// NewGit builds the gitsource definition.
func NewGit() *entdefine.Definition[GitSpec, GitState] {
	def := entdefine.New[GitSpec, GitState](GitKind,
		entdefine.WithSearchAttributes[GitSpec, GitState](true),
		entdefine.WithInit[GitSpec, GitState](func(ctx workflow.Context, spec GitSpec) (GitState, error) {
			var st GitState
			if err := spec.Validate(); err != nil {
				return st, temporal.NewNonRetryableApplicationError(err.Error(), "BadSpec", err)
			}
			ownership.Init(ctx, &st.State, ref.OwnerRef("pipeline/"+spec.PipelineId))
			res, err := fetch(ctx, spec)
			if err != nil {
				return st, err
			}
			applyCheckout(&st, res, workflow.Now(ctx))
			return st, nil
		}),
		entdefine.WithFinalize[GitSpec, GitState](func(ctx workflow.Context, _ *GitState) error {
			return sweep(ctx, idOf(ctx, GitKind))
		}),
	)
	ownership.Register(def, func(st *GitState) *ownership.State { return &st.State })
	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[GitSpec, GitState], _ SyncCmd) (GitRes, error) {
		res, err := fetch(ctx, ec.Spec())
		if err != nil {
			return GitRes{}, err
		}
		st := ec.State()
		applyCheckout(st, res, workflow.Now(ctx))
		return GitRes{
			TreeLocation: st.TreeLocation, TreeDigest: st.TreeDigest,
			Commit: st.Commit, Generation: st.Generation,
		}, nil
	})
	return def
}

func fetch(ctx workflow.Context, spec GitSpec) (FetchRes, error) {
	fctx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           wire.ServerQueue,
		StartToCloseTimeout: fetchTimeout,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	var res FetchRes
	err := workflow.ExecuteActivity(fctx, FetchActivity, FetchReq{
		SourceId: idOf(ctx, GitKind), Spec: spec,
	}).Get(ctx, &res)
	return res, err
}

func applyCheckout(st *GitState, res FetchRes, now time.Time) {
	st.TreeLocation, st.TreeDigest = res.TreeLocation, res.TreeDigest
	if res.Commit != "" {
		st.Commit = res.Commit
	}
	st.Generation++
	st.UpdatedAt = now.UTC().Format(time.RFC3339)
}

// sweep erases everything the source owns.
func sweep(ctx workflow.Context, sourceId string) error {
	sctx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           wire.ServerQueue,
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
	})
	return workflow.ExecuteActivity(sctx, SweepActivity, SweepReq{Prefix: BlobPrefix(sourceId)}).Get(ctx, nil)
}

// idOf recovers a source's id from its workflow id.
func idOf(ctx workflow.Context, kind entity.KindName) string {
	full := workflow.GetInfo(ctx).WorkflowExecution.ID
	prefix := string(kind) + "/"
	if len(full) > len(prefix) && full[:len(prefix)] == prefix {
		return full[len(prefix):]
	}
	return full
}

// SweepActivity erases the blobs of a deleted record: (SweepReq). A
// record's bytes are its own — the checkout — and nothing outside names
// them, so deleting the record has to take them along or they stay
// forever with no way back to them.
const SweepActivity = "source.blobs.sweep"

// SweepReq names the prefix to erase.
type SweepReq struct {
	Prefix string `json:"prefix"`
}

// BlobPrefix is where one source keeps everything it owns.
func BlobPrefix(sourceId string) string { return "sources/" + sourceId + "/" }
