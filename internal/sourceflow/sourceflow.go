// Package sourceflow holds the two kinds of SOURCE a pipeline can be
// built from. They are separate kinds because they differ in what may
// be DONE to them, not in the value of a field:
//
//   - a gitsource is a checkout of a ref. Its files are read-only, and
//     it moves only by fetching the ref again. Editing it would create
//     local changes on top of a commit — a tree to keep somewhere, diff
//     against upstream and merge on the next sync. That is version
//     control, and graphene is not one.
//   - a managedsource is the project's own tree. Every file is written
//     in place, each write is durable, and its generation counts them.
//
// Both live UNDER a pipeline. The pipeline keeps what a source has no
// business owning: the triggers, the stand, the runs, and the version
// its automatic starts use.
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

// The kinds of the source contour.
const (
	GitKind     = entity.KindName("gitsource")
	ManagedKind = entity.KindName("managedsource")
)

// --- gitsource ---

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

// --- managedsource ---

// ManagedSpec is what a managed source IS: where its FIRST tree came
// from. After that the files are its own — the spec never describes
// them again.
type ManagedSpec struct {
	// PipelineId is the project this source belongs to.
	PipelineId string `json:"pipelineId"`
	// From names another source to copy the initial tree from
	// ("gitsource/main"). This is how Git-sourced code becomes
	// editable: a copy, with provenance, that never syncs back.
	From string `json:"from,omitempty"`
	// Upload names an uploaded tar.gz to start from instead.
	Upload string `json:"upload,omitempty"`
	// Runtime is the toolchain this code is built with.
	Runtime string `json:"runtime,omitempty"`
	// Origin records where a copy came from. It is provenance, not a
	// link: nothing syncs, and the upstream never learns about it.
	Origin *Origin `json:"origin,omitempty"`
}

// Origin is the provenance of a copied tree.
type Origin struct {
	Url    string `json:"url,omitempty"`
	Ref    string `json:"ref,omitempty"`
	Commit string `json:"commit,omitempty"`
	// Source names the record it was copied from.
	Source string `json:"source,omitempty"`
}

// Validate refuses a source that names two beginnings.
func (s ManagedSpec) Validate() error {
	switch {
	case s.PipelineId == "":
		return fmt.Errorf("a source belongs to a pipeline: name it")
	case s.From != "" && s.Upload != "":
		return fmt.Errorf("a managed source starts from one place: from or upload, not both")
	}
	return nil
}

// ManagedState is the tree as it stands. The FILES are not here: they
// are blobs of their own, and the index names them. A write touches
// one file and the index, never the whole tree.
type ManagedState struct {
	ownership.State
	// IndexLocation names the index blob: path -> {blob, size, digest}.
	IndexLocation string `json:"indexLocation,omitempty"`
	// TreeDigest pins the whole tree — the index's own digest.
	TreeDigest string `json:"treeDigest,omitempty"`
	// Files counts what the index holds.
	Files int `json:"files"`
	// Generation counts writes.
	Generation uint64 `json:"generation,omitempty"`
	UpdatedAt  string `json:"updatedAt,omitempty"`
	// History is the recent generations, newest last. Every index ever
	// written is still in the store — they are immutable and addressed
	// by digest — so a generation here is enough to go back to it. The
	// list is bounded: a record's state is read on every describe, and
	// an unbounded one would grow without end. The full account stays
	// in the record's own history.
	History []Generation `json:"history,omitempty"`
}

// Generation is one past state of the tree.
type Generation struct {
	Generation    uint64 `json:"generation"`
	IndexLocation string `json:"indexLocation"`
	TreeDigest    string `json:"treeDigest"`
	Files         int    `json:"files"`
	At            string `json:"at"`
}

// historyKept bounds how far back a revert can reach through the
// record's state.
const historyKept = 20

// WriteCmd records one change of the tree: the door has already
// written the file blob and the new index, so the command carries
// their names — never their bytes.
type WriteCmd struct {
	IndexLocation string `json:"indexLocation"`
	TreeDigest    string `json:"treeDigest"`
	Files         int    `json:"files"`
}

// Name is the command's wire identity.
func (WriteCmd) Name() entity.CommandName { return "write" }

// Result binds the response type.
func (WriteCmd) Result() ManagedRes { return ManagedRes{} }

// Validate refuses a change that names no index.
func (c WriteCmd) Validate() error {
	if c.IndexLocation == "" {
		return fmt.Errorf("a write names the new index")
	}
	return nil
}

// RevertCmd puts a past generation back. It moves FORWARD: the old
// tree becomes a new generation, so the history stays an account of
// what happened rather than being rewritten.
type RevertCmd struct {
	// Generation names which past state to restore.
	Generation uint64 `json:"generation"`
}

// Name is the command's wire identity.
func (RevertCmd) Name() entity.CommandName { return "revert" }

// Result binds the response type.
func (RevertCmd) Result() ManagedRes { return ManagedRes{} }

// Validate refuses a revert that names nothing.
func (c RevertCmd) Validate() error {
	if c.Generation == 0 {
		return fmt.Errorf("name the generation to restore")
	}
	return nil
}

// ManagedRes reports the tree after a change.
type ManagedRes struct {
	IndexLocation string `json:"indexLocation,omitempty"`
	TreeDigest    string `json:"treeDigest,omitempty"`
	Files         int    `json:"files"`
	Generation    uint64 `json:"generation,omitempty"`
}

// AdoptActivity lays the initial tree out file by file and writes the
// index — served by the graphene worker.
const AdoptActivity = "source.managed.adopt"

// AdoptReq asks for one initial layout.
type AdoptReq struct {
	SourceId string      `json:"sourceId"`
	Spec     ManagedSpec `json:"spec"`
}

// AdoptRes is the laid-out tree.
type AdoptRes struct {
	IndexLocation string `json:"indexLocation"`
	TreeDigest    string `json:"treeDigest"`
	Files         int    `json:"files"`
}

// NewManaged builds the managedsource definition.
func NewManaged() *entdefine.Definition[ManagedSpec, ManagedState] {
	def := entdefine.New[ManagedSpec, ManagedState](ManagedKind,
		entdefine.WithSearchAttributes[ManagedSpec, ManagedState](true),
		entdefine.WithInit[ManagedSpec, ManagedState](func(ctx workflow.Context, spec ManagedSpec) (ManagedState, error) {
			var st ManagedState
			if err := spec.Validate(); err != nil {
				return st, temporal.NewNonRetryableApplicationError(err.Error(), "BadSpec", err)
			}
			ownership.Init(ctx, &st.State, ref.OwnerRef("pipeline/"+spec.PipelineId))
			// An empty source is legitimate: a project may start from a
			// blank tree and grow by writing files.
			actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				TaskQueue:           wire.ServerQueue,
				StartToCloseTimeout: fetchTimeout,
				HeartbeatTimeout:    2 * time.Minute,
				RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
			})
			var res AdoptRes
			if err := workflow.ExecuteActivity(actx, AdoptActivity,
				AdoptReq{SourceId: idOf(ctx, ManagedKind), Spec: spec}).Get(ctx, &res); err != nil {
				return st, err
			}
			st.IndexLocation, st.TreeDigest, st.Files = res.IndexLocation, res.TreeDigest, res.Files
			st.Generation = 1
			st.UpdatedAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
			return st, nil
		}),
		entdefine.WithFinalize[ManagedSpec, ManagedState](func(ctx workflow.Context, _ *ManagedState) error {
			return sweep(ctx, idOf(ctx, ManagedKind))
		}),
	)
	ownership.Register(def, func(st *ManagedState) *ownership.State { return &st.State })
	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[ManagedSpec, ManagedState], cmd WriteCmd) (ManagedRes, error) {
		return advance(ctx, ec.State(), cmd.IndexLocation, cmd.TreeDigest, cmd.Files), nil
	})
	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[ManagedSpec, ManagedState], cmd RevertCmd) (ManagedRes, error) {
		st := ec.State()
		for _, g := range st.History {
			if g.Generation == cmd.Generation {
				return advance(ctx, st, g.IndexLocation, g.TreeDigest, g.Files), nil
			}
		}
		err := fmt.Errorf("generation %d is not in the last %d of this source", cmd.Generation, historyKept)
		return ManagedRes{}, temporal.NewNonRetryableApplicationError(err.Error(), "NoSuchGeneration", err)
	})
	return def
}

// advance makes one new generation of the tree and remembers the one
// it replaced.
func advance(ctx workflow.Context, st *ManagedState, indexLocation, treeDigest string, files int) ManagedRes {
	now := workflow.Now(ctx).UTC().Format(time.RFC3339)
	if st.IndexLocation != "" {
		st.History = append(st.History, Generation{
			Generation: st.Generation, IndexLocation: st.IndexLocation,
			TreeDigest: st.TreeDigest, Files: st.Files, At: st.UpdatedAt,
		})
		if len(st.History) > historyKept {
			st.History = st.History[len(st.History)-historyKept:]
		}
	}
	st.IndexLocation, st.TreeDigest, st.Files = indexLocation, treeDigest, files
	st.Generation++
	st.UpdatedAt = now
	return ManagedRes{
		IndexLocation: st.IndexLocation, TreeDigest: st.TreeDigest,
		Files: st.Files, Generation: st.Generation,
	}
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
// record's bytes are its own — old file versions, old indexes, the
// checkout — and nothing outside names them, so deleting the record
// has to take them along or they stay forever with no way back to
// them.
const SweepActivity = "source.blobs.sweep"

// SweepReq names the prefix to erase.
type SweepReq struct {
	Prefix string `json:"prefix"`
}

// BlobPrefix is where one source keeps everything it owns.
func BlobPrefix(sourceId string) string { return "sources/" + sourceId + "/" }
