// Package workspaceflow holds the temporal flow of a WORKSPACE: the
// project's working area and the aggregate root of its life. A
// workspace has one source — a Git checkout or an uploaded snapshot —
// a runtime, a working tree that lives on the server, and at most one
// pipeline. Deleting it is the end of the project: the tree, its
// snapshots and everything the pipeline owns go with it.
package workspaceflow

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

// Kind is the entity kind name; workflow IDs are "workspace/{id}".
const Kind = entity.KindName("workspace")

// GitSource points at a repository. Graphene does not implement Git:
// it clones with the ordinary tool and hands the checkout over.
type GitSource struct {
	Url string `json:"url"`
	// Ref is a branch, tag or commit; empty takes the default branch.
	Ref string `json:"ref,omitempty"`
	// Subdir is the pipeline's root inside a monorepo.
	Subdir string `json:"subdir,omitempty"`
	// CredentialRef names the secret holding the token or key. Only the
	// NAME travels; the value resolves at the moment of the clone.
	CredentialRef string `json:"credentialRef,omitempty"`
}

// SnapshotSource is an uploaded tree — working with pipelines without
// Git at all.
type SnapshotSource struct {
	// Location names the uploaded tar.gz in the blob store.
	Location string `json:"location"`
	Digest   string `json:"digest,omitempty"`
}

// Spec is what a workspace IS.
type Spec struct {
	// Exactly one source: a workspace is either a Git checkout or an
	// uploaded snapshot.
	Git      *GitSource      `json:"git,omitempty"`
	Snapshot *SnapshotSource `json:"snapshot,omitempty"`
	// Runtime is the toolchain of this project ("go"); the version is
	// the installation's pinned one when empty.
	Runtime string `json:"runtime,omitempty"`
	// PipelineId names the pipeline this workspace publishes; it is the
	// only child a workspace may own.
	PipelineId string `json:"pipelineId,omitempty"`
}

// Validate rejects a workspace that names no source or two.
func (s Spec) Validate() error {
	switch {
	case s.Git == nil && s.Snapshot == nil:
		return fmt.Errorf("workspace needs a source: git or snapshot")
	case s.Git != nil && s.Snapshot != nil:
		return fmt.Errorf("workspace has one source, not both")
	case s.Git != nil && s.Git.Url == "":
		return fmt.Errorf("git source needs a url")
	case s.Snapshot != nil && s.Snapshot.Location == "":
		return fmt.Errorf("snapshot source needs a location")
	}
	return nil
}

// State is the resolved working tree.
type State struct {
	ownership.State
	// TreeLocation names the CURRENT working tree (tar.gz) in the blob
	// store — what a materialization builds.
	TreeLocation string `json:"treeLocation,omitempty"`
	// TreeDigest pins its content.
	TreeDigest string `json:"treeDigest,omitempty"`
	// GitCommit is the resolved commit of a Git workspace.
	GitCommit string `json:"gitCommit,omitempty"`
	// PipelineRef names the published pipeline once there is one.
	PipelineRef string `json:"pipelineRef,omitempty"`
	// Generation counts materializations of the source into the tree.
	Generation uint64 `json:"generation,omitempty"`
	CreatedAt  string `json:"createdAt,omitempty"`
	UpdatedAt  string `json:"updatedAt,omitempty"`
}

// FetchActivity resolves a workspace's source into a working tree —
// served by the graphene worker (git clone in an ephemeral container,
// or the uploaded snapshot as it is).
const FetchActivity = "workspace.fetch"

// FetchReq asks for one resolution.
type FetchReq struct {
	WorkspaceId string `json:"workspaceId"`
	Spec        Spec   `json:"spec"`
}

// FetchRes is the resolved tree.
type FetchRes struct {
	TreeLocation string `json:"treeLocation"`
	TreeDigest   string `json:"treeDigest"`
	GitCommit    string `json:"gitCommit,omitempty"`
}

// SyncCmd re-resolves the source: a Git workspace fetches its ref
// again, a snapshot workspace takes a newly uploaded tree.
type SyncCmd struct {
	// Location, when set, replaces the tree with a fresh upload
	// (snapshot workspaces).
	Location string `json:"location,omitempty"`
	Digest   string `json:"digest,omitempty"`
}

// Name is the command's wire identity.
func (SyncCmd) Name() entity.CommandName { return "sync" }

// Result binds the response type.
func (SyncCmd) Result() TreeRes { return TreeRes{} }

// BindPipelineCmd records the pipeline this workspace published. A
// workspace publishes ONE pipeline: the second is refused.
type BindPipelineCmd struct {
	PipelineId string `json:"pipelineId"`
}

// Name is the command's wire identity.
func (BindPipelineCmd) Name() entity.CommandName { return "bind-pipeline" }

// Result binds the response type.
func (BindPipelineCmd) Result() TreeRes { return TreeRes{} }

// TreeRes reports the workspace's tree after a command.
type TreeRes struct {
	TreeLocation string `json:"treeLocation,omitempty"`
	TreeDigest   string `json:"treeDigest,omitempty"`
	GitCommit    string `json:"gitCommit,omitempty"`
	PipelineRef  string `json:"pipelineRef,omitempty"`
	Generation   uint64 `json:"generation,omitempty"`
}

// fetchTimeout bounds one source resolution.
const fetchTimeout = 15 * time.Minute

// New builds the workspace definition.
func New() *entdefine.Definition[Spec, State] {
	def := entdefine.New[Spec, State](Kind,
		entdefine.WithSearchAttributes[Spec, State](true),
		entdefine.WithInit[Spec, State](func(ctx workflow.Context, spec Spec) (State, error) {
			var st State
			if err := spec.Validate(); err != nil {
				return st, err
			}
			// A workspace is a root: it owns, nothing owns it.
			ownership.Init(ctx, &st.State, "")
			now := workflow.Now(ctx).UTC().Format(time.RFC3339)
			st.CreatedAt, st.UpdatedAt = now, now
			res, err := fetch(ctx, spec)
			if err != nil {
				return st, err
			}
			applyTree(&st, res, now)
			st.PipelineRef = spec.PipelineId
			return st, nil
		}),
	)
	ownership.Register(def, func(st *State) *ownership.State { return &st.State })

	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[Spec, State], cmd SyncCmd) (TreeRes, error) {
		st := ec.State()
		spec := ec.Spec()
		now := workflow.Now(ctx).UTC().Format(time.RFC3339)
		if cmd.Location != "" {
			// A fresh upload replaces the tree; the source spec keeps
			// naming the ORIGINAL snapshot, the state names what is
			// current — the working tree is mutable, the spec is not.
			applyTree(st, FetchRes{TreeLocation: cmd.Location, TreeDigest: cmd.Digest}, now)
			return treeRes(st), nil
		}
		res, err := fetch(ctx, spec)
		if err != nil {
			return TreeRes{}, err
		}
		applyTree(st, res, now)
		return treeRes(st), nil
	})

	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[Spec, State], cmd BindPipelineCmd) (TreeRes, error) {
		st := ec.State()
		switch {
		case cmd.PipelineId == "":
			return TreeRes{}, fmt.Errorf("pipeline id is required")
		case st.PipelineRef != "" && st.PipelineRef != cmd.PipelineId:
			return TreeRes{}, fmt.Errorf("workspace already publishes pipeline %q: a second pipeline needs its own workspace", st.PipelineRef)
		}
		st.PipelineRef = cmd.PipelineId
		st.UpdatedAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
		return treeRes(st), nil
	})
	return def
}

func fetch(ctx workflow.Context, spec Spec) (FetchRes, error) {
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           wire.ServerQueue,
		StartToCloseTimeout: fetchTimeout,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	var res FetchRes
	err := workflow.ExecuteActivity(actx, FetchActivity, FetchReq{
		WorkspaceId: workspaceIdOf(ctx),
		Spec:        spec,
	}).Get(ctx, &res)
	return res, err
}

func applyTree(st *State, res FetchRes, now string) {
	st.TreeLocation = res.TreeLocation
	st.TreeDigest = res.TreeDigest
	if res.GitCommit != "" {
		st.GitCommit = res.GitCommit
	}
	st.Generation++
	st.UpdatedAt = now
}

func treeRes(st *State) TreeRes {
	return TreeRes{
		TreeLocation: st.TreeLocation,
		TreeDigest:   st.TreeDigest,
		GitCommit:    st.GitCommit,
		PipelineRef:  st.PipelineRef,
		Generation:   st.Generation,
	}
}

func workspaceIdOf(ctx workflow.Context) string {
	full := workflow.GetInfo(ctx).WorkflowExecution.ID
	prefix := string(Kind) + "/"
	if len(full) > len(prefix) {
		return full[len(prefix):]
	}
	return full
}
