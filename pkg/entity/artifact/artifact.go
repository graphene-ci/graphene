// Package artifact defines the Artifact system entity: a durable record
// about bytes stored elsewhere — name, owner, digest, location. The
// record is about WHERE the data is, not the data itself; deleting an
// owned record deletes its bytes too.
package artifact

import (
	"errors"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/graphene/pkg/id"
	"github.com/graphene-ci/graphene/pkg/ref"
)

// Kind is the entity kind name; workflow IDs are "artifact/{artifact-id}".
const Kind = entity.KindName("artifact")

// Spec is the desired state of an artifact record.
type Spec struct {
	Blob      ref.BlobRef       `json:"blob"`
	MediaType string            `json:"mediaType,omitempty"`
	Owner     ref.OwnerRef      `json:"owner,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// Validate checks the spec structurally.
func (s Spec) Validate() error {
	if s.Blob.Digest == "" || s.Blob.Location == "" {
		return errors.New("artifact requires blob digest and location")
	}
	if s.Owner != "" {
		return s.Owner.Validate()
	}
	return nil
}

// State is the observed state of the artifact.
type State struct {
	Verified bool `json:"verified"`
	// Blob is a teardown copy of the blob ref, set at creation: the
	// finalizer sees only State (temporal-entity limitation, candidate for
	// a chassis change).
	Blob *ref.BlobRef `json:"blob,omitempty"`
}

// Ops is the side-effect boundary: the blob store behind the records.
// Implementations live in the server; all methods must be idempotent.
type Ops interface {
	// Stat checks that the blob exists and matches the digest.
	Stat(artifactId id.ArtifactId, blob ref.BlobRef) (exists bool, err error)
	// Delete removes the bytes; not-found is not an error.
	Delete(artifactId id.ArtifactId, blob ref.BlobRef) error
}

// Activity names (registered by the server against its Ops).
const (
	StatActivity   = "artifact.stat"
	DeleteActivity = "artifact.delete"
)

// Definition builds the artifact entity definition.
func Definition() *entdefine.Definition[Spec, State] {
	return entdefine.New[Spec, State](Kind,
		entdefine.WithInit[Spec, State](func(ctx workflow.Context, spec Spec) (State, error) {
			var st State
			var exists bool
			if err := workflow.ExecuteActivity(activityCtx(ctx), StatActivity, artifactId(ctx), spec.Blob).Get(ctx, &exists); err != nil {
				return st, err
			}
			if !exists {
				return st, errors.New("blob not found at location")
			}
			st.Verified = true
			st.Blob = &spec.Blob
			return st, nil
		}),
		entdefine.WithFinalize[Spec, State](func(ctx workflow.Context, st *State) error {
			// Deleting the record deletes the bytes (owned data dies with
			// its record); the blob ref travels via the teardown copy.
			if st.Blob == nil {
				return nil
			}
			return workflow.ExecuteActivity(activityCtx(ctx), DeleteActivity, artifactId(ctx), *st.Blob).Get(ctx, nil)
		}),
		entdefine.WithSearchAttributes[Spec, State](true),
	)
}

func activityCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    30 * time.Second,
		},
	})
}

func artifactId(ctx workflow.Context) id.ArtifactId {
	full := workflow.GetInfo(ctx).WorkflowExecution.ID
	prefix := string(Kind) + "/"
	if len(full) > len(prefix) {
		return id.ArtifactId(full[len(prefix):])
	}
	return id.ArtifactId(full)
}
