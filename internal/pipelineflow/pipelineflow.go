// Package pipelineflow is the pipeline as an ENTITY: the record of
// what a pipeline binary IS. Its state is the last published manifest;
// its history is the version log — every content change is a lived
// "manifest published" event, an unchanged publication is a no-op.
package pipelineflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/workflow"
)

// Kind is the entity kind.
const Kind entity.KindName = "pipeline"

// Spec is empty — the record's id IS the pipeline id; everything else
// is state the publications write.
type Spec struct{}

// State holds the current manifest and the current worker image.
type State struct {
	// Manifest is graphene.manifest.v1.Manifest as protojson.
	Manifest json.RawMessage `json:"manifest,omitempty"`
	Digest   string          `json:"digest,omitempty"`
	// Image is the pipeline's current worker image — what a push
	// recorded last; runs started without an explicit image use it.
	Image string `json:"image,omitempty"`
}

// PublishCmd replaces the manifest when its content changed.
type PublishCmd struct {
	Manifest json.RawMessage `json:"manifest"`
	// Image, when set, updates the pipeline's worker image (a push);
	// empty keeps the current one (a worker start announcement).
	Image string `json:"image,omitempty"`
}

// Name is the command's wire identity.
func (PublishCmd) Name() entity.CommandName { return "publish-manifest" }

// Result binds the response type.
func (PublishCmd) Result() PublishRes { return PublishRes{} }

// PublishRes reports whether the content changed.
type PublishRes struct {
	Digest  string `json:"digest"`
	Changed bool   `json:"changed"`
}

// New builds the pipeline definition.
func New() *entdefine.Definition[Spec, State] {
	def := entdefine.New[Spec, State](Kind,
		entdefine.WithSearchAttributes[Spec, State](true),
	)
	entdefine.Handle(def, func(_ workflow.Context, ec *entdefine.Ctx[Spec, State], cmd PublishCmd) (PublishRes, error) {
		sum := sha256.Sum256(cmd.Manifest)
		digest := "sha256:" + hex.EncodeToString(sum[:])
		st := ec.State()
		if st.Digest == digest && (cmd.Image == "" || cmd.Image == st.Image) {
			return PublishRes{Digest: digest, Changed: false}, nil
		}
		if cmd.Image != "" {
			st.Image = cmd.Image
		}
		st.Manifest = cmd.Manifest
		st.Digest = digest
		return PublishRes{Digest: digest, Changed: true}, nil
	})
	return def
}
