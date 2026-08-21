// Package pipelineflow is the pipeline as an ENTITY: the record of
// what a pipeline binary IS. Its state is the last published manifest,
// the current worker image, and the concurrency policy; its history is
// the version log — every content change is a lived "manifest
// published" event, an unchanged publication is a no-op.
//
// The record is also the ARBITER of automatic starts: every trigger
// firing lands here as a fire command, the policy (queue /
// cancel-previous / parallel) is applied under the record's own
// serialization, and the decision is an event of this history.
package pipelineflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Kind is the entity kind.
const Kind entity.KindName = "pipeline"

// Spec is empty — the record's id IS the pipeline id; everything else
// is state the publications write.
type Spec struct{}

// State holds the current manifest, worker image, and start policy.
type State struct {
	// Manifest is graphene.manifest.v1.Manifest as protojson.
	Manifest json.RawMessage `json:"manifest,omitempty"`
	Digest   string          `json:"digest,omitempty"`
	// Image is the pipeline's current worker image — what a push
	// recorded last; runs started without an explicit image use it.
	Image string `json:"image,omitempty"`
	// Concurrency is the policy for automatic starts: "queue"
	// (default), "cancel-previous", "parallel".
	Concurrency string `json:"concurrency,omitempty"`
	// Pending is the one deferred firing under the queue policy —
	// cron semantics: firings do not pile up, the latest wins.
	Pending *Fire `json:"pending,omitempty"`
}

// Fire is one trigger firing awaiting or receiving a decision.
type Fire struct {
	Trigger string          `json:"trigger"`
	Params  json.RawMessage `json:"params,omitempty"`
	Event   json.RawMessage `json:"event,omitempty"`
}

// PublishCmd replaces the manifest when its content changed.
type PublishCmd struct {
	Manifest json.RawMessage `json:"manifest"`
	// Image, when set, updates the pipeline's worker image (a push);
	// empty keeps the current one (a worker start announcement).
	Image string `json:"image,omitempty"`
	// Concurrency is the declared policy from the manifest.
	Concurrency string `json:"concurrency,omitempty"`
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

// FireCmd is one trigger firing: the arbiter applies the policy.
type FireCmd struct {
	Trigger string          `json:"trigger"`
	Params  json.RawMessage `json:"params,omitempty"`
	Event   json.RawMessage `json:"event,omitempty"`
}

// Name is the command's wire identity.
func (FireCmd) Name() entity.CommandName { return "fire" }

// Result binds the response type.
func (FireCmd) Result() FireRes { return FireRes{} }

// FireRes reports the decision.
type FireRes struct {
	// Decision: "started" | "queued" | "replaced-previous".
	Decision string `json:"decision"`
	RunId    string `json:"runId,omitempty"`
}

// Server-side activities the arbiter drives (registered by the server
// worker on ServerQueue).
const (
	// StartActivity starts a run of the pipeline: (StartReq) -> run id.
	StartActivity = "server.run.start"
	// CountActivity counts the pipeline's running runs: (pipelineId) -> int64.
	CountActivity = "server.run.count"
	// CancelActivity cancels the pipeline's running runs: (pipelineId).
	CancelActivity = "server.run.cancel"
)

// StartReq asks the server to start one run.
type StartReq struct {
	PipelineId string          `json:"pipelineId"`
	Trigger    string          `json:"trigger"`
	Params     json.RawMessage `json:"params,omitempty"`
	Event      json.RawMessage `json:"event,omitempty"`
}

// New builds the pipeline definition. tick bounds how fast a queued
// firing starts after the live run finishes.
func New(tick time.Duration) *entdefine.Definition[Spec, State] {
	def := entdefine.New[Spec, State](Kind,
		entdefine.WithSearchAttributes[Spec, State](true),
		entdefine.WithReconcileEvery[Spec, State](tick, pendingTick),
	)
	entdefine.Handle(def, func(_ workflow.Context, ec *entdefine.Ctx[Spec, State], cmd PublishCmd) (PublishRes, error) {
		sum := sha256.Sum256(cmd.Manifest)
		digest := "sha256:" + hex.EncodeToString(sum[:])
		st := ec.State()
		same := st.Digest == digest &&
			(cmd.Image == "" || cmd.Image == st.Image) &&
			cmd.Concurrency == st.Concurrency
		if same {
			return PublishRes{Digest: digest, Changed: false}, nil
		}
		if cmd.Image != "" {
			st.Image = cmd.Image
		}
		st.Concurrency = cmd.Concurrency
		st.Manifest = cmd.Manifest
		st.Digest = digest
		return PublishRes{Digest: digest, Changed: true}, nil
	})
	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[Spec, State], cmd FireCmd) (FireRes, error) {
		st := ec.State()
		fire := Fire{Trigger: cmd.Trigger, Params: cmd.Params, Event: cmd.Event}
		running, err := countRuns(ctx, ec)
		if err != nil {
			return FireRes{}, err
		}
		policy := st.Concurrency
		if policy == "" {
			policy = "queue"
		}
		switch {
		case running == 0 || policy == "parallel":
			runId, err := start(ctx, ec, fire)
			if err != nil {
				return FireRes{}, err
			}
			return FireRes{Decision: "started", RunId: runId}, nil
		case policy == "cancel-previous":
			if err := workflow.ExecuteActivity(actx(ctx), CancelActivity, pipelineId(ctx)).Get(ctx, nil); err != nil {
				return FireRes{}, err
			}
			runId, err := start(ctx, ec, fire)
			if err != nil {
				return FireRes{}, err
			}
			return FireRes{Decision: "replaced-previous", RunId: runId}, nil
		default: // queue
			st.Pending = &fire
			return FireRes{Decision: "queued"}, nil
		}
	})
	return def
}

// pendingTick drains the one queued firing once the live run is gone.
func pendingTick(ctx workflow.Context, ec *entdefine.Ctx[Spec, State]) error {
	st := ec.State()
	if st.Pending == nil {
		return nil
	}
	running, err := countRuns(ctx, ec)
	if err != nil {
		return err
	}
	if running > 0 {
		return nil
	}
	fire := *st.Pending
	if _, err := start(ctx, ec, fire); err != nil {
		return err
	}
	st.Pending = nil
	return nil
}

func start(ctx workflow.Context, _ *entdefine.Ctx[Spec, State], fire Fire) (string, error) {
	var runId string
	err := workflow.ExecuteActivity(actx(ctx), StartActivity, StartReq{
		PipelineId: pipelineId(ctx),
		Trigger:    fire.Trigger,
		Params:     fire.Params,
		Event:      fire.Event,
	}).Get(ctx, &runId)
	return runId, err
}

func countRuns(ctx workflow.Context, _ *entdefine.Ctx[Spec, State]) (int64, error) {
	var n int64
	err := workflow.ExecuteActivity(actx(ctx), CountActivity, pipelineId(ctx)).Get(ctx, &n)
	return n, err
}

func actx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		TaskQueue:           wire.ServerQueue,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
}

// pipelineId is the record's id: the entity workflow id is "kind/id".
func pipelineId(ctx workflow.Context) string {
	return strings.TrimPrefix(workflow.GetInfo(ctx).WorkflowExecution.ID, string(Kind)+"/")
}
