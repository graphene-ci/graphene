// Package worker assembles the server's Temporal worker: the system
// entity flows from the pipeline repository, their Ops activities, and
// the server-queue activities the pipeline library calls
// (declare/delete/ensure/user-data). This worker talks to Temporal
// directly — it IS the server; everyone else goes through the proxy.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gopherex/xlog"
	"io"
	"strings"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	agentpb "github.com/graphene-ci/agent/pkg/proto/agent/v1"
	"github.com/graphene-ci/graphene/internal/agents"
	"github.com/graphene-ci/graphene/internal/authz"
	"github.com/graphene-ci/graphene/internal/infrastructure/blob"
	"github.com/graphene-ci/graphene/internal/kindflow"
	syslabels "github.com/graphene-ci/graphene/internal/labels"
	"github.com/graphene-ci/graphene/internal/materialize"
	"github.com/graphene-ci/graphene/internal/nsflow"
	"github.com/graphene-ci/graphene/internal/ops"
	"github.com/graphene-ci/graphene/internal/pipelineflow"
	"github.com/graphene-ci/graphene/internal/rbacflow"
	"github.com/graphene-ci/graphene/internal/revisionflow"
	"github.com/graphene-ci/graphene/internal/secrets"
	"github.com/graphene-ci/graphene/internal/sourceflow"
	"github.com/graphene-ci/graphene/internal/standflow"
	"github.com/graphene-ci/graphene/internal/triggerflow"
	"github.com/graphene-ci/graphene/internal/valueflow"
	agentflow "github.com/graphene-ci/pipeline/pkg/flow/agent"
	"github.com/graphene-ci/pipeline/pkg/flow/artifact"
	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/obs"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	manifestpb "github.com/graphene-ci/pipeline/pkg/proto/manifest/v1"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
	"google.golang.org/protobuf/encoding/protojson"
)

// Deps is everything the worker needs — one worker per NAMESPACE, its
// client bound to the namespace's Temporal namespace.
type Deps struct {
	Namespace   string
	Client      client.Client
	Registry    *agents.Registry
	AgentOps    *ops.AgentOps
	ArtifactOps *ops.ArtifactOps
	External    string
	// StandTick is how often stands check their holdings for expiry.
	StandTick time.Duration
	// RunToken is handed to machine containers so their worker passes the
	// Temporal proxy. (Per-run minted tokens replace this static one.)
	RunToken string
	// Materializer builds source revisions; nil disables the
	// source-first contour on this installation.
	Materializer *materialize.Materializer
	// Blobs holds uploaded sources, manifests and build logs.
	Blobs blob.Store
	// Secrets resolves this namespace's secret values (git credentials).
	Secrets secrets.Store
	// SecretWriter is the sealed store the secret RECORDS keep their
	// values in; nil disables writing them.
	SecretWriter *secrets.Namespaced
	Log          *xlog.Logger
}

// RunStarter starts one run — wired by the bundle after the managed
// runner exists (the same path the management door uses).
type RunStarter func(ctx context.Context, runId, pipelineId string, params []byte, image string, labels map[string]string, trigger string) error

// Worker is the assembled server worker.
type Worker struct {
	w            worker.Worker
	deps         Deps
	agentDef     *entdefine.Definition[pipeline.AgentSpec, agentflow.State]
	artifactDef  *entdefine.Definition[pipeline.ArtifactSpec, artifact.State]
	standDef     *entdefine.Definition[standflow.Spec, standflow.State]
	pipelineDef  *entdefine.Definition[pipelineflow.Spec, pipelineflow.State]
	triggerDef   *entdefine.Definition[triggerflow.Spec, triggerflow.State]
	revisionDef  *entdefine.Definition[revisionflow.Spec, revisionflow.State]
	roleDef      *entdefine.Definition[rbacflow.RoleSpec, rbacflow.RoleState]
	bindingDef   *entdefine.Definition[rbacflow.BindingSpec, rbacflow.BindingState]
	accountDef   *entdefine.Definition[rbacflow.AccountSpec, rbacflow.AccountState]
	varDef       *entdefine.Definition[valueflow.VarSpec, valueflow.VarState]
	secretDef    *entdefine.Definition[valueflow.SecretSpec, valueflow.SecretState]
	namespaceDef *entdefine.Definition[nsflow.Spec, nsflow.State]
	gitSourceDef *entdefine.Definition[sourceflow.GitSpec, sourceflow.GitState]
	kindDef      *entdefine.Definition[kindflow.Spec, kindflow.State]

	// varCache holds variable values for a moment — one submit resolves
	// several names, and each is a record read.
	varCache *valueCache
	// secretWriter is the sealed store behind the secret records; nil
	// on an installation without one.
	secretWriter *secrets.Namespaced

	// kinds is what this installation knows how to declare and command
	// — the vocabulary a generic Apply and a UI both read.
	kinds map[string]*kindEntry

	startRun RunStarter
	// ensureHost brings a run-queue worker container up — the managed
	// runner behind a hook, because the runner is built after this
	// worker. Serves resurrection and the brought-kind apply host.
	ensureHost HostEnsurer
	// ensureNs and retireNs are the bundle manager's half of the
	// namespace record; nil on a worker that is not the default one.
	ensureNs func(ctx context.Context, name string, retentionDays int32) error
	retireNs func(ctx context.Context, name string) error
}

// HostEnsurer makes sure the worker container of one run queue exists
// and polls — minting the run's token on the way. revived reports
// whether anything had to be brought back (an alive worker is a no-op).
type HostEnsurer func(ctx context.Context, runId id.RunId, image string) (revived bool, err error)

// SetHostEnsurer wires the managed runner's ensure path; called by the
// bundle once the runner exists.
func (s *Worker) SetHostEnsurer(fn HostEnsurer) { s.ensureHost = fn }

// SetNamespaceOps wires what only the bundle manager can do: register
// a namespace and stop serving one. The manager builds this worker, so
// the wiring arrives after construction.
func (s *Worker) SetNamespaceOps(ensure func(ctx context.Context, name string, retentionDays int32) error, retire func(ctx context.Context, name string) error) {
	s.ensureNs, s.retireNs = ensure, retire
}

// SetRunStarter wires the start path for trigger-driven runs; called by
// the bundle once the managed runner exists.
func (s *Worker) SetRunStarter(fn RunStarter) { s.startRun = fn }

// New builds and registers everything; Run starts polling.
func New(deps Deps) (*Worker, error) {
	// Every activity of every system record is observable through the
	// SDK's one interceptor (subject, logs, metrics, retries), plus the
	// Temporal OTel one for spans — the same pair every other worker
	// runs, so a system record and a library record answer dimensions
	// 3-5 identically.
	workerInterceptors := []interceptor.WorkerInterceptor{}
	if tracing, terr := temporalotel.NewTracingInterceptor(temporalotel.TracerOptions{}); terr == nil {
		workerInterceptors = append(workerInterceptors, tracing)
	}
	// obs sits INSIDE tracing so the open span gets the subject.
	workerInterceptors = append(workerInterceptors, &obs.Interceptor{Contour: "server"})
	w := worker.New(deps.Client, wire.ServerQueue, worker.Options{
		Interceptors: workerInterceptors,
	})
	standTick := deps.StandTick
	if standTick == 0 {
		standTick = 30 * time.Second
	}
	s := &Worker{
		w:            w,
		deps:         deps,
		agentDef:     agentflow.Definition(agentflow.Options{}),
		artifactDef:  artifact.Definition(),
		standDef:     standflow.New(standTick),
		pipelineDef:  pipelineflow.New(standTick),
		triggerDef:   triggerflow.New(standTick),
		revisionDef:  revisionflow.New(),
		roleDef:      rbacflow.NewRole(),
		bindingDef:   rbacflow.NewBinding(),
		accountDef:   rbacflow.NewAccount(),
		varDef:       valueflow.NewVar(),
		secretDef:    valueflow.NewSecret(),
		namespaceDef: nsflow.New(),
		gitSourceDef: sourceflow.NewGit(),
		kindDef:      kindflow.New(),
		kinds:        buildKinds(),
		varCache:     newValueCache(3 * time.Second),
		secretWriter: deps.SecretWriter,
	}

	if err := errors.Join(
		s.agentDef.Register(w), s.artifactDef.Register(w),
		s.standDef.Register(w), s.pipelineDef.Register(w),
		s.triggerDef.Register(w), s.revisionDef.Register(w),
		s.roleDef.Register(w), s.bindingDef.Register(w), s.accountDef.Register(w),
		s.varDef.Register(w), s.secretDef.Register(w),
		s.namespaceDef.Register(w),
		s.gitSourceDef.Register(w),
		s.kindDef.Register(w),
	); err != nil {
		return nil, err
	}

	// Ops behind the flows.
	w.RegisterActivityWithOptions(deps.AgentOps.InstallSSH, activity.RegisterOptions{Name: agentflow.InstallSSHActivity})
	w.RegisterActivityWithOptions(deps.AgentOps.AgentStatus, activity.RegisterOptions{Name: agentflow.AgentStatusActivity})
	w.RegisterActivityWithOptions(deps.ArtifactOps.Stat, activity.RegisterOptions{Name: artifact.StatActivity})
	w.RegisterActivityWithOptions(deps.ArtifactOps.Delete, activity.RegisterOptions{Name: artifact.DeleteActivity})

	// The server-queue contract the pipeline library calls.
	w.RegisterActivityWithOptions(s.declareAgent, activity.RegisterOptions{Name: wire.DeclareAgentActivity})
	w.RegisterActivityWithOptions(s.declareArtifact, activity.RegisterOptions{Name: wire.DeclareArtifactActivity})
	w.RegisterActivityWithOptions(s.deleteResource, activity.RegisterOptions{Name: wire.DeleteResourceActivity})
	w.RegisterActivityWithOptions(s.ensureContainer, activity.RegisterOptions{Name: wire.EnsureContainerActivity})
	w.RegisterActivityWithOptions(s.runCleanup, activity.RegisterOptions{Name: wire.RunCleanupActivity})
	w.RegisterActivityWithOptions(s.agentUserData, activity.RegisterOptions{Name: wire.AgentUserDataActivity})
	w.RegisterActivityWithOptions(s.attachAgent, activity.RegisterOptions{Name: wire.AttachAgentActivity})
	w.RegisterActivityWithOptions(s.attachArtifact, activity.RegisterOptions{Name: wire.AttachArtifactActivity})
	w.RegisterActivityWithOptions(s.selectAgents, activity.RegisterOptions{Name: wire.SelectAgentsActivity})
	w.RegisterActivityWithOptions(s.publishCapability, activity.RegisterOptions{Name: wire.PublishCapabilityActivity})
	w.RegisterActivityWithOptions(s.transferResource, activity.RegisterOptions{Name: wire.TransferResourceActivity})
	w.RegisterActivityWithOptions(s.standCascade, activity.RegisterOptions{Name: standflow.CascadeActivity})
	w.RegisterActivityWithOptions(s.runActive, activity.RegisterOptions{Name: standflow.RunActiveActivity})
	// The trigger contour: the arbiter's arms and the firing path.
	w.RegisterActivityWithOptions(s.triggerFire, activity.RegisterOptions{Name: triggerflow.FireActivity})
	w.RegisterActivityWithOptions(s.autoStartRun, activity.RegisterOptions{Name: pipelineflow.StartActivity})
	w.RegisterActivityWithOptions(s.countRuns, activity.RegisterOptions{Name: pipelineflow.CountActivity})
	w.RegisterActivityWithOptions(s.cancelRuns, activity.RegisterOptions{Name: pipelineflow.CancelActivity})
	w.RegisterActivityWithOptions(s.resolveRevision, activity.RegisterOptions{Name: pipelineflow.ResolveActivity})
	w.RegisterActivityWithOptions(s.forgetSecret, activity.RegisterOptions{Name: valueflow.ForgetActivity})
	w.RegisterActivityWithOptions(s.ensureNamespace, activity.RegisterOptions{Name: nsflow.EnsureActivity})
	w.RegisterActivityWithOptions(s.retireNamespace, activity.RegisterOptions{Name: nsflow.RetireActivity})
	// The source contour: a checkout is fetched.
	w.RegisterActivityWithOptions(s.fetchGitSource, activity.RegisterOptions{Name: sourceflow.FetchActivity})
	// The dictionary polices itself through these two.
	w.RegisterActivityWithOptions(s.auditKind, activity.RegisterOptions{Name: kindflow.AuditActivity})
	w.RegisterActivityWithOptions(s.retireKind, activity.RegisterOptions{Name: kindflow.RetireActivity})
	// Deleting a record takes its bytes with it.
	w.RegisterActivityWithOptions(s.sweepSource, activity.RegisterOptions{Name: sourceflow.SweepActivity})
	w.RegisterActivityWithOptions(s.sweepRevision, activity.RegisterOptions{Name: revisionflow.SweepActivity})
	w.RegisterActivityWithOptions(s.sweepPipeline, activity.RegisterOptions{Name: pipelineflow.SweepActivity})
	w.RegisterActivityWithOptions(s.reconcileTriggersAct, activity.RegisterOptions{Name: pipelineflow.ReconcileTriggersActivity})
	// The source-first contour: the revision record's Init calls this.
	w.RegisterActivityWithOptions(s.materializeRevision, activity.RegisterOptions{Name: revisionflow.MaterializeActivity})
	return s, nil
}

// triggerFire lands one firing on the pipeline record — the arbiter.
func (s *Worker) triggerFire(ctx context.Context, req triggerflow.FireRequest) error {
	pipelines := entclient.Bind(s.pipelineDef, s.deps.Client, wire.ServerQueue)
	_, err := entclient.ExecWithStart(ctx, pipelines, entity.ResourceID(req.PipelineId),
		pipelineflow.Spec{}, pipelineflow.FireCmd{Trigger: req.Trigger, Params: req.Params, Event: req.Event})
	return err
}

// resolveRevision reads what one revision holds — the image it built
// and the manifest it left in the blob store. The pipeline record
// calls this while activating, so the manifest never travels through a
// command payload into that record's history.
func (s *Worker) resolveRevision(ctx context.Context, req pipelineflow.ResolveReq) (pipelineflow.Activation, error) {
	ph, _, st, err := s.DescribeRevision(ctx, req.PipelineId, req.RevisionId)
	if err != nil {
		return pipelineflow.Activation{}, err
	}
	if ph != entity.PhaseReady || st.Image == "" {
		return pipelineflow.Activation{}, fmt.Errorf("revision %s is %s, not a built image", req.RevisionId, ph)
	}
	raw, err := s.blobBytes(ctx, st.ManifestLocation)
	if err != nil {
		return pipelineflow.Activation{}, fmt.Errorf("manifest of %s: %w", req.RevisionId, err)
	}
	var m manifestpb.Manifest
	if err := protojson.Unmarshal(raw, &m); err != nil {
		return pipelineflow.Activation{}, fmt.Errorf("manifest: %w", err)
	}
	// A revision that declares a trigger this pipeline cannot serve
	// never becomes the active one.
	if err := validateTriggers(&m); err != nil {
		return pipelineflow.Activation{}, temporal.NewNonRetryableApplicationError(err.Error(), "BadManifest", err)
	}
	return pipelineflow.Activation{Image: st.Image, Manifest: raw, Concurrency: m.GetConcurrency()}, nil
}

// reconcileTriggersAct lands a manifest's declarations as records:
// its triggers, and its kinds into the dictionary — both are records
// of their own, so making them match is a side effect, not a decision.
func (s *Worker) reconcileTriggersAct(ctx context.Context, req pipelineflow.ReconcileReq) error {
	var m manifestpb.Manifest
	if err := protojson.Unmarshal(req.Manifest, &m); err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	if err := s.reconcileKindRecords(ctx, req.PipelineId, m.GetKinds(), m.GetKindDecls()); err != nil {
		return err
	}
	return s.reconcileTriggers(ctx, req.PipelineId, m.GetTriggers())
}

// blobBytes reads one blob whole.
func (s *Worker) blobBytes(ctx context.Context, location string) ([]byte, error) {
	if location == "" || s.deps.Blobs == nil {
		return nil, fmt.Errorf("no manifest was stored")
	}
	rc, err := s.deps.Blobs.Get(ctx, s.deps.Namespace, location)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(rc)
}

// FirePipeline lands one firing on the pipeline record and returns the
// arbiter's decision — the ONE path a run starts by, whoever asked.
func (s *Worker) FirePipeline(ctx context.Context, pipelineId string, cmd pipelineflow.FireCmd) (pipelineflow.FireRes, error) {
	pipelines := entclient.Bind(s.pipelineDef, s.deps.Client, wire.ServerQueue)
	return entclient.ExecWithStart(ctx, pipelines, entity.ResourceID(pipelineId), pipelineflow.Spec{}, cmd)
}

// autoStartRun starts a trigger-decided run: the image from the
// pipeline record, the webhook body merged into the reserved "event"
// params field.
func (s *Worker) autoStartRun(ctx context.Context, req pipelineflow.StartReq) (string, error) {
	if s.startRun == nil {
		return "", fmt.Errorf("run starter is not wired yet")
	}
	st, err := s.GetPipeline(ctx, req.PipelineId)
	if err != nil {
		return "", err
	}
	params := req.Params
	if len(req.Event) > 0 {
		merged := map[string]json.RawMessage{}
		if len(params) > 0 {
			if err := json.Unmarshal(params, &merged); err != nil {
				return "", fmt.Errorf("trigger params: %w", err)
			}
		}
		merged["event"] = req.Event
		if params, err = json.Marshal(merged); err != nil {
			return "", err
		}
	}
	// A person's firing carries no trigger name; it still goes through
	// the arbiter, so the concurrency policy applies to humans exactly
	// as it applies to cron.
	fired := req.Trigger
	if fired == "" {
		fired = "manual"
	}
	runId := req.RunId
	if runId == "" {
		runId = fmt.Sprintf("%s-%s-%s", req.PipelineId, fired,
			time.Now().UTC().Format("20060102-150405"))
	}
	trigger := triggerLabelValue(st.Manifest, req.Trigger)
	if err := s.startRun(ctx, runId, req.PipelineId, params, st.Image, req.Labels, trigger); err != nil {
		return "", err
	}
	return runId, nil
}

// triggerLabelValue renders "kind:name" for the run's trigger label —
// the kind comes from the published manifest; a trigger the manifest
// no longer names keeps just its name.
func triggerLabelValue(rawManifest json.RawMessage, name string) string {
	var m manifestpb.Manifest
	if len(rawManifest) > 0 && protojson.Unmarshal(rawManifest, &m) == nil {
		for _, tr := range m.GetTriggers() {
			if tr.GetName() == name {
				return tr.GetKind() + ":" + name
			}
		}
	}
	return name
}

// countRuns counts the pipeline's running runs via visibility.
func (s *Worker) countRuns(ctx context.Context, pipelineId string) (int64, error) {
	resp, err := s.deps.Client.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Query: fmt.Sprintf("WorkflowType = %q AND ExecutionStatus = 'Running'", pipelineId),
	})
	if err != nil {
		return 0, err
	}
	return resp.GetCount(), nil
}

// cancelRuns cancels every running run of the pipeline (the
// cancel-previous policy); teardown still runs on each.
func (s *Worker) cancelRuns(ctx context.Context, pipelineId string) error {
	list, err := s.deps.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Query: fmt.Sprintf("WorkflowType = %q AND ExecutionStatus = 'Running'", pipelineId),
	})
	if err != nil {
		return err
	}
	for _, ex := range list.GetExecutions() {
		if err := s.deps.Client.CancelWorkflow(ctx, ex.GetExecution().GetWorkflowId(), ""); err != nil {
			s.deps.Log.Warn("cancel-previous", xlog.String("workflow", ex.GetExecution().GetWorkflowId()), xlog.Err(err))
		}
	}
	return nil
}

// runActive reports whether a workflow (a run) still runs — the
// stand's guard against tearing a holding from under its origin.
func (s *Worker) runActive(ctx context.Context, workflowId string) (bool, error) {
	desc, err := s.deps.Client.DescribeWorkflowExecution(ctx, workflowId, "")
	if err != nil {
		// An unknown record is not a live run.
		return false, nil
	}
	return desc.GetWorkflowExecutionInfo().GetStatus() == enums.WORKFLOW_EXECUTION_STATUS_RUNNING, nil
}

// standCascade is the stand's teardown arm: subtree first, then the
// resource itself.
func (s *Worker) standCascade(ctx context.Context, held string) error {
	if err := s.CascadeDelete(ctx, ref.OwnerRef(held)); err != nil {
		return err
	}
	return s.DeleteOne(ctx, held)
}

// PublishManifest records what a pipeline binary is (lazy entity, dedup
// by content inside). A non-empty image also updates the pipeline's
// current worker image — that is what a push records.
func (s *Worker) PublishManifest(ctx context.Context, pipelineId string, raw json.RawMessage, image string) error {
	return s.publishManifest(ctx, pipelineId, raw, image)
}

func (s *Worker) publishManifest(ctx context.Context, pipelineId string, raw json.RawMessage, image string) error {
	var m manifestpb.Manifest
	if err := protojson.Unmarshal(raw, &m); err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	if err := validateTriggers(&m); err != nil {
		return err
	}
	pipelines := entclient.Bind(s.pipelineDef, s.deps.Client, wire.ServerQueue)
	_, err := entclient.ExecWithStart(ctx, pipelines, entity.ResourceID(pipelineId),
		pipelineflow.Spec{},
		pipelineflow.PublishCmd{Manifest: raw, Image: image, Concurrency: m.GetConcurrency()})
	if err != nil {
		return err
	}
	if err := s.reconcileKindRecords(ctx, pipelineId, m.GetKinds(), m.GetKindDecls()); err != nil {
		return err
	}
	return s.reconcileTriggers(ctx, pipelineId, m.GetTriggers())
}

// validateTriggers rejects declarations the pipeline cannot serve: a
// webhook needs the reserved "event" params field to land its body in.
func validateTriggers(m *manifestpb.Manifest) error {
	hasEvent := false
	for _, f := range m.GetParamsSchema().GetFields() {
		if f.GetName() == "event" {
			hasEvent = true
		}
	}
	for _, t := range m.GetTriggers() {
		if (t.GetKind() == "webhook" || t.GetKind() == "pipeline") && !hasEvent {
			return fmt.Errorf("%s trigger %q: the params type needs an `event json.RawMessage` field for the payload", t.GetKind(), t.GetName())
		}
		if t.GetKind() == "pipeline" {
			upstream, _, _ := strings.Cut(t.GetSpec(), ":")
			if upstream == m.GetPipelineId() {
				return fmt.Errorf("trigger %q: a pipeline cannot be its own upstream", t.GetName())
			}
		}
	}
	return nil
}

// reconcileTriggers makes the trigger records match the declarations:
// create the new, delete the vanished, recreate the changed (a spec is
// immutable — the firing history restarts with it).
func (s *Worker) reconcileTriggers(ctx context.Context, pipelineId string, declared []*manifestpb.Trigger) error {
	triggers := entclient.Bind(s.triggerDef, s.deps.Client, wire.ServerQueue)
	want := map[entity.ResourceID]triggerflow.Spec{}
	for _, t := range declared {
		want[triggerflow.Id(pipelineId, t.GetName())] = triggerflow.Spec{
			PipelineId: pipelineId,
			Kind:       t.GetKind(),
			Name:       t.GetName(),
			Spec:       t.GetSpec(),
			SecretName: t.GetSecretName(),
			Params:     t.GetParams(),
		}
	}
	// The existing records of this pipeline, by the ownership mirror.
	list, err := s.deps.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Query: fmt.Sprintf("%s = 'trigger' AND %s = %q AND ExecutionStatus = 'Running'",
			entdefine.SearchAttrKind.GetName(), wire.SearchAttrOwner.GetName(), "pipeline/"+pipelineId),
	})
	if err != nil {
		return err
	}
	existing := map[entity.ResourceID]bool{}
	for _, ex := range list.GetExecutions() {
		rid := entity.ResourceID(strings.TrimPrefix(ex.GetExecution().GetWorkflowId(), string(triggerflow.Kind)+"/"))
		existing[rid] = true
		spec, ok := want[rid]
		if !ok {
			// Vanished from the code — the next push unapplies it.
			if err := triggers.Delete(ctx, rid); err != nil {
				return fmt.Errorf("trigger %s: %w", rid, err)
			}
			continue
		}
		desc, err := triggers.Describe(ctx, rid)
		if err == nil && !specEqual(desc.Spec, spec) {
			// Changed: recreate (specs are immutable).
			if err := triggers.Delete(ctx, rid); err != nil {
				return fmt.Errorf("trigger %s: %w", rid, err)
			}
			existing[rid] = false
		}
	}
	for rid, spec := range want {
		if existing[rid] {
			continue
		}
		if _, err := triggers.CreateOrAttach(ctx, rid, spec,
			entclient.WithLabels(map[string]string{syslabels.Pipeline: pipelineId})); err != nil {
			return fmt.Errorf("trigger %s: %w", rid, err)
		}
	}
	return nil
}

// GetPipeline reads the pipeline record's state.
func (s *Worker) GetPipeline(ctx context.Context, pipelineId string) (pipelineflow.State, error) {
	pipelines := entclient.Bind(s.pipelineDef, s.deps.Client, wire.ServerQueue)
	desc, err := pipelines.Describe(ctx, entity.ResourceID(pipelineId))
	if err != nil {
		return pipelineflow.State{}, err
	}
	return desc.State, nil
}

// DescribePipelineFull reads a pipeline record whole — its declared
// source as well as its current state.
func (s *Worker) DescribePipelineFull(ctx context.Context, pipelineId string) (entity.Phase, pipelineflow.Spec, pipelineflow.State, error) {
	pipelines := entclient.Bind(s.pipelineDef, s.deps.Client, wire.ServerQueue)
	out, err := pipelines.Describe(ctx, entity.ResourceID(pipelineId))
	if err != nil {
		return "", pipelineflow.Spec{}, pipelineflow.State{}, err
	}
	return out.Phase, out.Spec, out.State, nil
}

// materializeRevision is the activity behind a revision's Init: read
// the uploaded source, build it, keep the log. Progress goes out as
// heartbeats — the record's own liveness, readable by anyone watching
// it, with no connection held open anywhere.
func (s *Worker) materializeRevision(ctx context.Context, req revisionflow.MaterializeReq) (revisionflow.MaterializeRes, error) {
	var res revisionflow.MaterializeRes
	if s.deps.Materializer == nil || s.deps.Blobs == nil {
		return res, fmt.Errorf("materialization is not configured on this installation")
	}
	rc, err := s.deps.Blobs.Get(ctx, s.deps.Namespace, req.SourceLocation)
	if err != nil {
		return res, fmt.Errorf("source %s: %w", req.SourceLocation, err)
	}
	src, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return res, fmt.Errorf("source %s: %w", req.SourceLocation, err)
	}

	out, buildErr := s.deps.Materializer.Materialize(ctx, s.deps.Namespace, req.PipelineId, req.RevisionId, req.Runtime, src,
		func(stage, message string) {
			activity.RecordHeartbeat(ctx, stage+": "+message)
		})
	// The log is diagnostics: it is kept for a failed build too — that
	// is the whole point of the record surviving its failure.
	if out.BuildLog != "" {
		location := fmt.Sprintf("revisions/%s/%s/build.log", req.PipelineId, req.RevisionId)
		if _, err := s.deps.Blobs.Put(ctx, s.deps.Namespace, location, strings.NewReader(out.BuildLog)); err == nil {
			res.LogLocation = location
		}
	}
	if buildErr != nil {
		return res, buildErr
	}
	res.Image = out.ImageRef
	res.ManifestLocation = out.ManifestLocation
	// The binary's own name must be the record's: the door starts runs
	// by the RECORD's name, and a worker that answers to another one
	// leaves every run hanging on an empty queue. Refusing here turns
	// a silent forever-Running into a build error.
	if raw, rerr := s.blobBytes(ctx, out.ManifestLocation); rerr == nil {
		var m struct {
			PipelineId string `json:"pipelineId"`
		}
		if json.Unmarshal(raw, &m) == nil && m.PipelineId != "" && m.PipelineId != req.PipelineId {
			return res, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("the binary declares pipeline %q, the record is %q: pipeline.Main's name must match the record", m.PipelineId, req.PipelineId),
				"WrongPipelineName", nil)
		}
	}
	return res, nil
}

// DeclareRevision creates (or attaches to) one revision record: the
// build IS its Init, so the same source digest attaches to the running
// or finished build instead of starting a second one.
func (s *Worker) DeclareRevision(ctx context.Context, pipelineId, revisionId string, spec revisionflow.Spec) error {
	revisions := entclient.Bind(s.revisionDef, s.deps.Client, wire.ServerQueue)
	raw, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	_, err = revisions.CreateOrAttach(ctx, revisionflow.Id(pipelineId, revisionId), spec,
		entclient.WithLabels(s.marksFor(string(revisionflow.Kind), raw)))
	return err
}

// DescribeRevision reads one revision record.
func (s *Worker) DescribeRevision(ctx context.Context, pipelineId, revisionId string) (entity.Phase, revisionflow.Spec, revisionflow.State, error) {
	revisions := entclient.Bind(s.revisionDef, s.deps.Client, wire.ServerQueue)
	out, err := revisions.Describe(ctx, revisionflow.Id(pipelineId, revisionId))
	if err != nil {
		return "", revisionflow.Spec{}, revisionflow.State{}, err
	}
	return out.Phase, out.Spec, out.State, nil
}

// --- the authorization store (authz.Store) ---

// Roles returns the namespace's roles: the built-ins, then whatever
// the installation wrote over them.
func (s *Worker) Roles(ctx context.Context, namespace string) (map[string]authz.Rules, error) {
	out := authz.Builtins()
	ids, err := s.listKind(ctx, string(rbacflow.RoleKind))
	if err != nil {
		return out, err
	}
	roles := entclient.Bind(s.roleDef, s.deps.Client, wire.ServerQueue)
	for _, id := range ids {
		desc, err := roles.Describe(ctx, entity.ResourceID(id))
		if err != nil || desc.Phase != entity.PhaseReady {
			continue
		}
		out[id] = desc.State.Rules
	}
	return out, nil
}

// Bindings returns the namespace's role bindings.
func (s *Worker) Bindings(ctx context.Context, namespace string) ([]authz.Binding, error) {
	ids, err := s.listKind(ctx, string(rbacflow.BindingKind))
	if err != nil {
		return nil, err
	}
	bindings := entclient.Bind(s.bindingDef, s.deps.Client, wire.ServerQueue)
	out := make([]authz.Binding, 0, len(ids))
	for _, id := range ids {
		desc, err := bindings.Describe(ctx, entity.ResourceID(id))
		if err != nil || desc.Phase != entity.PhaseReady {
			continue
		}
		st := desc.State
		out = append(out, authz.Binding{Role: st.Role, Subjects: st.Subjects, Namespace: st.Namespace})
	}
	return out, nil
}

// listKind lists the ids of one kind from visibility.
func (s *Worker) listKind(ctx context.Context, kind string) ([]string, error) {
	page, err := s.deps.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Query: fmt.Sprintf("%s = %q AND ExecutionStatus = 'Running'", entdefine.SearchAttrKind.GetName(), kind),
	})
	if err != nil {
		return nil, err
	}
	prefix := kind + "/"
	out := make([]string, 0, len(page.GetExecutions()))
	for _, e := range page.GetExecutions() {
		id := e.GetExecution().GetWorkflowId()
		if len(id) > len(prefix) {
			out = append(out, id[len(prefix):])
		}
	}
	return out, nil
}

// DeclareRole writes one role; its rules are validated by the record.
func (s *Worker) DeclareRole(ctx context.Context, name string, spec rbacflow.RoleSpec) error {
	roles := entclient.Bind(s.roleDef, s.deps.Client, wire.ServerQueue)
	if _, err := roles.CreateOrAttach(ctx, entity.ResourceID(name), spec); err != nil {
		return err
	}
	// An existing role takes the new rules through its own command, so
	// the change lands in its history.
	_, err := entclient.Exec(ctx, roles, entity.ResourceID(name), rbacflow.SetRulesCmd{Rules: spec.Rules})
	return err
}

// DeclareBinding writes one role binding.
func (s *Worker) DeclareBinding(ctx context.Context, name string, spec rbacflow.BindingSpec) error {
	bindings := entclient.Bind(s.bindingDef, s.deps.Client, wire.ServerQueue)
	if _, err := bindings.CreateOrAttach(ctx, entity.ResourceID(name), spec); err != nil {
		return err
	}
	_, err := entclient.Exec(ctx, bindings, entity.ResourceID(name), rbacflow.SetSubjectsCmd{Subjects: spec.Subjects})
	return err
}

// DeclareAccount creates a service account.
func (s *Worker) DeclareAccount(ctx context.Context, name string, spec rbacflow.AccountSpec) error {
	accounts := entclient.Bind(s.accountDef, s.deps.Client, wire.ServerQueue)
	_, err := accounts.CreateOrAttach(ctx, entity.ResourceID(name), spec)
	return err
}

// IssueAccountToken records a minted token on its account, by hash.
func (s *Worker) IssueAccountToken(ctx context.Context, account string, cmd rbacflow.IssueTokenCmd) error {
	accounts := entclient.Bind(s.accountDef, s.deps.Client, wire.ServerQueue)
	_, err := entclient.Exec(ctx, accounts, entity.ResourceID(account), cmd)
	return err
}

// RevokeAccountToken forgets one token of an account.
func (s *Worker) RevokeAccountToken(ctx context.Context, account, tokenId string) error {
	accounts := entclient.Bind(s.accountDef, s.deps.Client, wire.ServerQueue)
	_, err := entclient.Exec(ctx, accounts, entity.ResourceID(account), rbacflow.RevokeTokenCmd{Id: tokenId})
	return err
}

// AccountForToken finds the service account a token hash belongs to.
func (s *Worker) AccountForToken(ctx context.Context, hash string) (string, bool) {
	ids, err := s.listKind(ctx, string(rbacflow.ServiceAccountKind))
	if err != nil {
		return "", false
	}
	accounts := entclient.Bind(s.accountDef, s.deps.Client, wire.ServerQueue)
	now := time.Now()
	for _, id := range ids {
		desc, err := accounts.Describe(ctx, entity.ResourceID(id))
		if err != nil {
			continue
		}
		for _, t := range desc.State.Tokens {
			if t.Hash != hash {
				continue
			}
			if t.Expires != "" {
				if exp, err := time.Parse(time.RFC3339, t.Expires); err == nil && now.After(exp) {
					continue
				}
			}
			return id, true
		}
	}
	return "", false
}

// Note lands one domain event in a record's own history. Used for the
// AUDIT of mutations: who did what to this record, kept where the
// record keeps everything else. Best effort by design — an audit line
// must never fail the operation it describes; a record that is gone
// has nothing to audit.
func (s *Worker) Note(ctx context.Context, ref string, event any) {
	payload, err := json.Marshal(event)
	if err != nil {
		return
	}
	if err := s.deps.Client.SignalWorkflow(ctx, ref, "", entity.NoteSignalName, json.RawMessage(payload)); err != nil {
		s.deps.Log.Debug("audit note not delivered",
			xlog.String("record", ref), xlog.Err(err))
	}
}

// RevisionProgress reads the build's last heartbeat — the record's own
// liveness, no connection held anywhere. Empty when nothing is
// building or the detail is unreadable.
func (s *Worker) RevisionProgress(ctx context.Context, pipelineId, revisionId string) string {
	workflowId := string(revisionflow.Kind) + "/" + string(revisionflow.Id(pipelineId, revisionId))
	desc, err := s.deps.Client.DescribeWorkflowExecution(ctx, workflowId, "")
	if err != nil {
		return ""
	}
	for _, pa := range desc.GetPendingActivities() {
		if pa.GetActivityType().GetName() != revisionflow.MaterializeActivity {
			continue
		}
		var beat string
		if err := converter.GetDefaultDataConverter().FromPayloads(pa.GetHeartbeatDetails(), &beat); err == nil {
			return beat
		}
	}
	return ""
}

// PublishCapability writes a capability onto an agent's record — also
// the HTTP door's implementation.
func (s *Worker) PublishCapability(ctx context.Context, agentId id.AgentId, capability pipeline.Capability) error {
	agents := entclient.Bind(s.agentDef, s.deps.Client, wire.ServerQueue)
	_, err := entclient.Exec(ctx, agents, entity.ResourceID(agentId), agentflow.PublishCapabilityCmd{Capability: capability})
	return err
}

// publishCapability is the activity form.
func (s *Worker) publishCapability(ctx context.Context, agentId id.AgentId, capability pipeline.Capability) error {
	return s.PublishCapability(ctx, agentId, capability)
}

// attachAgent recognizes an EXISTING agent: no record — an error, never
// a creation. It waits until the agent is ready AND the needs are met.
// AgentExists reports whether an agent record is declared in this
// namespace — the guard against a valid token opening a session for a
// machine no pipeline ever declared. A pipeline declares the record
// before the agent connects, so a legitimate agent always finds one.
func (s *Worker) AgentExists(ctx context.Context, agentId id.AgentId) bool {
	agents := entclient.Bind(s.agentDef, s.deps.Client, wire.ServerQueue)
	_, err := agents.Describe(ctx, entity.ResourceID(agentId))
	return err == nil
}

func (s *Worker) attachAgent(ctx context.Context, agentId id.AgentId, needs []wire.NeedSpec) (pipeline.AgentState, error) {
	ctx = obs.WithEntity(ctx, "agent/"+string(agentId))
	// An attach WAITS for a record it never creates; say so, or the
	// wait reads as silence.
	obs.Info(ctx, "attach: waiting for the agent record to exist and be ready")
	agents := entclient.Bind(s.agentDef, s.deps.Client, wire.ServerQueue)
	rid := entity.ResourceID(agentId)
	for {
		out, err := agents.Describe(ctx, rid)
		if err != nil {
			return pipeline.AgentState{}, fmt.Errorf("agent %s is not known here: %w", agentId, err)
		}
		switch out.Phase {
		case entity.PhaseReady:
			if pipeline.NeedsSatisfied(needs, out.State.Capabilities) {
				return out.State.AgentState, nil
			}
			activity.RecordHeartbeat(ctx, "waiting for capabilities")
		case entity.PhaseCreating:
			activity.RecordHeartbeat(ctx, string(out.Phase))
		case entity.PhaseDeleting, entity.PhaseDeleted, entity.PhaseDeleteFailed, entity.PhaseCreateFailed:
			return pipeline.AgentState{}, fmt.Errorf("agent %s is going away (phase %s)", agentId, out.Phase)
		}
		select {
		case <-ctx.Done():
			return pipeline.AgentState{}, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// attachArtifact recognizes an EXISTING artifact.
func (s *Worker) attachArtifact(ctx context.Context, artifactId id.ArtifactId) (pipeline.ArtifactState, error) {
	ctx = obs.WithEntity(ctx, "artifact/"+string(artifactId))
	obs.Info(ctx, "attach: waiting for the artifact record to exist")
	artifacts := entclient.Bind(s.artifactDef, s.deps.Client, wire.ServerQueue)
	rid := entity.ResourceID(artifactId)
	for {
		out, err := artifacts.Describe(ctx, rid)
		if err != nil {
			return pipeline.ArtifactState{}, fmt.Errorf("artifact %s is not known here: %w", artifactId, err)
		}
		switch out.Phase {
		case entity.PhaseReady:
			return out.State.ArtifactState, nil
		case entity.PhaseCreating:
			activity.RecordHeartbeat(ctx, string(out.Phase))
		case entity.PhaseDeleting, entity.PhaseDeleted, entity.PhaseDeleteFailed, entity.PhaseCreateFailed:
			return pipeline.ArtifactState{}, fmt.Errorf("artifact %s is going away (phase %s)", artifactId, out.Phase)
		}
		select {
		case <-ctx.Done():
			return pipeline.ArtifactState{}, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// selectAgents lists the agents matching the selector — a snapshot.
func (s *Worker) selectAgents(ctx context.Context, sel wire.AgentSelector) ([]id.AgentId, error) {
	ids, err := listRunning(ctx, s.deps.Client, agentflow.Kind)
	if err != nil {
		return nil, err
	}
	agents := entclient.Bind(s.agentDef, s.deps.Client, wire.ServerQueue)
	var out []id.AgentId
	for _, rid := range ids {
		desc, err := agents.Describe(ctx, rid)
		if err != nil || desc.Phase != entity.PhaseReady {
			continue
		}
		if !labelsMatch(sel.Labels, desc.Spec.Labels) {
			continue
		}
		if !pipeline.NeedsSatisfied(sel.Needs, desc.State.Capabilities) {
			continue
		}
		out = append(out, id.AgentId(rid))
	}
	return out, nil
}

// recordLabels validates the user's labels and adds the system markers:
// graphene.io/run is the creating run, taken from the calling workflow.
func recordLabels(ctx context.Context, user map[string]string) (map[string]string, error) {
	if err := wire.ValidateUserLabels(user); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(user)+1)
	for k, v := range user {
		out[k] = v
	}
	if activity.IsActivity(ctx) {
		info := activity.GetInfo(ctx)
		out[wire.LabelRun] = strings.TrimPrefix(info.WorkflowExecution.ID, "run/")
		// A run's workflow TYPE is its pipeline: a resource created by
		// a run belongs to that project, and asking "what did this
		// project leave behind" must not require walking two records.
		out = syslabels.Merge(out, map[string]string{syslabels.Pipeline: info.WorkflowType.Name})
	}
	return out, nil
}

func labelsMatch(want, have map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

// transferResource is the activity form of Transfer. The calling
// workflow (the run doing ToStand) is stamped as the transfer's
// origin: the stand will not tear the holding down from under it.
func (s *Worker) transferResource(ctx context.Context, req wire.TransferResourceRequest) error {
	ctx = obs.WithEntity(ctx, string(req.Resource))
	if req.From == "" {
		req.From = activity.GetInfo(ctx).WorkflowExecution.ID
	}
	return s.Transfer(ctx, req)
}

// Transfer gives a resource to a new owner through the entity's own
// transfer command: typed for the system kinds, by wire identity for
// every kind that registered the command (library kinds included).
//
// A resource declared inside an ownership chain moves as ONE tree:
// naming any node hands over the chain's ROOT — the subtree follows by
// ownership. Declared dependencies are indivisible (a vm on a stand
// must not strand its subnet under a finishing run); resources with
// separate fates are simply not chained.
func (s *Worker) Transfer(ctx context.Context, req wire.TransferResourceRequest) error {
	standId, toStand := strings.CutPrefix(string(req.NewOwner), "stand/")
	if toStand {
		// The climb is a STAND semantic: handing over survivors moves
		// the whole declared tree. A point re-parenting (adopting a
		// declared child) stays point-wise.
		req.Resource = s.transferRoot(ctx, req.Resource)
	}
	kind, resource, ok := strings.Cut(string(req.Resource), "/")
	if !ok {
		return fmt.Errorf("resource ref %q: want kind/id", req.Resource)
	}
	if req.Keep > 0 && !toStand {
		return fmt.Errorf("KeepFor is a stand-stay bound; owner %q is not a stand", req.NewOwner)
	}
	if err := s.transferCmd(ctx, kind, resource, req); err != nil {
		return err
	}
	if toStand {
		// The resource remembers WHERE it is parked. The owner edge
		// already says it, but an owner changes again on release and
		// the marker is what a listing filters by — "what is standing
		// on this stand" must be one query, not a tree walk.
		if err := entclient.SetLabelsRaw(ctx, s.deps.Client, string(req.Resource),
			map[string]string{syslabels.Stand: standId}); err != nil {
			s.deps.Log.Warn("stand marker not set",
				xlog.String("resource", string(req.Resource)), xlog.Err(err))
		}
		// The stand LIVES the handover: its own timer enforces the
		// keep, its history records it.
		stands := entclient.Bind(s.standDef, s.deps.Client, wire.ServerQueue)
		// The stand is the PIPELINE's: the pipeline is the project, so
		// what stands beside it is its child.
		_, err := entclient.ExecWithStart(ctx, stands, entity.ResourceID(standId),
			standflow.Spec{PipelineId: standId},
			standflow.AcceptCmd{Ref: req.Resource, Keep: req.Keep, From: req.From},
			entclient.WithLabels(map[string]string{syslabels.Pipeline: standId}))
		return err
	}
	return nil
}

// transferRoot climbs the ancestor chain of a resource: an owner that
// is itself a RESOURCE record continues the climb; run, stand,
// pipeline, or no owner ends it. Best effort — an unreadable record
// transfers what was named.
func (s *Worker) transferRoot(ctx context.Context, res ref.OwnerRef) ref.OwnerRef {
	cur := res
	for range 32 {
		owner, err := s.ownerOf(ctx, string(cur))
		if err != nil || owner == "" {
			return cur
		}
		kind, _, ok := strings.Cut(owner, "/")
		if !ok || kind == "run" || kind == "stand" || kind == "pipeline" {
			return cur
		}
		if cur != res {
			s.deps.Log.Info("transfer climbs the ownership chain",
				xlog.String("named", string(res)), xlog.String("via", string(cur)))
		}
		cur = ref.OwnerRef(owner)
	}
	return cur
}

// ownerOf reads a record's EntityOwner from visibility — no workflow
// query, no worker woken.
func (s *Worker) ownerOf(ctx context.Context, workflowId string) (string, error) {
	desc, err := s.deps.Client.DescribeWorkflowExecution(ctx, workflowId, "")
	if err != nil {
		return "", err
	}
	fields := desc.GetWorkflowExecutionInfo().GetSearchAttributes().GetIndexedFields()
	payload, ok := fields[wire.SearchAttrOwner.GetName()]
	if !ok {
		return "", nil
	}
	var owner string
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &owner); err != nil {
		return "", err
	}
	return owner, nil
}

// transferCmd lands transfer-owner on the resource itself.
func (s *Worker) transferCmd(ctx context.Context, kind, resource string, req wire.TransferResourceRequest) error {
	cmd := ownership.TransferCmd{NewOwner: req.NewOwner, Keep: req.Keep}
	switch entity.KindName(kind) {
	case agentflow.Kind:
		agents := entclient.Bind(s.agentDef, s.deps.Client, wire.ServerQueue)
		_, err := entclient.Exec(ctx, agents, entity.ResourceID(resource), cmd)
		return err
	case artifact.Kind:
		artifacts := entclient.Bind(s.artifactDef, s.deps.Client, wire.ServerQueue)
		_, err := entclient.Exec(ctx, artifacts, entity.ResourceID(resource), cmd)
		return err
	default:
		payload, err := json.Marshal(cmd)
		if err != nil {
			return err
		}
		_, err = entclient.ExecRaw(ctx, s.deps.Client, string(req.Resource), wire.TransferOwnerCmdName, payload, "")
		return err
	}
}

// Run polls until ctx ends.
func (s *Worker) Run(ctx context.Context) error {
	err := s.w.Run(interruptFromContext(ctx))
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func interruptFromContext(ctx context.Context) <-chan any {
	ch := make(chan any, 1)
	go func() {
		<-ctx.Done()
		ch <- struct{}{}
	}()
	return ch
}

// declareAgent creates (or attaches to) the machine entity and blocks
// until it is ready, heartbeating while it converges.
func (s *Worker) declareAgent(ctx context.Context, agentId id.AgentId, spec pipeline.AgentSpec) (pipeline.AgentState, error) {
	// Called FROM a run, ABOUT an agent: the subject is the agent.
	ctx = obs.WithEntity(ctx, "agent/"+string(agentId))
	machines := entclient.Bind(s.agentDef, s.deps.Client, wire.ServerQueue)
	rid := entity.ResourceID(agentId)
	labels, err := recordLabels(ctx, spec.Labels)
	if err != nil {
		return pipeline.AgentState{}, err
	}
	if _, err := machines.CreateOrAttach(ctx, rid, spec, entclient.WithLabels(labels)); err != nil {
		return pipeline.AgentState{}, err
	}
	for {
		out, err := machines.Describe(ctx, rid)
		if err != nil {
			return pipeline.AgentState{}, err
		}
		switch out.Phase {
		case entity.PhaseReady:
			if pipeline.NeedsSatisfied(spec.Needs, out.State.Capabilities) {
				return out.State.AgentState, nil
			}
			activity.RecordHeartbeat(ctx, "waiting for capabilities")
		case entity.PhaseCreateFailed:
			return pipeline.AgentState{}, fmt.Errorf("agent %s failed to create", agentId)
		case entity.PhaseDeleting, entity.PhaseDeleted, entity.PhaseDeleteFailed:
			return pipeline.AgentState{}, fmt.Errorf("machine %s is going away (phase %s)", agentId, out.Phase)
		case entity.PhaseCreating:
			// still converging — keep polling
		}
		activity.RecordHeartbeat(ctx, string(out.Phase))
		select {
		case <-ctx.Done():
			return pipeline.AgentState{}, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// declareArtifact creates the artifact entity and waits for verification.
func (s *Worker) declareArtifact(ctx context.Context, artifactId id.ArtifactId, spec pipeline.ArtifactSpec) (pipeline.ArtifactState, error) {
	ctx = obs.WithEntity(ctx, "artifact/"+string(artifactId))
	artifacts := entclient.Bind(s.artifactDef, s.deps.Client, wire.ServerQueue)
	rid := entity.ResourceID(artifactId)
	labels, err := recordLabels(ctx, spec.Labels)
	if err != nil {
		return pipeline.ArtifactState{}, err
	}
	if _, err := artifacts.CreateOrAttach(ctx, rid, spec, entclient.WithLabels(labels)); err != nil {
		return pipeline.ArtifactState{}, err
	}
	for {
		out, err := artifacts.Describe(ctx, rid)
		if err != nil {
			return pipeline.ArtifactState{}, err
		}
		switch out.Phase {
		case entity.PhaseReady:
			return out.State.ArtifactState, nil
		case entity.PhaseCreateFailed:
			return pipeline.ArtifactState{}, fmt.Errorf("artifact %s failed verification", artifactId)
		case entity.PhaseDeleting, entity.PhaseDeleted, entity.PhaseDeleteFailed:
			return pipeline.ArtifactState{}, fmt.Errorf("artifact %s is going away (phase %s)", artifactId, out.Phase)
		case entity.PhaseCreating:
			// still converging — keep polling
		}
		activity.RecordHeartbeat(ctx, string(out.Phase))
		select {
		case <-ctx.Done():
			return pipeline.ArtifactState{}, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// CascadeDelete deletes everything owned by owner, deepest first — the
// sweeper's entry point and the run cleanup's engine. The tree is read
// from the EntityOwner search attribute: one visibility query per node;
// children go before their parent (the vm before the subnet before the
// network).
func (s *Worker) CascadeDelete(ctx context.Context, owner ref.OwnerRef) error {
	return s.cascade(ctx, owner)
}

// DeleteOne deletes one resource by its workflow id and waits for it to
// close.
func (s *Worker) DeleteOne(ctx context.Context, workflowId string) error {
	if err := s.deps.Client.SignalWorkflow(ctx, workflowId, "", entity.DeleteSignalName, nil); err != nil {
		// A record that is already gone IS deleted. Teardown has to be
		// idempotent: a stand whose holding vanished some other way
		// would otherwise retry forever and never finish deleting
		// itself, taking the whole project's deletion with it.
		if alreadyGone(err) {
			return nil
		}
		return err
	}
	return awaitClosed(ctx, s.deps.Client, workflowId)
}

// alreadyGone reports whether an error means "there is nothing there
// any more" — a finished or never-existing execution.
func alreadyGone(err error) bool {
	var notFound *serviceerror.NotFound
	return errors.As(err, &notFound)
}

// deleteResource is the activity form of the cascade.
func (s *Worker) deleteResource(ctx context.Context, owner ref.OwnerRef) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	return s.cascade(ctx, owner)
}

// cascade deletes everything owned by owner (recursively), deepest
// first; the owner itself is the CALLER's business.
func (s *Worker) cascade(ctx context.Context, owner ref.OwnerRef) error {
	children, err := OwnedBy(ctx, s.deps.Client, owner)
	if err != nil {
		return err
	}
	var errs []error
	for _, child := range children {
		if err := s.cascade(ctx, ref.OwnerRef(child)); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := s.DeleteOne(ctx, child); err != nil {
			errs = append(errs, fmt.Errorf("delete %s: %w", child, err))
		}
	}
	return errors.Join(errs...)
}

// ownedBy lists the workflow ids of the entities currently owned by
// owner. A workflow id IS the resource ref ("kind/id") by construction.
func OwnedBy(ctx context.Context, c client.Client, owner ref.OwnerRef) ([]string, error) {
	query := fmt.Sprintf("EntityOwner = '%s' AND ExecutionStatus = 'Running'", string(owner))
	var out []string
	var token []byte
	for {
		resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query:         query,
			NextPageToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list owned by %s: %w", owner, err)
		}
		for _, e := range resp.GetExecutions() {
			out = append(out, e.GetExecution().GetWorkflowId())
		}
		token = resp.GetNextPageToken()
		if len(token) == 0 {
			return out, nil
		}
	}
}

// listRunning lists the running entity workflows of one kind through
// visibility.
func listRunning(ctx context.Context, c client.Client, kind entity.KindName) ([]entity.ResourceID, error) {
	query := fmt.Sprintf("EntityKind = '%s' AND ExecutionStatus = 'Running'", kind)
	prefix := string(kind) + "/"
	var out []entity.ResourceID
	var token []byte
	for {
		resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query:         query,
			NextPageToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list %s entities: %w", kind, err)
		}
		for _, e := range resp.GetExecutions() {
			wfid := e.GetExecution().GetWorkflowId()
			if rest, ok := cutPrefix(wfid, prefix); ok {
				out = append(out, entity.ResourceID(rest))
			}
		}
		token = resp.GetNextPageToken()
		if len(token) == 0 {
			return out, nil
		}
	}
}

func cutPrefix(s, prefix string) (string, bool) {
	if len(s) > len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):], true
	}
	return "", false
}

// awaitClosed polls until the workflow execution is no longer running.
// The finalize semantics live in the entity itself; a closed workflow
// means the lifecycle — including teardown — has run its course.
func awaitClosed(ctx context.Context, c client.Client, workflowID string) error {
	for {
		d, err := c.DescribeWorkflowExecution(ctx, workflowID, "")
		if err != nil {
			return nil // not found: already gone
		}
		if d.GetWorkflowExecutionInfo().GetStatus() != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return nil
		}
		if activity.IsActivity(ctx) {
			activity.RecordHeartbeat(ctx, "awaiting teardown of "+workflowID)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// ensureContainer brings the (machine × run) worker container up on the
// machine's agent. Idempotent — the agent treats a running container as
// a no-op.
func (s *Worker) ensureContainer(ctx context.Context, req wire.EnsureContainerRequest) error {
	if req.Image == "" {
		return errors.New("ensure container: the run has no worker image (" + wire.EnvImage + " was empty)")
	}
	// EnsureContainer BLOCKS while the agent pulls the run image and starts
	// the executor with runc — on a fresh machine, over an insecure
	// registry across NAT, that is routinely longer than the heartbeat
	// timeout. Left un-heartbeated it looks dead: the activity times out
	// mid-pull and the whole run tears down over a slow registry, never
	// over a real failure. Heartbeat while the agent makes progress; the
	// agent's own error still surfaces when it finishes.
	done := make(chan error, 1)
	go func() {
		done <- s.deps.Registry.EnsureContainer(ctx, s.deps.Namespace,
			s.containerSpec(req.AgentId, req.RunId, req.Image))
	}()
	if err := heartbeatUntil(ctx, done, "pulling the run image and starting the executor on the machine"); err != nil {
		return err
	}
	// "Ensured" means the worker inside the container POLLS, not that a
	// process exists: the activity dispatched right after this must land.
	queue := wire.AgentRunQueue(req.AgentId, req.RunId)
	for {
		tq, err := s.deps.Client.DescribeTaskQueue(ctx, queue, enums.TASK_QUEUE_TYPE_ACTIVITY)
		if err == nil && len(tq.GetPollers()) > 0 {
			return nil
		}
		activity.RecordHeartbeat(ctx, "waiting for the container worker to poll")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// heartbeatUntil waits for a blocking operation while keeping the
// activity's heartbeat alive: a cross-machine call that can outlast the
// heartbeat timeout (an image pull) must not read as a dead worker.
func heartbeatUntil(ctx context.Context, done <-chan error, note string) error {
	for {
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			activity.RecordHeartbeat(ctx, note)
		}
	}
}

// containerSpec renders the (machine × run) executor container of this
// namespace — ONE shape whether the run's own path ensures it or the
// resurrection tick brings it back.
func (s *Worker) containerSpec(agentId id.AgentId, runId id.RunId, image string) *agentpb.ContainerSpec {
	return &agentpb.ContainerSpec{
		AgentId: string(agentId),
		RunId:   string(runId),
		Image:   image,
		Env: map[string]string{
			wire.EnvRole:      "machine",
			wire.EnvAddress:   s.deps.External,
			wire.EnvNamespace: s.deps.Namespace,
			wire.EnvRunId:     string(runId),
			wire.EnvAgentId:   string(agentId),
			wire.EnvImage:     image,
			wire.EnvToken:     s.deps.RunToken,
			// TODO(tls): drop once the door serves TLS.
			wire.EnvInsecure: "1",
		},
	}
}

// runCleanup is the guaranteed teardown of a finished run: delete every
// resource the run owns, then stop its containers on every machine —
// EXCEPT where entities still live on the executor's queue (a
// stand-held docker resource keeps its machine executor alive; the
// reaper collects it when the last record dies).
func (s *Worker) runCleanup(ctx context.Context, req wire.RunCleanupRequest) error {
	if err := s.deleteResource(ctx, ref.RunOwner(req.RunId)); err != nil {
		return err
	}
	if err := s.deps.Registry.StopRunContainers(ctx, s.deps.Namespace, req.RunId, func(agentId id.AgentId) bool {
		return s.queueHasLiveEntities(ctx, wire.AgentRunQueue(agentId, req.RunId))
	}); err != nil {
		return err
	}
	// The CROSS-PIPELINE edge: a finished run is what an Upstream
	// trigger waits for. The pipeline this run executed is the
	// activity's workflow type; firing must not fail the cleanup —
	// a downstream that cannot start is its own record's problem.
	upstream := activity.GetInfo(ctx).WorkflowType.Name
	s.fireDownstream(ctx, upstream, string(req.RunId), req.Outcome)
	return nil
}

// fireDownstream lands a firing on every pipeline whose Upstream
// trigger names this one with a matching outcome.
func (s *Worker) fireDownstream(ctx context.Context, upstream, runId, outcome string) {
	ids, err := s.listKind(ctx, string(triggerflow.Kind))
	if err != nil {
		s.deps.Log.Warn("downstream scan failed", xlog.Err(err))
		return
	}
	triggers := entclient.Bind(s.triggerDef, s.deps.Client, wire.ServerQueue)
	for _, tid := range ids {
		desc, err := triggers.Describe(ctx, entity.ResourceID(tid))
		if err != nil || desc.Spec.Kind != "pipeline" {
			continue
		}
		wantPipeline, wantOutcome, _ := strings.Cut(desc.Spec.Spec, ":")
		if wantPipeline != upstream {
			continue
		}
		if wantOutcome != "any" && wantOutcome != outcome {
			continue
		}
		event, _ := json.Marshal(map[string]string{
			"upstreamPipeline": upstream, "upstreamRun": runId, "outcome": outcome,
		})
		if err := s.triggerFire(ctx, triggerflow.FireRequest{
			PipelineId: desc.Spec.PipelineId,
			Trigger:    "pipeline:" + desc.Spec.Name,
			Params:     desc.Spec.Params,
			Event:      event,
		}); err != nil {
			s.deps.Log.Warn("downstream firing failed",
				xlog.String("upstream", upstream), xlog.String("downstream", desc.Spec.PipelineId), xlog.Err(err))
		} else {
			s.deps.Log.Info("downstream fired",
				xlog.String("upstream", upstream), xlog.String("downstream", desc.Spec.PipelineId),
				xlog.String("outcome", outcome))
		}
	}
}

// ReapExecutors keeps the "executor lives while its records do"
// invariant in BOTH directions. The reap half collects machine
// executors whose run is over and whose queue carries no live entities;
// the resurrection half brings a worker back for any queue that still
// HAS live entities but nobody serving them — a container removed by
// hand, a pruned daemon, a crashed host that came back. Without it
// those records are orphans forever: reconcile dead, drift unhealed, a
// stand's TTL delete hanging while the cloud resource burns money.
func (s *Worker) ReapExecutors(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for agentId, runs := range s.deps.Registry.RunContainers(s.deps.Namespace) {
			for _, runId := range runs {
				if s.runOpen(ctx, runId) {
					continue
				}
				if s.queueHasLiveEntities(ctx, wire.AgentRunQueue(agentId, runId)) {
					continue
				}
				s.deps.Log.Info("reaping executor",
					xlog.Any("agent", agentId), xlog.Any("run", runId))
				if err := s.deps.Registry.StopContainer(ctx, s.deps.Namespace, agentId, runId); err != nil {
					s.deps.Log.Error("executor reap", xlog.Err(err))
				}
			}
		}
		s.resurrectExecutors(ctx)
	}
}

// resurrectExecutors is the tick's second pass: every queue with live
// entity records gets its worker ensured. Ensure paths are idempotent
// and cheap when the worker is already alive, so this simply runs for
// every live queue.
func (s *Worker) resurrectExecutors(ctx context.Context) {
	queues, err := s.liveEntityQueues(ctx)
	if err != nil {
		s.deps.Log.Warn("resurrection scan failed", xlog.Err(err))
		return
	}
	served := s.deps.Registry.RunContainers(s.deps.Namespace)
	for queue := range queues {
		switch {
		case strings.HasPrefix(queue, "run/"):
			if s.ensureHost == nil {
				continue
			}
			runId := id.RunId(strings.TrimPrefix(queue, "run/"))
			image := s.imageForRunQueue(ctx, queue, runId)
			if image == "" {
				s.deps.Log.Warn("orphaned records with no recoverable image",
					xlog.String("queue", queue))
				continue
			}
			revived, err := s.ensureHost(ctx, runId, image)
			if err != nil {
				s.deps.Log.Warn("run worker resurrection failed",
					xlog.Any("run", runId), xlog.Err(err))
				continue
			}
			if revived {
				rctx := obs.WithEntity(ctx, "run/"+string(runId))
				obs.Warn(rctx, "run worker resurrected", obs.Str("queue", queue))
				obs.Count(rctx, MetricResurrected, 1, obs.Str("contour", "run"))
			}
		case strings.HasPrefix(queue, "agent/"):
			// agent/<agentId>/run/<runId>
			rest := strings.TrimPrefix(queue, "agent/")
			agentRaw, runRaw, ok := strings.Cut(rest, "/run/")
			if !ok {
				continue
			}
			agentId, runId := id.AgentId(agentRaw), id.RunId(runRaw)
			alive := false
			for _, r := range served[agentId] {
				if r == runId {
					alive = true
					break
				}
			}
			if alive {
				continue
			}
			image := s.imageForRunQueue(ctx, queue, runId)
			if image == "" {
				s.deps.Log.Warn("orphaned records with no recoverable image",
					xlog.String("queue", queue))
				continue
			}
			// The same ensure the run's own path uses; an offline agent
			// is an error here and a retry next tick — a PERMANENTLY
			// dead machine is not resurrection's case but a burial.
			if err := s.deps.Registry.EnsureContainer(ctx, s.deps.Namespace,
				s.containerSpec(agentId, runId, image)); err != nil {
				s.deps.Log.Warn("executor resurrection failed",
					xlog.Any("agent", agentId), xlog.Any("run", runId), xlog.Err(err))
				continue
			}
			rctx := obs.WithEntity(ctx, "run/"+string(runId))
			obs.Warn(rctx, "executor resurrected",
				obs.Str("agent", string(agentId)), obs.Str("queue", queue))
			obs.Count(rctx, MetricResurrected, 1, obs.Str("contour", "machine"))
		}
	}
}

// MetricResurrected counts workers brought back for orphaned records.
const MetricResurrected = "graphene.executor.resurrected"

// liveEntityQueues lists the task queues that still serve live entity
// records — visibility is the source, so the scan survives any loss of
// process memory. The server's own queue is not a candidate.
func (s *Worker) liveEntityQueues(ctx context.Context) (map[string]struct{}, error) {
	queues := map[string]struct{}{}
	var nextPage []byte
	for {
		resp, err := s.deps.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query:         "ExecutionStatus = 'Running' AND EntityKind IS NOT NULL",
			NextPageToken: nextPage,
		})
		if err != nil {
			return nil, err
		}
		for _, info := range resp.GetExecutions() {
			if q := info.GetTaskQueue(); q != "" && q != wire.ServerQueue {
				queues[q] = struct{}{}
			}
		}
		nextPage = resp.GetNextPageToken()
		if len(nextPage) == 0 {
			return queues, nil
		}
	}
}

// imageForRunQueue recovers the worker image of a run's queue: first
// from the run workflow's own attributes (the graphene.io/image mark),
// then — for queues with no run workflow (a brought-kind host) or a
// run past retention — from the pipeline mark on the queue's records,
// resolving the pipeline's ACTIVE image. The fallback may run a newer
// code version than the records were born under; that is the accepted
// price against leaving them orphaned forever (Р-Н23).
func (s *Worker) imageForRunQueue(ctx context.Context, queue string, runId id.RunId) string {
	if desc, err := s.deps.Client.DescribeWorkflowExecution(ctx, "run/"+string(runId), ""); err == nil {
		if img := labelFromAttributes(desc.GetWorkflowExecutionInfo().GetSearchAttributes().GetIndexedFields(), syslabels.Image); img != "" {
			return img
		}
	}
	resp, err := s.deps.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Query:    fmt.Sprintf("TaskQueue = '%s' AND ExecutionStatus = 'Running' AND EntityKind IS NOT NULL", queue),
		PageSize: 1,
	})
	if err != nil || len(resp.GetExecutions()) == 0 {
		return ""
	}
	pipelineId := labelFromAttributes(resp.GetExecutions()[0].GetSearchAttributes().GetIndexedFields(), syslabels.Pipeline)
	if pipelineId == "" {
		return ""
	}
	st, err := s.GetPipeline(ctx, pipelineId)
	if err != nil {
		return ""
	}
	return st.Image
}

// labelFromAttributes digs one "k=v" label out of the EntityLabels
// search attribute.
func labelFromAttributes(fields map[string]*commonpb.Payload, key string) string {
	payload, ok := fields[entdefine.SearchAttrLabels.GetName()]
	if !ok {
		return ""
	}
	var pairs []string
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &pairs); err != nil {
		return ""
	}
	for _, p := range pairs {
		if v, found := strings.CutPrefix(p, key+"="); found {
			return v
		}
	}
	return ""
}

// runOpen reports whether the run workflow is still running.
func (s *Worker) runOpen(ctx context.Context, runId id.RunId) bool {
	desc, err := s.deps.Client.DescribeWorkflowExecution(ctx, "run/"+string(runId), "")
	if err != nil {
		return true // unknown — do not reap on doubt
	}
	return desc.GetWorkflowExecutionInfo().GetStatus() == enums.WORKFLOW_EXECUTION_STATUS_RUNNING
}

// queueHasLiveEntities reports whether any open entity workflow still
// serves its lifecycle from this task queue.
func (s *Worker) queueHasLiveEntities(ctx context.Context, queue string) bool {
	resp, err := s.deps.Client.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Query: fmt.Sprintf("TaskQueue = '%s' AND ExecutionStatus = 'Running' AND EntityKind IS NOT NULL", queue),
	})
	if err != nil {
		// When visibility cannot answer, keep the executor — losing an
		// executor under a live record is worse than a late reap.
		return true
	}
	return resp.GetCount() > 0
}

// agentUserData renders the install script for a machine.
func (s *Worker) agentUserData(_ context.Context, agentId id.AgentId) (string, error) {
	return s.deps.AgentOps.UserData(agentId)
}

// DescribeTrigger reads a trigger declaration; the door checks the
// webhook's secret against it before delivering.
func (s *Worker) DescribeTrigger(ctx context.Context, pipelineId, name string) (triggerflow.Spec, error) {
	triggers := entclient.Bind(s.triggerDef, s.deps.Client, wire.ServerQueue)
	desc, err := triggers.Describe(ctx, triggerflow.Id(pipelineId, name))
	if err != nil {
		return triggerflow.Spec{}, err
	}
	return desc.Spec, nil
}

// DeliverHook lands one verified webhook delivery on the trigger
// record — the firing and its outcome belong to THAT history.
func (s *Worker) DeliverHook(ctx context.Context, pipelineId, name string, event json.RawMessage) error {
	triggers := entclient.Bind(s.triggerDef, s.deps.Client, wire.ServerQueue)
	_, err := entclient.Exec(ctx, triggers, triggerflow.Id(pipelineId, name), triggerflow.HookCmd{Event: event})
	return err
}

// specEqual compares trigger declarations (RawMessage forbids ==).
func specEqual(a, b triggerflow.Spec) bool {
	return a.PipelineId == b.PipelineId && a.Kind == b.Kind && a.Name == b.Name &&
		a.Spec == b.Spec && a.SecretName == b.SecretName && bytes.Equal(a.Params, b.Params)
}
