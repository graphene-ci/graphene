// Package worker assembles the server's Temporal worker: the system
// entity flows from the pipeline repository, their Ops activities, and
// the server-queue activities the pipeline library calls
// (declare/delete/ensure/user-data). This worker talks to Temporal
// directly — it IS the server; everyone else goes through the proxy.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gopherex/xlog"
	"strings"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	agentpb "github.com/graphene-ci/agent/pkg/proto/agent/v1"
	"github.com/graphene-ci/graphene/internal/agents"
	"github.com/graphene-ci/graphene/internal/ops"
	"github.com/graphene-ci/graphene/internal/pipelineflow"
	"github.com/graphene-ci/graphene/internal/standflow"
	agentflow "github.com/graphene-ci/pipeline/pkg/flow/agent"
	"github.com/graphene-ci/pipeline/pkg/flow/artifact"
	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
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
	Log      *xlog.Logger
}

// Worker is the assembled server worker.
type Worker struct {
	w           worker.Worker
	deps        Deps
	agentDef    *entdefine.Definition[pipeline.AgentSpec, agentflow.State]
	artifactDef *entdefine.Definition[pipeline.ArtifactSpec, artifact.State]
	standDef    *entdefine.Definition[standflow.Spec, standflow.State]
	pipelineDef *entdefine.Definition[pipelineflow.Spec, pipelineflow.State]
}

// New builds and registers everything; Run starts polling.
func New(deps Deps) (*Worker, error) {
	w := worker.New(deps.Client, wire.ServerQueue, worker.Options{})
	standTick := deps.StandTick
	if standTick == 0 {
		standTick = 30 * time.Second
	}
	s := &Worker{
		w:           w,
		deps:        deps,
		agentDef:    agentflow.Definition(agentflow.Options{}),
		artifactDef: artifact.Definition(),
		standDef:    standflow.New(standTick),
		pipelineDef: pipelineflow.New(),
	}

	if err := errors.Join(
		s.agentDef.Register(w), s.artifactDef.Register(w),
		s.standDef.Register(w), s.pipelineDef.Register(w),
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
	return s, nil
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
	pipelines := entclient.Bind(s.pipelineDef, s.deps.Client, wire.ServerQueue)
	_, err := entclient.ExecWithStart(ctx, pipelines, entity.ResourceID(pipelineId),
		pipelineflow.Spec{}, pipelineflow.PublishCmd{Manifest: raw, Image: image})
	return err
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
func (s *Worker) attachAgent(ctx context.Context, agentId id.AgentId, needs []wire.NeedSpec) (pipeline.AgentState, error) {
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
		wfId := activity.GetInfo(ctx).WorkflowExecution.ID
		out[wire.LabelRun] = strings.TrimPrefix(wfId, "run/")
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

// transferResource is the activity form of Transfer.
func (s *Worker) transferResource(ctx context.Context, req wire.TransferResourceRequest) error {
	return s.Transfer(ctx, req)
}

// Transfer gives a resource to a new owner through the entity's own
// transfer command: typed for the system kinds, by wire identity for
// every kind that registered the command (library kinds included).
func (s *Worker) Transfer(ctx context.Context, req wire.TransferResourceRequest) error {
	kind, resource, ok := strings.Cut(string(req.Resource), "/")
	if !ok {
		return fmt.Errorf("resource ref %q: want kind/id", req.Resource)
	}
	standId, toStand := strings.CutPrefix(string(req.NewOwner), "stand/")
	if req.Keep > 0 && !toStand {
		return fmt.Errorf("KeepFor is a stand-stay bound; owner %q is not a stand", req.NewOwner)
	}
	if err := s.transferCmd(ctx, kind, resource, req); err != nil {
		return err
	}
	if toStand {
		// The stand LIVES the handover: its own timer enforces the
		// keep, its history records it.
		stands := entclient.Bind(s.standDef, s.deps.Client, wire.ServerQueue)
		_, err := entclient.ExecWithStart(ctx, stands, entity.ResourceID(standId),
			standflow.Spec{PipelineId: standId},
			standflow.AcceptCmd{Ref: req.Resource, Keep: req.Keep})
		return err
	}
	return nil
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
		return err
	}
	return awaitClosed(ctx, s.deps.Client, workflowId)
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
	env := map[string]string{
		wire.EnvRole:      "machine",
		wire.EnvAddress:   s.deps.External,
		wire.EnvNamespace: s.deps.Namespace,
		wire.EnvRunId:     string(req.RunId),
		wire.EnvAgentId:   string(req.AgentId),
		wire.EnvImage:     req.Image,
		wire.EnvToken:     s.deps.RunToken,
		// TODO(tls): drop once the door serves TLS.
		wire.EnvInsecure: "1",
	}
	if err := s.deps.Registry.EnsureContainer(ctx, s.deps.Namespace, &agentpb.ContainerSpec{
		AgentId: string(req.AgentId),
		RunId:   string(req.RunId),
		Image:   req.Image,
		Env:     env,
	}); err != nil {
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

// runCleanup is the guaranteed teardown of a finished run: delete every
// resource the run owns, then stop its containers on every machine —
// EXCEPT where entities still live on the executor's queue (a
// stand-held docker resource keeps its machine executor alive; the
// reaper collects it when the last record dies).
func (s *Worker) runCleanup(ctx context.Context, runId id.RunId) error {
	if err := s.deleteResource(ctx, ref.RunOwner(runId)); err != nil {
		return err
	}
	return s.deps.Registry.StopRunContainers(ctx, s.deps.Namespace, runId, func(agentId id.AgentId) bool {
		return s.queueHasLiveEntities(ctx, wire.AgentRunQueue(agentId, runId))
	})
}

// ReapExecutors collects machine executors whose run is over AND whose
// queue carries no live entities any more — the counterpart of "the
// executor lives while its records do": when the stand's cascade kills
// the last docker record, this tick stops the container.
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
	}
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
