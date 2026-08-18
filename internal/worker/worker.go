// Package worker assembles the server's Temporal worker: the system
// entity flows from the pipeline repository, their Ops activities, and
// the server-queue activities the pipeline library calls
// (declare/delete/ensure/user-data). This worker talks to Temporal
// directly — it IS the server; everyone else goes through the proxy.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/graphene-ci/agent/pkg/agentpb"
	"github.com/graphene-ci/graphene/internal/agents"
	"github.com/graphene-ci/graphene/internal/ops"
	"github.com/graphene-ci/pipeline/pkg/flow/artifact"
	"github.com/graphene-ci/pipeline/pkg/flow/machine"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Deps is everything the worker needs.
type Deps struct {
	Client       client.Client
	Registry     *agents.Registry
	MachineOps   *ops.MachineOps
	ArtifactOps  *ops.ArtifactOps
	ExternalGRPC string
	// RunToken is handed to machine containers so their worker passes the
	// Temporal proxy. (Per-run minted tokens replace this static one.)
	RunToken string
	Log      *slog.Logger
}

// Worker is the assembled server worker.
type Worker struct {
	w           worker.Worker
	deps        Deps
	machineDef  *entdefine.Definition[pipeline.MachineSpec, machine.State]
	artifactDef *entdefine.Definition[pipeline.ArtifactSpec, artifact.State]
}

// New builds and registers everything; Run starts polling.
func New(deps Deps) (*Worker, error) {
	w := worker.New(deps.Client, wire.ServerQueue, worker.Options{})
	s := &Worker{
		w:           w,
		deps:        deps,
		machineDef:  machine.Definition(machine.Options{}),
		artifactDef: artifact.Definition(),
	}

	if err := errors.Join(s.machineDef.Register(w), s.artifactDef.Register(w)); err != nil {
		return nil, err
	}

	// Ops behind the flows.
	w.RegisterActivityWithOptions(deps.MachineOps.InstallSSH, activity.RegisterOptions{Name: machine.InstallSSHActivity})
	w.RegisterActivityWithOptions(deps.MachineOps.AgentStatus, activity.RegisterOptions{Name: machine.AgentStatusActivity})
	w.RegisterActivityWithOptions(deps.ArtifactOps.Stat, activity.RegisterOptions{Name: artifact.StatActivity})
	w.RegisterActivityWithOptions(deps.ArtifactOps.Delete, activity.RegisterOptions{Name: artifact.DeleteActivity})

	// The server-queue contract the pipeline library calls.
	w.RegisterActivityWithOptions(s.declareMachine, activity.RegisterOptions{Name: wire.DeclareMachineActivity})
	w.RegisterActivityWithOptions(s.declareArtifact, activity.RegisterOptions{Name: wire.DeclareArtifactActivity})
	w.RegisterActivityWithOptions(s.deleteResource, activity.RegisterOptions{Name: wire.DeleteResourceActivity})
	w.RegisterActivityWithOptions(s.ensureContainer, activity.RegisterOptions{Name: wire.EnsureContainerActivity})
	w.RegisterActivityWithOptions(s.runCleanup, activity.RegisterOptions{Name: wire.RunCleanupActivity})
	w.RegisterActivityWithOptions(s.agentUserData, activity.RegisterOptions{Name: wire.AgentUserDataActivity})
	return s, nil
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

// declareMachine creates (or attaches to) the machine entity and blocks
// until it is ready, heartbeating while it converges.
func (s *Worker) declareMachine(ctx context.Context, machineId id.MachineId, spec pipeline.MachineSpec) (pipeline.MachineState, error) {
	machines := entclient.Bind(s.machineDef, s.deps.Client, wire.ServerQueue)
	rid := entity.ResourceID(machineId)
	if _, err := machines.CreateOrAttach(ctx, rid, spec); err != nil {
		return pipeline.MachineState{}, err
	}
	for {
		out, err := machines.Describe(ctx, rid)
		if err != nil {
			return pipeline.MachineState{}, err
		}
		switch out.Phase {
		case entity.PhaseReady:
			return out.State.MachineState, nil
		case entity.PhaseCreateFailed:
			return pipeline.MachineState{}, fmt.Errorf("machine %s failed to create", machineId)
		case entity.PhaseDeleting, entity.PhaseDeleted, entity.PhaseDeleteFailed:
			return pipeline.MachineState{}, fmt.Errorf("machine %s is going away (phase %s)", machineId, out.Phase)
		case entity.PhaseCreating:
			// still converging — keep polling
		}
		activity.RecordHeartbeat(ctx, string(out.Phase))
		select {
		case <-ctx.Done():
			return pipeline.MachineState{}, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// declareArtifact creates the artifact entity and waits for verification.
func (s *Worker) declareArtifact(ctx context.Context, artifactId id.ArtifactId, spec pipeline.ArtifactSpec) (pipeline.ArtifactState, error) {
	artifacts := entclient.Bind(s.artifactDef, s.deps.Client, wire.ServerQueue)
	rid := entity.ResourceID(artifactId)
	if _, err := artifacts.CreateOrAttach(ctx, rid, spec); err != nil {
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

// deleteResource tears down every entity owned by the given owner and
// waits until each is gone. The owned set is found by scanning the
// running entities of the known kinds and matching spec.Owner.
// TODO(chassis): an EntityOwner search attribute in temporal-entity
// turns the scan into one visibility query.
func (s *Worker) deleteResource(ctx context.Context, owner ref.OwnerRef) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	machines := entclient.Bind(s.machineDef, s.deps.Client, wire.ServerQueue)
	artifacts := entclient.Bind(s.artifactDef, s.deps.Client, wire.ServerQueue)

	var errs []error
	errs = append(errs, deleteOwned(ctx, s.deps.Client, machine.Kind, machines,
		func(spec pipeline.MachineSpec) ref.OwnerRef { return spec.Owner }, owner))
	errs = append(errs, deleteOwned(ctx, s.deps.Client, artifact.Kind, artifacts,
		func(spec pipeline.ArtifactSpec) ref.OwnerRef { return spec.Owner }, owner))
	return errors.Join(errs...)
}

// deleteOwned finds the running entities of one kind owned by owner,
// signals Delete, and waits for each to finish.
func deleteOwned[Spec, State any](
	ctx context.Context,
	c client.Client,
	kind entity.KindName,
	cl *entclient.Client[Spec, State],
	ownerOf func(Spec) ref.OwnerRef,
	owner ref.OwnerRef,
) error {
	ids, err := listRunning(ctx, c, kind)
	if err != nil {
		return err
	}
	var owned []entity.ResourceID
	for _, rid := range ids {
		out, err := cl.Describe(ctx, rid)
		if err != nil {
			continue // gone between list and describe
		}
		if ownerOf(out.Spec) != owner {
			continue
		}
		if err := cl.Delete(ctx, rid); err != nil {
			return err
		}
		owned = append(owned, rid)
	}
	for _, rid := range owned {
		if err := awaitClosed(ctx, c, cl.WorkflowID(rid)); err != nil {
			return err
		}
	}
	return nil
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
		activity.RecordHeartbeat(ctx, "awaiting teardown of "+workflowID)
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
		wire.EnvAddress:   s.deps.ExternalGRPC,
		wire.EnvRunId:     string(req.RunId),
		wire.EnvMachineId: string(req.MachineId),
		wire.EnvImage:     req.Image,
		wire.EnvToken:     s.deps.RunToken,
		// TODO(tls): drop once the door serves TLS.
		wire.EnvInsecure: "1",
	}
	return s.deps.Registry.EnsureContainer(ctx, &agentpb.ContainerSpec{
		MachineId: string(req.MachineId),
		RunId:     string(req.RunId),
		Image:     req.Image,
		Env:       env,
	})
}

// runCleanup is the guaranteed teardown of a finished run: delete every
// resource the run owns, then stop its containers on every machine.
func (s *Worker) runCleanup(ctx context.Context, runId id.RunId) error {
	if err := s.deleteResource(ctx, ref.RunOwner(runId)); err != nil {
		return err
	}
	return s.deps.Registry.StopRunContainers(ctx, runId)
}

// agentUserData renders the install script for a machine.
func (s *Worker) agentUserData(_ context.Context, machineId id.MachineId) (string, error) {
	return s.deps.MachineOps.UserData(machineId)
}
