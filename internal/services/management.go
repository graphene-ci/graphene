package services

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gopherex/xlog"
	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/nsbundle"
	"github.com/graphene-ci/graphene/internal/secrets"
	"github.com/graphene-ci/graphene/internal/worker"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Management serves the management plane.
type Management struct {
	managementv1.UnimplementedRunsAPIServer
	managementv1.UnimplementedResourcesAPIServer
	managementv1.UnimplementedNamespacesAPIServer
	managementv1.UnimplementedSecretsAPIServer

	Bundles *nsbundle.Manager
	// Base is a namespace-agnostic client for cluster admin calls.
	Base    client.Client
	Secrets *secrets.Namespaced
	Log     *xlog.Logger
}

// --- RunsAPI ---

// StartRun starts the run workflow; with an image the run is MANAGED —
// the server launches the worker container itself.
func (m *Management) StartRun(ctx context.Context, req *managementv1.StartRunRequest) (*managementv1.StartRunResponse, error) {
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	runId, err := id.ParseRunId(req.GetRunId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if req.GetPipeline() == "" {
		return nil, status.Error(codes.InvalidArgument, "pipeline is required")
	}
	opts := client.StartWorkflowOptions{
		ID:        "run/" + string(runId),
		TaskQueue: wire.RunQueue(runId),
		// A run id names ONE run: starting it twice attaches, never forks.
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}
	var args []any
	if len(req.GetParams()) > 0 {
		args = append(args, json.RawMessage(req.GetParams()))
	}
	run, err := b.Client.ExecuteWorkflow(ctx, opts, req.GetPipeline(), args...)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if req.GetImage() != "" {
		if err := b.Runner.Start(ctx, runId, req.GetImage()); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	m.Log.Info("run started",
		xlog.String("namespace", b.Namespace),
		xlog.Any("run", runId),
		xlog.String("pipeline", req.GetPipeline()),
		xlog.Bool("managed", req.GetImage() != ""))
	return &managementv1.StartRunResponse{WorkflowId: run.GetID(), TemporalRunId: run.GetRunID()}, nil
}

// GetRun reports the run's status.
func (m *Management) GetRun(ctx context.Context, req *managementv1.GetRunRequest) (*managementv1.GetRunResponse, error) {
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	desc, err := b.Client.DescribeWorkflowExecution(ctx, "run/"+req.GetRunId(), "")
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &managementv1.GetRunResponse{
		Status: desc.GetWorkflowExecutionInfo().GetStatus().String(),
	}, nil
}

// RunResult waits for the run and returns its typed result as JSON.
func (m *Management) RunResult(ctx context.Context, req *managementv1.RunResultRequest) (*managementv1.RunResultResponse, error) {
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	var out json.RawMessage
	if err := b.Client.GetWorkflow(ctx, "run/"+req.GetRunId(), "").Get(ctx, &out); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &managementv1.RunResultResponse{Result: out}, nil
}

// CancelRun asks the run to stop; the guaranteed-teardown path runs.
func (m *Management) CancelRun(ctx context.Context, req *managementv1.CancelRunRequest) (*managementv1.CancelRunResponse, error) {
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	if err := b.Client.CancelWorkflow(ctx, "run/"+req.GetRunId(), ""); err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &managementv1.CancelRunResponse{}, nil
}

// ListRuns lists the namespace's runs.
func (m *Management) ListRuns(ctx context.Context, req *managementv1.ListRunsRequest) (*managementv1.ListRunsResponse, error) {
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	query := `WorkflowId STARTS_WITH "run/"`
	if req.GetStatus() != "" {
		query += fmt.Sprintf(" AND ExecutionStatus = '%s'", req.GetStatus())
	}
	resp := &managementv1.ListRunsResponse{}
	var token []byte
	for {
		page, err := b.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query:         query,
			NextPageToken: token,
		})
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		for _, e := range page.GetExecutions() {
			resp.Runs = append(resp.Runs, &managementv1.RunInfo{
				RunId:    stripPrefix(e.GetExecution().GetWorkflowId(), "run/"),
				Pipeline: e.GetType().GetName(),
				Status:   e.GetStatus().String(),
			})
		}
		token = page.GetNextPageToken()
		if len(token) == 0 {
			return resp, nil
		}
	}
}

// --- ResourcesAPI ---

// List returns the resources matching the selector.
func (m *Management) List(ctx context.Context, req *managementv1.ListRequest) (*managementv1.ListResponse, error) {
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	sel := req.GetSelector()
	query := `EntityKind IS NOT NULL`
	if sel.GetKind() != "" {
		query = fmt.Sprintf("EntityKind = '%s'", sel.GetKind())
	}
	if sel.GetPhase() != "" {
		query += fmt.Sprintf(" AND EntityPhase = '%s'", sel.GetPhase())
	}
	if sel.GetOwner() != "" {
		query += fmt.Sprintf(" AND EntityOwner = '%s'", sel.GetOwner())
	}
	query += ` AND ExecutionStatus = 'Running'`
	resp := &managementv1.ListResponse{}
	var token []byte
	for {
		page, err := b.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query:         query,
			NextPageToken: token,
		})
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		for _, e := range page.GetExecutions() {
			res, err := m.describe(ctx, b, e.GetExecution().GetWorkflowId())
			if err != nil {
				continue // gone between list and describe
			}
			if !labelsMatch(sel.GetLabels(), res) {
				continue
			}
			resp.Resources = append(resp.Resources, res)
		}
		token = page.GetNextPageToken()
		if len(token) == 0 {
			return resp, nil
		}
	}
}

// Get describes one resource.
func (m *Management) Get(ctx context.Context, req *managementv1.GetRequest) (*managementv1.GetResponse, error) {
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	res, err := m.describe(ctx, b, req.GetRef())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &managementv1.GetResponse{Resource: res}, nil
}

// Tree returns the ownership subtree under an owner.
func (m *Management) Tree(ctx context.Context, req *managementv1.TreeRequest) (*managementv1.TreeResponse, error) {
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	roots, err := m.subtree(ctx, b, ref.OwnerRef(req.GetOwner()))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &managementv1.TreeResponse{Roots: roots}, nil
}

func (m *Management) subtree(ctx context.Context, b *nsbundle.Bundle, owner ref.OwnerRef) ([]*managementv1.TreeNode, error) {
	children, err := worker.OwnedBy(ctx, b.Client, owner)
	if err != nil {
		return nil, err
	}
	var out []*managementv1.TreeNode
	for _, child := range children {
		node := &managementv1.TreeNode{}
		if res, err := m.describe(ctx, b, child); err == nil {
			node.Resource = res
		} else {
			node.Resource = &managementv1.Resource{Ref: child}
		}
		grand, err := m.subtree(ctx, b, ref.OwnerRef(child))
		if err != nil {
			return nil, err
		}
		node.Children = grand
		out = append(out, node)
	}
	return out, nil
}

// Delete tears the resource down with its subtree, deepest first.
func (m *Management) Delete(ctx context.Context, req *managementv1.DeleteRequest) (*managementv1.DeleteResponse, error) {
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	if err := b.Worker.CascadeDelete(ctx, ref.OwnerRef(req.GetRef())); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := b.Worker.DeleteOne(ctx, req.GetRef()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &managementv1.DeleteResponse{}, nil
}

// Transfer gives the resource to a new owner.
func (m *Management) Transfer(ctx context.Context, req *managementv1.TransferRequest) (*managementv1.TransferResponse, error) {
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	if err := b.Worker.Transfer(ctx, wire.TransferResourceRequest{
		Resource: ref.OwnerRef(req.GetRef()),
		NewOwner: ref.OwnerRef(req.GetNewOwner()),
		Keep:     time.Duration(req.GetKeepSeconds()) * time.Second,
	}); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &managementv1.TransferResponse{}, nil
}

// Invoke sends an entity command by wire identity.
func (m *Management) Invoke(ctx context.Context, req *managementv1.InvokeRequest) (*managementv1.InvokeResponse, error) {
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	out, err := entclient.ExecRaw(ctx, b.Client, req.GetRef(), req.GetCommand(), req.GetPayload(), req.GetRequestId())
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &managementv1.InvokeResponse{Result: out}, nil
}

// --- NamespacesAPI ---

// CreateNamespace registers a namespace with the graphene search
// attributes and starts its runtime bundle.
func (m *Management) CreateNamespace(ctx context.Context, req *managementv1.CreateNamespaceRequest) (*managementv1.CreateNamespaceResponse, error) {
	if _, err := scope(ctx, auth.RoleAdmin); err != nil {
		return nil, err
	}
	if err := m.Bundles.CreateNamespace(ctx, m.Base, req.GetName(), req.GetRetentionDays()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &managementv1.CreateNamespaceResponse{}, nil
}

// ListNamespaces lists the registered namespaces.
func (m *Management) ListNamespaces(ctx context.Context, _ *managementv1.ListNamespacesRequest) (*managementv1.ListNamespacesResponse, error) {
	if _, err := scope(ctx, auth.RoleAdmin); err != nil {
		return nil, err
	}
	names, err := m.Bundles.ListNamespaces(ctx, m.Base)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &managementv1.ListNamespacesResponse{Names: names}, nil
}

// --- SecretsAPI (management) ---

// SetSecret stores a value; it never comes back through this plane.
func (m *Management) SetSecret(ctx context.Context, req *managementv1.SetSecretRequest) (*managementv1.SetSecretResponse, error) {
	namespace, err := scope(ctx, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	m.Secrets.Set(namespace, req.GetName(), req.GetValue())
	return &managementv1.SetSecretResponse{}, nil
}

// DeleteSecret forgets a name.
func (m *Management) DeleteSecret(ctx context.Context, req *managementv1.DeleteSecretRequest) (*managementv1.DeleteSecretResponse, error) {
	namespace, err := scope(ctx, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	m.Secrets.Delete(namespace, req.GetName())
	return &managementv1.DeleteSecretResponse{}, nil
}

// ListSecrets lists names — never values.
func (m *Management) ListSecrets(ctx context.Context, _ *managementv1.ListSecretsRequest) (*managementv1.ListSecretsResponse, error) {
	namespace, err := scope(ctx, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	return &managementv1.ListSecretsResponse{Names: m.Secrets.List(namespace)}, nil
}

// --- helpers ---

// describeOut mirrors temporal-entity's DescribeOut with raw halves.
type describeOut struct {
	Phase             string          `json:"phase"`
	Spec              json.RawMessage `json:"spec"`
	State             json.RawMessage `json:"state"`
	PendingCommands   int32           `json:"pendingCommands"`
	MarkedForDeletion bool            `json:"markedForDeletion"`
}

func (m *Management) describe(ctx context.Context, b *nsbundle.Bundle, workflowId string) (*managementv1.Resource, error) {
	raw, err := entclient.DescribeRaw(ctx, b.Client, workflowId)
	if err != nil {
		return nil, err
	}
	var out describeOut
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	var state struct {
		Owner string `json:"owner"`
	}
	_ = json.Unmarshal(out.State, &state)
	kind, _, _ := strings.Cut(workflowId, "/")
	return &managementv1.Resource{
		Ref:               workflowId,
		Kind:              kind,
		Phase:             out.Phase,
		Owner:             state.Owner,
		Spec:              out.Spec,
		State:             out.State,
		PendingCommands:   out.PendingCommands,
		MarkedForDeletion: out.MarkedForDeletion,
	}, nil
}

// labelsMatch filters by the record's spec labels.
func labelsMatch(want map[string]string, res *managementv1.Resource) bool {
	if len(want) == 0 {
		return true
	}
	var spec struct {
		Labels map[string]string `json:"labels"`
	}
	_ = json.Unmarshal(res.GetSpec(), &spec)
	for k, v := range want {
		if spec.Labels[k] != v {
			return false
		}
	}
	return true
}

func stripPrefix(s, prefix string) string {
	if len(s) > len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):]
	}
	return s
}
