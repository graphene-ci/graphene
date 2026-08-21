package services

import (
	"connectrpc.com/connect"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gopherex/xlog"
	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
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

// Management serves the management plane — connect-native: the
// generated connect handlers speak the connect, gRPC, and gRPC-Web
// protocols themselves, so this one implementation is the whole
// surface (mounted on the door's HTTP half by MountConnect).
type Management struct {
	Bundles *nsbundle.Manager
	// Base is a namespace-agnostic client for cluster admin calls.
	Base    client.Client
	Secrets *secrets.Namespaced
	Log     *xlog.Logger
}

// --- RunsAPI ---

// StartRun starts the run workflow; with an image the run is MANAGED —
// the server launches the worker container itself.
func (m *Management) StartRun(ctx context.Context, creq *connect.Request[managementv1.StartRunRequest]) (*connect.Response[managementv1.StartRunResponse], error) {
	req := creq.Msg
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	workflowId, temporalRunId, err := startRunCore(ctx, b, m.Log,
		req.GetRunId(), req.GetPipeline(), req.GetParams(), req.GetImage(), req.GetLabels())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&managementv1.StartRunResponse{WorkflowId: workflowId, TemporalRunId: temporalRunId}), nil
}

// startRunCore is the start logic shared by both doors: the management
// plane and the pipeline binary's own worker-plane RunsAPI.
func startRunCore(ctx context.Context, b *nsbundle.Bundle, log *xlog.Logger,
	runIdRaw, pipelineName string, params []byte, image string, labels map[string]string,
) (string, string, error) {
	runId, err := id.ParseRunId(runIdRaw)
	if err != nil {
		return "", "", status.Error(codes.InvalidArgument, err.Error())
	}
	if pipelineName == "" {
		return "", "", status.Error(codes.InvalidArgument, "pipeline is required")
	}
	if err := wire.ValidateUserLabels(labels); err != nil {
		return "", "", status.Error(codes.InvalidArgument, err.Error())
	}
	// Validate params against the published manifest — a bad submit
	// fails at the door. No manifest (never pushed) — no gate. The
	// validated form comes back duration-normalized for the workflow.
	if st, err := b.Worker.GetPipeline(ctx, pipelineName); err == nil && len(st.Manifest) > 0 {
		normalized, err := validateParams(st.Manifest, params)
		if err != nil {
			return "", "", status.Error(codes.InvalidArgument, err.Error())
		}
		params = normalized
	}
	opts := client.StartWorkflowOptions{
		ID:        "run/" + string(runId),
		TaskQueue: wire.RunQueue(runId),
		// A run id names ONE run: starting it twice attaches, never forks.
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}
	if len(labels) > 0 {
		// The run carries its labels in the same EntityLabels attribute
		// resources use — one label language across the system.
		opts.TypedSearchAttributes = temporal.NewSearchAttributes(
			entdefine.SearchAttrLabels.ValueSet(labelPairs(labels)),
		)
	}
	var args []any
	if len(params) > 0 {
		args = append(args, json.RawMessage(params))
	}
	run, err := b.Client.ExecuteWorkflow(ctx, opts, pipelineName, args...)
	if err != nil {
		return "", "", status.Error(codes.Internal, err.Error())
	}
	if image != "" {
		if err := b.Runner.Start(ctx, runId, image); err != nil {
			return "", "", status.Error(codes.Internal, err.Error())
		}
	}
	log.Info("run started",
		xlog.String("namespace", b.Namespace),
		xlog.Any("run", runId),
		xlog.String("pipeline", pipelineName),
		xlog.Bool("managed", image != ""))
	return run.GetID(), run.GetRunID(), nil
}

// GetRun reports the run's status.
func (m *Management) GetRun(ctx context.Context, creq *connect.Request[managementv1.GetRunRequest]) (*connect.Response[managementv1.GetRunResponse], error) {
	req := creq.Msg
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	desc, err := b.Client.DescribeWorkflowExecution(ctx, "run/"+req.GetRunId(), "")
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return connect.NewResponse(&managementv1.GetRunResponse{
		Status: desc.GetWorkflowExecutionInfo().GetStatus().String(),
	}), nil
}

// WatchRun streams status transitions until a terminal one.
func (m *Management) WatchRun(ctx context.Context, creq *connect.Request[managementv1.WatchRunRequest], stream *connect.ServerStream[managementv1.WatchRunEvent]) error {
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return err
	}
	return watchRunCore(ctx, b, creq.Msg.GetRunId(), func(s string) error {
		return stream.Send(&managementv1.WatchRunEvent{Status: s})
	})
}

// watchRunCore polls the run's status and pushes every transition into
// send, ending on a terminal status. Shared by both doors.
func watchRunCore(ctx context.Context, b *nsbundle.Bundle, runId string, send func(status string) error) error {
	last := ""
	for {
		desc, err := b.Client.DescribeWorkflowExecution(ctx, "run/"+runId, "")
		if err != nil {
			return status.Error(codes.NotFound, err.Error())
		}
		s := desc.GetWorkflowExecutionInfo().GetStatus()
		if name := s.String(); name != last {
			if err := send(name); err != nil {
				return err
			}
			last = name
		}
		if s != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
}

// RunResult waits for the run and returns its typed result as JSON.
func (m *Management) RunResult(ctx context.Context, creq *connect.Request[managementv1.RunResultRequest]) (*connect.Response[managementv1.RunResultResponse], error) {
	req := creq.Msg
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	var out json.RawMessage
	if err := b.Client.GetWorkflow(ctx, "run/"+req.GetRunId(), "").Get(ctx, &out); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return connect.NewResponse(&managementv1.RunResultResponse{Result: out}), nil
}

// CancelRun asks the run to stop; the guaranteed-teardown path runs.
func (m *Management) CancelRun(ctx context.Context, creq *connect.Request[managementv1.CancelRunRequest]) (*connect.Response[managementv1.CancelRunResponse], error) {
	req := creq.Msg
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	if err := b.Client.CancelWorkflow(ctx, "run/"+req.GetRunId(), ""); err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return connect.NewResponse(&managementv1.CancelRunResponse{}), nil
}

// ListRuns lists the namespace's runs.
func (m *Management) ListRuns(ctx context.Context, creq *connect.Request[managementv1.ListRunsRequest]) (*connect.Response[managementv1.ListRunsResponse], error) {
	req := creq.Msg
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	if err := noQuotes(req.GetStatus()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	query := `WorkflowId STARTS_WITH "run/"`
	if req.GetStatus() != "" {
		query += fmt.Sprintf(" AND ExecutionStatus = '%s'", req.GetStatus())
	}
	labelTerms, err := labelQueryTerms(req.GetLabels())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	query += labelTerms
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
				Labels:   labelsFromSearchAttrs(e.GetSearchAttributes()),
			})
		}
		token = page.GetNextPageToken()
		if len(token) == 0 {
			return connect.NewResponse(resp), nil
		}
	}
}

// --- ResourcesAPI ---

// List returns the resources matching the selector.
func (m *Management) List(ctx context.Context, creq *connect.Request[managementv1.ListRequest]) (*connect.Response[managementv1.ListResponse], error) {
	req := creq.Msg
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	sel := req.GetSelector()
	if err := noQuotes(sel.GetKind(), sel.GetPhase(), sel.GetOwner()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
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
	labelTerms, err := labelQueryTerms(sel.GetLabels())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	query += labelTerms
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
			resp.Resources = append(resp.Resources, res)
		}
		token = page.GetNextPageToken()
		if len(token) == 0 {
			return connect.NewResponse(resp), nil
		}
	}
}

// Get describes one resource.
func (m *Management) Get(ctx context.Context, creq *connect.Request[managementv1.GetRequest]) (*connect.Response[managementv1.GetResponse], error) {
	req := creq.Msg
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	res, err := m.describe(ctx, b, req.GetRef())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return connect.NewResponse(&managementv1.GetResponse{Resource: res}), nil
}

// Tree returns the ownership subtree under an owner.
func (m *Management) Tree(ctx context.Context, creq *connect.Request[managementv1.TreeRequest]) (*connect.Response[managementv1.TreeResponse], error) {
	req := creq.Msg
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	roots, err := m.subtree(ctx, b, ref.OwnerRef(req.GetOwner()))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return connect.NewResponse(&managementv1.TreeResponse{Roots: roots}), nil
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
func (m *Management) Delete(ctx context.Context, creq *connect.Request[managementv1.DeleteRequest]) (*connect.Response[managementv1.DeleteResponse], error) {
	req := creq.Msg
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
	return connect.NewResponse(&managementv1.DeleteResponse{}), nil
}

// Transfer gives the resource to a new owner.
func (m *Management) Transfer(ctx context.Context, creq *connect.Request[managementv1.TransferRequest]) (*connect.Response[managementv1.TransferResponse], error) {
	req := creq.Msg
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
	return connect.NewResponse(&managementv1.TransferResponse{}), nil
}

// Invoke sends an entity command by wire identity.
func (m *Management) Invoke(ctx context.Context, creq *connect.Request[managementv1.InvokeRequest]) (*connect.Response[managementv1.InvokeResponse], error) {
	req := creq.Msg
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	out, err := entclient.ExecRaw(ctx, b.Client, req.GetRef(), req.GetCommand(), req.GetPayload(), req.GetRequestId())
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return connect.NewResponse(&managementv1.InvokeResponse{Result: out}), nil
}

// --- NamespacesAPI ---

// CreateNamespace registers a namespace with the graphene search
// attributes and starts its runtime bundle.
func (m *Management) CreateNamespace(ctx context.Context, creq *connect.Request[managementv1.CreateNamespaceRequest]) (*connect.Response[managementv1.CreateNamespaceResponse], error) {
	req := creq.Msg
	if _, err := scope(ctx, auth.RoleAdmin); err != nil {
		return nil, err
	}
	if err := m.Bundles.CreateNamespace(ctx, m.Base, req.GetName(), req.GetRetentionDays()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return connect.NewResponse(&managementv1.CreateNamespaceResponse{}), nil
}

// Whoami answers who the caller's token is — login's handshake. Any
// authenticated principal may ask; no role gate.
func (m *Management) Whoami(ctx context.Context, _ *connect.Request[managementv1.WhoamiRequest]) (*connect.Response[managementv1.WhoamiResponse], error) {
	p, ok := auth.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "no principal")
	}
	return connect.NewResponse(&managementv1.WhoamiResponse{
		Role:      string(p.Role),
		Namespace: p.Namespace,
	}), nil
}

// ListNamespaces lists the registered namespaces.
func (m *Management) ListNamespaces(ctx context.Context, _ *connect.Request[managementv1.ListNamespacesRequest]) (*connect.Response[managementv1.ListNamespacesResponse], error) {
	if _, err := scope(ctx, auth.RoleAdmin); err != nil {
		return nil, err
	}
	names, err := m.Bundles.ListNamespaces(ctx, m.Base)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return connect.NewResponse(&managementv1.ListNamespacesResponse{Names: names}), nil
}

// --- SecretsAPI (management) ---

// SetSecret stores a value; it never comes back through this plane.
func (m *Management) SetSecret(ctx context.Context, creq *connect.Request[managementv1.SetSecretRequest]) (*connect.Response[managementv1.SetSecretResponse], error) {
	req := creq.Msg
	namespace, err := scope(ctx, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	m.Secrets.Set(namespace, req.GetName(), req.GetValue())
	return connect.NewResponse(&managementv1.SetSecretResponse{}), nil
}

// DeleteSecret forgets a name.
func (m *Management) DeleteSecret(ctx context.Context, creq *connect.Request[managementv1.DeleteSecretRequest]) (*connect.Response[managementv1.DeleteSecretResponse], error) {
	req := creq.Msg
	namespace, err := scope(ctx, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	m.Secrets.Delete(namespace, req.GetName())
	return connect.NewResponse(&managementv1.DeleteSecretResponse{}), nil
}

// ListSecrets lists names — never values.
func (m *Management) ListSecrets(ctx context.Context, _ *connect.Request[managementv1.ListSecretsRequest]) (*connect.Response[managementv1.ListSecretsResponse], error) {
	namespace, err := scope(ctx, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&managementv1.ListSecretsResponse{Names: m.Secrets.List(namespace)}), nil
}

// --- helpers ---

// describeOut mirrors temporal-entity's DescribeOut with raw halves.
type describeOut struct {
	Phase             string            `json:"phase"`
	Spec              json.RawMessage   `json:"spec"`
	State             json.RawMessage   `json:"state"`
	Labels            map[string]string `json:"labels"`
	PendingCommands   int32             `json:"pendingCommands"`
	MarkedForDeletion bool              `json:"markedForDeletion"`
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
		Labels:            out.Labels,
		PendingCommands:   out.PendingCommands,
		MarkedForDeletion: out.MarkedForDeletion,
	}, nil
}

// labelQueryTerms renders a label selector as visibility terms over the
// EntityLabels keyword list: one AND per pair. Values with quotes are
// rejected — they cannot be escaped in a visibility query.
func labelQueryTerms(labels map[string]string) (string, error) {
	var out strings.Builder
	for k, v := range labels {
		pair := k + "=" + v
		if err := noQuotes(pair); err != nil {
			return "", err
		}
		fmt.Fprintf(&out, " AND EntityLabels IN ('%s')", pair)
	}
	return out.String(), nil
}

// noQuotes guards every value interpolated into a visibility query:
// quotes cannot be escaped there, so they are refused outright.
func noQuotes(values ...string) error {
	for _, v := range values {
		if strings.ContainsAny(v, `'"`) {
			return fmt.Errorf("%q: quotes are not allowed in a selector", v)
		}
	}
	return nil
}

// labelPairs renders labels as sorted "k=v" keywords.
func labelPairs(labels map[string]string) []string {
	out := make([]string, 0, len(labels))
	for k, v := range labels {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// labelsFromSearchAttrs decodes the EntityLabels keyword list back into
// a map.
func labelsFromSearchAttrs(sa *common.SearchAttributes) map[string]string {
	payload := sa.GetIndexedFields()["EntityLabels"]
	if payload == nil {
		return nil
	}
	var pairs []string
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &pairs); err != nil {
		return nil
	}
	out := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		if k, v, ok := strings.Cut(pair, "="); ok {
			out[k] = v
		}
	}
	return out
}

func stripPrefix(s, prefix string) string {
	if len(s) > len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):]
	}
	return s
}
