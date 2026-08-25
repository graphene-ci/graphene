package services

import (
	"regexp"

	"connectrpc.com/connect"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/gopherex/xlog"
	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/authz"
	"github.com/graphene-ci/graphene/internal/infrastructure/blob"
	syslabels "github.com/graphene-ci/graphene/internal/labels"
	"github.com/graphene-ci/graphene/internal/materialize"
	"github.com/graphene-ci/graphene/internal/nsbundle"
	"github.com/graphene-ci/graphene/internal/runtimes"
	"github.com/graphene-ci/graphene/internal/secrets"
	"github.com/graphene-ci/graphene/internal/selector"
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
	Vars    *secrets.Namespaced
	// Materializer serves the source-first contour; nil disables it.
	Materializer *materialize.Materializer
	// Blobs reads revision manifests.
	Blobs blob.Store
	// Runtimes is the installation's toolchain catalogue.
	Runtimes *runtimes.Catalogue
	// Authz decides what a caller may do; nil falls back to the
	// built-in roles of the caller's own token.
	Authz *authz.Resolver
	// Version is the server build version, for ServerInfo.
	Version string
	Log     *xlog.Logger
}

// --- RunsAPI ---

// StartRun starts the run workflow; with an image the run is MANAGED —
// the server launches the worker container itself.
func (m *Management) StartRun(ctx context.Context, creq *connect.Request[managementv1.StartRunRequest]) (*connect.Response[managementv1.StartRunResponse], error) {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbRun, authz.KindPipeline)
	if err != nil {
		return nil, err
	}
	workflowId, temporalRunId, err := startRunCore(ctx, b, m.Log,
		req.GetRunId(), req.GetPipeline(), req.GetParams(), req.GetImage(), req.GetLabels(),
		syslabels.TriggerManual)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&managementv1.StartRunResponse{WorkflowId: workflowId, TemporalRunId: temporalRunId}), nil
}

// StartRunOnBundle exposes the start path to the server wiring: the
// trigger contour starts runs through the same logic as the doors.
func StartRunOnBundle(ctx context.Context, b *nsbundle.Bundle, log *xlog.Logger,
	runId, pipelineId string, params []byte, image string, labels map[string]string, trigger string,
) error {
	_, _, err := startRunCore(ctx, b, log, runId, pipelineId, params, image, labels, trigger)
	return err
}

// startRunCore is the start logic shared by both doors: the management
// plane and the pipeline binary's own worker-plane RunsAPI.
func startRunCore(ctx context.Context, b *nsbundle.Bundle, log *xlog.Logger,
	runIdRaw, pipelineName string, params []byte, image string, labels map[string]string, trigger string,
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
	// Variables substitute FIRST — "${var:name}" placeholders become
	// the installation's values, so validation sees what the workflow
	// will see and a missing variable fails the submit here.
	params, err = substituteVars(params, b.Vars)
	if err != nil {
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
		// A secret-typed param names a secret; the name must resolve
		// NOW — the run must not discover a missing secret hours later
		// inside an activity.
		if err := checkSecretRefs(st.Manifest, params, b.Secrets); err != nil {
			return "", "", status.Error(codes.InvalidArgument, err.Error())
		}
	}
	opts := client.StartWorkflowOptions{
		ID:        "run/" + string(runId),
		TaskQueue: wire.RunQueue(runId),
		// A run id names ONE run: starting it twice attaches, never forks.
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}
	// The run carries its labels in the same EntityLabels attribute
	// resources use — one label language across the system. The system
	// adds WHAT started the run after user labels are validated.
	withTrigger := make(map[string]string, len(labels)+1)
	for k, v := range labels {
		withTrigger[k] = v
	}
	if trigger != "" {
		withTrigger[syslabels.Trigger] = trigger
	}
	opts.TypedSearchAttributes = temporal.NewSearchAttributes(
		entdefine.SearchAttrLabels.ValueSet(labelPairs(withTrigger)),
	)
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
	b, err := m.allow(ctx, authz.VerbGet, authz.KindRun)
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
	b, err := m.allow(ctx, authz.VerbWatch, authz.KindRun)
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
	b, err := m.allow(ctx, authz.VerbGet, authz.KindRun)
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
	b, err := m.allow(ctx, authz.VerbInvoke, authz.KindRun)
	if err != nil {
		return nil, err
	}
	if err := b.Client.CancelWorkflow(ctx, "run/"+req.GetRunId(), ""); err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return connect.NewResponse(&managementv1.CancelRunResponse{}), nil
}

// listedKind reads the kind a listing is narrowed to — from the
// selector a client fills, or from the raw visibility query. A listing
// of ONE kind is authorized against that kind; a listing of everything
// needs the right to see everything.
func listedKind(selector *managementv1.Selector, query string) authz.Kind {
	if k := selector.GetKind(); k != "" {
		return authz.KindOf(k + "/")
	}
	m := kindInQuery.FindStringSubmatch(query)
	if m == nil {
		return authz.KindAll
	}
	return authz.KindOf(m[1] + "/")
}

var kindInQuery = regexp.MustCompile(`EntityKind\s*=\s*'([^']+)'`)

// listQuery resolves a request's query/selector duality into one
// visibility query. Exactly one of the two may be set.
func listQuery(query string, sel *managementv1.Selector) (string, error) {
	structural := sel.GetKind() != "" || sel.GetPhase() != "" || sel.GetOwner() != "" || len(sel.GetLabels()) > 0
	if query != "" && structural {
		return "", fmt.Errorf("set query or selector, not both")
	}
	if query != "" {
		parsed, err := selector.Parse(query)
		if err != nil {
			return "", err
		}
		return selector.Compile(parsed, time.Now())
	}
	if err := noQuotes(sel.GetKind(), sel.GetPhase(), sel.GetOwner()); err != nil {
		return "", err
	}
	out := `EntityKind IS NOT NULL`
	if sel.GetKind() != "" {
		out = fmt.Sprintf("EntityKind = '%s'", sel.GetKind())
	}
	if sel.GetPhase() != "" {
		out += fmt.Sprintf(" AND EntityPhase = '%s'", sel.GetPhase())
	}
	if sel.GetOwner() != "" {
		out += fmt.Sprintf(" AND EntityOwner = '%s'", sel.GetOwner())
	}
	labelTerms, err := labelQueryTerms(sel.GetLabels())
	if err != nil {
		return "", err
	}
	out += labelTerms
	out += ` AND ExecutionStatus = 'Running'`
	return out, nil
}

// Page tokens on the wire are base64 of Temporal's opaque cursor.
func encodePageToken(token []byte) string {
	if len(token) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(token)
}

func decodePageToken(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("page token: %w", err)
	}
	return raw, nil
}

// --- ResourcesAPI ---

// List returns the resources matching the selector.
func (m *Management) List(ctx context.Context, creq *connect.Request[managementv1.ListRequest]) (*connect.Response[managementv1.ListResponse], error) {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbList, listedKind(req.GetSelector(), req.GetQuery()))
	if err != nil {
		return nil, err
	}
	query, err := listQuery(req.GetQuery(), req.GetSelector())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	token, err := decodePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	resp := &managementv1.ListResponse{}
	for {
		page, err := b.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query:         query,
			PageSize:      req.GetPageSize(),
			NextPageToken: token,
		})
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		for _, e := range page.GetExecutions() {
			// The row comes from VISIBILITY alone: ref, kind, phase,
			// owner, and labels are all mirrored search attributes. A
			// per-entity describe is a workflow QUERY — it wakes the
			// record's worker and made listing O(n) queries; the full
			// record stays behind Get.
			resp.Resources = append(resp.Resources, resourceFromVisibility(e))
		}
		token = page.GetNextPageToken()
		// A bounded request answers with ONE page and the cursor; an
		// unbounded one keeps the old drain-everything behavior.
		if req.GetPageSize() > 0 {
			resp.NextPageToken = encodePageToken(token)
			return connect.NewResponse(resp), nil
		}
		if len(token) == 0 {
			return connect.NewResponse(resp), nil
		}
	}
}

// Count answers how many records match, optionally grouped by
// execution status — visibility's CountWorkflow, no rows fetched.
func (m *Management) Count(ctx context.Context, creq *connect.Request[managementv1.CountRequest]) (*connect.Response[managementv1.CountResponse], error) {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbList, listedKind(req.GetSelector(), req.GetQuery()))
	if err != nil {
		return nil, err
	}
	query, err := listQuery(req.GetQuery(), req.GetSelector())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if req.GetGroupByStatus() {
		query += " GROUP BY ExecutionStatus"
	}
	out, err := b.Client.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{Query: query})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &managementv1.CountResponse{Total: out.GetCount()}
	for _, g := range out.GetGroups() {
		group := &managementv1.CountResponse_Group{Count: g.GetCount()}
		for _, v := range g.GetGroupValues() {
			var s string
			if err := converter.GetDefaultDataConverter().FromPayload(v, &s); err == nil {
				group.Status = s
				break
			}
		}
		resp.Groups = append(resp.Groups, group)
	}
	return connect.NewResponse(resp), nil
}

// CountOwned answers how many live records each named owner holds —
// one parallel sweep of cheap visibility counts.
func (m *Management) CountOwned(ctx context.Context, creq *connect.Request[managementv1.CountOwnedRequest]) (*connect.Response[managementv1.CountOwnedResponse], error) {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbList, authz.KindAll)
	if err != nil {
		return nil, err
	}
	owners := req.GetOwners()
	if len(owners) > 100 {
		return nil, status.Error(codes.InvalidArgument, "at most 100 owners per call")
	}
	if err := noQuotes(owners...); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	counts := make(map[string]int64, len(owners))
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, owner := range owners {
		g.Go(func() error {
			out, err := b.Client.CountWorkflow(gctx, &workflowservice.CountWorkflowExecutionsRequest{
				Query: fmt.Sprintf("EntityOwner = '%s' AND ExecutionStatus = 'Running'", owner),
			})
			if err != nil {
				return err
			}
			mu.Lock()
			counts[owner] = out.GetCount()
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return connect.NewResponse(&managementv1.CountOwnedResponse{Counts: counts}), nil
}

// Get describes one resource.
func (m *Management) Get(ctx context.Context, creq *connect.Request[managementv1.GetRequest]) (*connect.Response[managementv1.GetResponse], error) {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbGet, authz.KindOf(req.GetRef()))
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
	b, err := m.allow(ctx, authz.VerbList, authz.KindOf(req.GetOwner()))
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
	// Visibility carries everything a tree node shows; a describe per
	// node would wake every record's worker.
	page, err := b.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Query: fmt.Sprintf("%s = '%s' AND ExecutionStatus = 'Running'",
			wire.SearchAttrOwner.GetName(), string(owner)),
	})
	if err != nil {
		return nil, err
	}
	var out []*managementv1.TreeNode
	for _, e := range page.GetExecutions() {
		node := &managementv1.TreeNode{Resource: resourceFromVisibility(e)}
		grand, err := m.subtree(ctx, b, ref.OwnerRef(node.Resource.GetRef()))
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
	b, err := m.allow(ctx, authz.VerbDelete, authz.KindOf(req.GetRef()))
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
	b, err := m.allow(ctx, authz.VerbTransfer, authz.KindOf(req.GetRef()))
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
	b, err := m.allow(ctx, authz.VerbInvoke, authz.KindOf(req.GetRef()))
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
	if _, err := m.allow(ctx, authz.VerbCreate, authz.KindNamespace); err != nil {
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

// ServerInfo reports the installation for the console's status line:
// build version and component health. Temporal is probed with the
// cheapest possible call; the answer is a fact about NOW.
func (m *Management) ServerInfo(ctx context.Context, _ *connect.Request[managementv1.ServerInfoRequest]) (*connect.Response[managementv1.ServerInfoResponse], error) {
	if _, ok := auth.FromContext(ctx); !ok {
		return nil, status.Error(codes.Unauthenticated, "no principal")
	}
	resp := &managementv1.ServerInfoResponse{Version: m.Version}
	temporalOk, detail := true, ""
	probe, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := m.Base.CheckHealth(probe, &client.CheckHealthRequest{}); err != nil {
		temporalOk, detail = false, err.Error()
	}
	resp.Components = append(resp.Components, &managementv1.ServerInfoResponse_Component{
		Name: "temporal", Ok: temporalOk, Detail: detail,
	})
	return connect.NewResponse(resp), nil
}

// ListNamespaces lists the registered namespaces.
func (m *Management) ListNamespaces(ctx context.Context, _ *connect.Request[managementv1.ListNamespacesRequest]) (*connect.Response[managementv1.ListNamespacesResponse], error) {
	if _, err := m.allow(ctx, authz.VerbList, authz.KindNamespace); err != nil {
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
	b, err := m.allow(ctx, authz.VerbCreate, authz.KindSecret)
	if err != nil {
		return nil, err
	}
	namespace := b.Namespace
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	m.Secrets.Set(namespace, req.GetName(), req.GetValue())
	return connect.NewResponse(&managementv1.SetSecretResponse{}), nil
}

// DeleteSecret forgets a name.
func (m *Management) DeleteSecret(ctx context.Context, creq *connect.Request[managementv1.DeleteSecretRequest]) (*connect.Response[managementv1.DeleteSecretResponse], error) {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbDelete, authz.KindSecret)
	if err != nil {
		return nil, err
	}
	namespace := b.Namespace
	m.Secrets.Delete(namespace, req.GetName())
	return connect.NewResponse(&managementv1.DeleteSecretResponse{}), nil
}

// ListSecrets lists names — never values.
func (m *Management) ListSecrets(ctx context.Context, _ *connect.Request[managementv1.ListSecretsRequest]) (*connect.Response[managementv1.ListSecretsResponse], error) {
	b, err := m.allow(ctx, authz.VerbList, authz.KindSecret)
	if err != nil {
		return nil, err
	}
	namespace := b.Namespace
	return connect.NewResponse(&managementv1.ListSecretsResponse{Names: m.Secrets.List(namespace)}), nil
}

// --- VarsAPI (management) ---

// SetVar stores a variable — the visible sibling of a secret.
func (m *Management) SetVar(ctx context.Context, creq *connect.Request[managementv1.SetVarRequest]) (*connect.Response[managementv1.SetVarResponse], error) {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbCreate, authz.KindVar)
	if err != nil {
		return nil, err
	}
	namespace := b.Namespace
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	m.Vars.Set(namespace, req.GetName(), req.GetValue())
	return connect.NewResponse(&managementv1.SetVarResponse{}), nil
}

// DeleteVar forgets a name.
func (m *Management) DeleteVar(ctx context.Context, creq *connect.Request[managementv1.DeleteVarRequest]) (*connect.Response[managementv1.DeleteVarResponse], error) {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbDelete, authz.KindVar)
	if err != nil {
		return nil, err
	}
	namespace := b.Namespace
	m.Vars.Delete(namespace, req.GetName())
	return connect.NewResponse(&managementv1.DeleteVarResponse{}), nil
}

// ListVars returns names AND values — that is the plane's difference
// from secrets.
func (m *Management) ListVars(ctx context.Context, _ *connect.Request[managementv1.ListVarsRequest]) (*connect.Response[managementv1.ListVarsResponse], error) {
	b, err := m.allow(ctx, authz.VerbList, authz.KindVar)
	if err != nil {
		return nil, err
	}
	namespace := b.Namespace
	items := m.Vars.Items(namespace)
	names := make([]string, 0, len(items))
	for name := range items {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*managementv1.ListVarsResponse_Var, 0, len(names))
	for _, name := range names {
		out = append(out, &managementv1.ListVarsResponse_Var{Name: name, Value: items[name]})
	}
	return connect.NewResponse(&managementv1.ListVarsResponse{Vars: out}), nil
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

// resourceFromVisibility builds a listing row from ONE visibility
// execution: ref, kind, phase, owner, and labels are all mirrored
// search attributes — no workflow query, no worker woken. Spec and
// state stay behind Get: a listing is a projection, not a describe.
func resourceFromVisibility(e *workflowpb.WorkflowExecutionInfo) *managementv1.Resource {
	workflowId := e.GetExecution().GetWorkflowId()
	kind, _, _ := strings.Cut(workflowId, "/")
	res := &managementv1.Resource{
		Ref:        workflowId,
		Kind:       kind,
		StartedAt:  e.GetStartTime(),
		FinishedAt: e.GetCloseTime(),
	}
	fields := e.GetSearchAttributes().GetIndexedFields()
	dc := converter.GetDefaultDataConverter()
	str := func(name string) string {
		p, ok := fields[name]
		if !ok {
			return ""
		}
		var out string
		_ = dc.FromPayload(p, &out)
		return out
	}
	res.Phase = str(entdefine.SearchAttrPhase.GetName())
	res.Owner = str(wire.SearchAttrOwner.GetName())
	if p, ok := fields[entdefine.SearchAttrLabels.GetName()]; ok {
		var pairs []string
		_ = dc.FromPayload(p, &pairs)
		labels := make(map[string]string, len(pairs))
		for _, pair := range pairs {
			if k, v, found := strings.Cut(pair, "="); found {
				labels[k] = v
			}
		}
		if len(labels) > 0 {
			res.Labels = labels
		}
	}
	// A run row is a workflow, not an entity record: its phase is the
	// execution status and the pipeline rides as a synthetic label.
	if kind == selector.KindRun {
		res.Phase = e.GetStatus().String()
		if res.Labels == nil {
			res.Labels = map[string]string{}
		}
		res.Labels["graphene.io/pipeline"] = e.GetType().GetName()
	}
	return res
}
