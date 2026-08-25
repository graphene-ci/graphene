package services

// RevisionsAPI — the source-first contour's door: source in, revision
// out; draft runs and activation against EXPLICIT revisions. The proof
// of the Studio model: no user-side push anywhere on this path.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"connectrpc.com/connect"
	"github.com/gopherex/xlog"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/materialize"
	"github.com/graphene-ci/graphene/internal/pipelineflow"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/wire"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
)

// maxSourceBytes bounds one uploaded source tree (tar.gz).
const maxSourceBytes = 64 << 20

// heartbeatEvery keeps a silent build's stream alive: `go build`
// prints nothing for minutes and an idle connection dies in the first
// NAT between the client and the door.
const heartbeatEvery = 15 * time.Second

// Materialize builds one source tree into a pipeline revision,
// streaming the build as it happens — the Studio build log.
func (m *Management) Materialize(ctx context.Context, creq *connect.Request[managementv1.MaterializeRequest], stream *connect.ServerStream[managementv1.MaterializeEvent]) error {
	req := creq.Msg
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return err
	}
	if m.Materializer == nil {
		return status.Error(codes.Unimplemented, "materialization is not configured on this installation")
	}
	if req.GetPipelineId() == "" {
		return status.Error(codes.InvalidArgument, "pipeline_id is required")
	}
	if len(req.GetSource()) == 0 || len(req.GetSource()) > maxSourceBytes {
		return status.Errorf(codes.InvalidArgument, "source must be a tar.gz up to %d bytes", maxSourceBytes)
	}

	// The build runs in its own goroutine; this one owns the stream —
	// one writer, no interleaving.
	type outcome struct {
		res materialize.Result
		err error
	}
	events := make(chan *managementv1.MaterializeEvent, 256)
	done := make(chan outcome, 1)
	go func() {
		res, err := m.Materializer.Materialize(ctx, b.Namespace, req.GetPipelineId(), req.GetSource(),
			func(stage, message string) {
				select {
				case events <- &managementv1.MaterializeEvent{Stage: stage, Message: message}:
				default: // a slow reader never blocks the build
				}
			})
		close(events)
		done <- outcome{res: res, err: err}
	}()

	started := time.Now()
	beat := time.NewTicker(heartbeatEvery)
	defer beat.Stop()
	for events != nil {
		select {
		case ev, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			if err := stream.Send(ev); err != nil {
				return err
			}
		case <-beat.C:
			if err := stream.Send(&managementv1.MaterializeEvent{
				Stage:   "build",
				Message: fmt.Sprintf("working… %s elapsed", time.Since(started).Round(time.Second)),
			}); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	out := <-done
	if out.err != nil {
		return status.Errorf(codes.FailedPrecondition, "materialize: %v", out.err)
	}
	res := out.res
	if err := b.Worker.AddRevision(ctx, req.GetPipelineId(), res.RevisionId, pipelineflow.Revision{
		Image:            res.ImageRef,
		ManifestLocation: res.ManifestLocation,
		SourceDigest:     res.SourceDigest,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	m.Log.Info("revision materialized",
		xlog.String("namespace", b.Namespace),
		xlog.String("pipeline", req.GetPipelineId()),
		xlog.String("revision", res.RevisionId))
	return stream.Send(&managementv1.MaterializeEvent{
		Stage:   "done",
		Message: fmt.Sprintf("revision %s in %s", res.RevisionId, time.Since(started).Round(time.Second)),
		Result: &managementv1.MaterializeResult{
			RevisionId: res.RevisionId,
			Image:      res.ImageRef,
			Manifest:   res.Manifest,
		},
	})
}

// ListRevisions lists the pipeline's materialized revisions; the
// active one is whichever image the pipeline currently points at.
func (m *Management) ListRevisions(ctx context.Context, creq *connect.Request[managementv1.ListRevisionsRequest]) (*connect.Response[managementv1.ListRevisionsResponse], error) {
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	st, err := b.Worker.GetPipeline(ctx, creq.Msg.GetPipelineId())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	ids := make([]string, 0, len(st.Revisions))
	for rid := range st.Revisions {
		ids = append(ids, rid)
	}
	sort.Slice(ids, func(i, j int) bool {
		return st.Revisions[ids[i]].CreatedAt > st.Revisions[ids[j]].CreatedAt
	})
	out := make([]*managementv1.ListRevisionsResponse_Revision, 0, len(ids))
	for _, rid := range ids {
		rev := st.Revisions[rid]
		out = append(out, &managementv1.ListRevisionsResponse_Revision{
			Id:           rid,
			Image:        rev.Image,
			SourceDigest: rev.SourceDigest,
			CreatedAt:    rev.CreatedAt,
			Active:       rev.Image == st.Image && st.Image != "",
		})
	}
	return connect.NewResponse(&managementv1.ListRevisionsResponse{Revisions: out}), nil
}

// RunRevision starts a DRAFT run of one explicit revision: validated
// against that revision's manifest, executed with that revision's
// image — active or not.
func (m *Management) RunRevision(ctx context.Context, creq *connect.Request[managementv1.RunRevisionRequest]) (*connect.Response[managementv1.RunRevisionResponse], error) {
	req := creq.Msg
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	rev, manifest, err := m.revisionOf(ctx, b.Namespace, req.GetPipelineId(), req.GetRevisionId())
	if err != nil {
		return nil, err
	}
	runId, err := id.ParseRunId(req.GetRunId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := wire.ValidateUserLabels(req.GetLabels()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	params := req.GetParams()
	if params, err = substituteVars(params, b.Vars); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if params, err = validateParams(manifest, params); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := checkSecretRefs(manifest, params, b.Secrets); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	opts := client.StartWorkflowOptions{
		ID:                       "run/" + string(runId),
		TaskQueue:                wire.RunQueue(runId),
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		TypedSearchAttributes: temporal.NewSearchAttributes(
			entdefine.SearchAttrLabels.ValueSet(labelPairs(req.GetLabels()))),
	}
	var args []any
	if len(params) > 0 {
		args = append(args, json.RawMessage(params))
	}
	run, err := b.Client.ExecuteWorkflow(ctx, opts, req.GetPipelineId(), args...)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := b.Runner.Start(ctx, runId, rev.Image); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	m.Log.Info("draft run started",
		xlog.String("namespace", b.Namespace),
		xlog.Any("run", runId),
		xlog.String("pipeline", req.GetPipelineId()),
		xlog.String("revision", req.GetRevisionId()))
	return connect.NewResponse(&managementv1.RunRevisionResponse{WorkflowId: run.GetID(), TemporalRunId: run.GetRunID()}), nil
}

// ActivateRevision makes one revision the version automatic starts
// use: the ordinary publish path with the revision's manifest and
// image — the pipeline record updates atomically and the triggers
// reconcile against the NEW declaration set.
func (m *Management) ActivateRevision(ctx context.Context, creq *connect.Request[managementv1.ActivateRevisionRequest]) (*connect.Response[managementv1.ActivateRevisionResponse], error) {
	req := creq.Msg
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	rev, manifest, err := m.revisionOf(ctx, b.Namespace, req.GetPipelineId(), req.GetRevisionId())
	if err != nil {
		return nil, err
	}
	if err := b.Worker.PublishManifest(ctx, req.GetPipelineId(), manifest, rev.Image); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	m.Log.Info("revision activated",
		xlog.String("namespace", b.Namespace),
		xlog.String("pipeline", req.GetPipelineId()),
		xlog.String("revision", req.GetRevisionId()))
	return connect.NewResponse(&managementv1.ActivateRevisionResponse{}), nil
}

// revisionOf resolves one revision and loads its manifest blob.
func (m *Management) revisionOf(ctx context.Context, namespace, pipelineId, revisionId string) (pipelineflow.Revision, json.RawMessage, error) {
	b, err := m.Bundles.Get(namespace)
	if err != nil {
		return pipelineflow.Revision{}, nil, status.Error(codes.Internal, err.Error())
	}
	st, err := b.Worker.GetPipeline(ctx, pipelineId)
	if err != nil {
		return pipelineflow.Revision{}, nil, status.Error(codes.NotFound, err.Error())
	}
	rev, ok := st.Revisions[revisionId]
	if !ok {
		return pipelineflow.Revision{}, nil, status.Errorf(codes.NotFound, "pipeline %s has no revision %s", pipelineId, revisionId)
	}
	rc, err := m.Blobs.Get(ctx, namespace, rev.ManifestLocation)
	if err != nil {
		return pipelineflow.Revision{}, nil, status.Errorf(codes.Internal, "revision manifest: %v", err)
	}
	defer func() { _ = rc.Close() }()
	manifest, err := io.ReadAll(rc)
	if err != nil {
		return pipelineflow.Revision{}, nil, status.Errorf(codes.Internal, "revision manifest: %v", err)
	}
	return rev, manifest, nil
}
