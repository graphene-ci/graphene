package services

// RevisionsAPI — the source-first contour's door: source in, revision
// out; draft runs and activation against EXPLICIT revisions. The proof
// of the Studio model: no user-side push anywhere on this path.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/gopherex/xlog"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/authz"
	"github.com/graphene-ci/graphene/internal/revisionflow"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/wire"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
)

// maxSourceBytes bounds one uploaded source tree (tar.gz).
const maxSourceBytes = 64 << 20

// pollEvery is how often the stream re-reads the revision record while
// its build runs.
const pollEvery = 3 * time.Second

// Materialize uploads a source tree and declares its REVISION — the
// build is the record's own Init, so this call only watches it. The
// client may hang up at any moment: the build lives in the record, not
// in this connection.
func (m *Management) Materialize(ctx context.Context, creq *connect.Request[managementv1.MaterializeRequest], stream *connect.ServerStream[managementv1.MaterializeEvent]) error {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbBuild, authz.KindRevision)
	if err != nil {
		return err
	}
	pipelineId := req.GetPipelineId()
	sourceLocation, digest, runtimeName := "", "", ""
	workspaceId := req.GetWorkspaceId()

	switch {
	case workspaceId != "":
		// Building a WORKSPACE: its own working tree is the source and
		// the pipeline is the one it publishes. Nothing is uploaded
		// here — the tree already lives on the server.
		_, spec, st, err := b.Worker.DescribeWorkspace(ctx, workspaceId)
		if err != nil {
			return status.Errorf(codes.NotFound, "workspace %s: %v", workspaceId, err)
		}
		if st.TreeLocation == "" {
			return status.Errorf(codes.FailedPrecondition, "workspace %s has no working tree yet", workspaceId)
		}
		if pipelineId == "" {
			pipelineId = st.PipelineRef
			if pipelineId == "" {
				pipelineId = spec.PipelineId
			}
		}
		if pipelineId == "" {
			return status.Error(codes.InvalidArgument, "workspace publishes no pipeline yet: name one with pipeline_id")
		}
		sourceLocation = st.TreeLocation
		digest = strings.TrimPrefix(st.TreeDigest, "sha256:")
		// The project's language is the workspace's, not the build's.
		runtimeName = spec.Runtime
	case len(req.GetSource()) > 0:
		if len(req.GetSource()) > maxSourceBytes {
			return status.Errorf(codes.InvalidArgument, "source must be a tar.gz up to %d bytes", maxSourceBytes)
		}
		if pipelineId == "" {
			return status.Error(codes.InvalidArgument, "pipeline_id is required")
		}
		sum := sha256.Sum256(req.GetSource())
		digest = hex.EncodeToString(sum[:])
		sourceLocation = fmt.Sprintf("sources/%s/%s.tgz", pipelineId, digest[:16])
		if _, err := m.Blobs.Put(ctx, b.Namespace, sourceLocation, bytes.NewReader(req.GetSource())); err != nil {
			return status.Errorf(codes.Internal, "store source: %v", err)
		}
	default:
		return status.Error(codes.InvalidArgument, "materialize needs a workspace_id or a source tree")
	}

	// The revision id IS the source digest: the same tree declares the
	// same record, and create-or-attach makes deduplication free.
	if len(digest) < 16 {
		return status.Error(codes.FailedPrecondition, "source has no digest")
	}
	revisionId := digest[:16]
	if err := stream.Send(&managementv1.MaterializeEvent{
		Stage: "upload", Message: "source stored, declaring revision " + revisionId,
	}); err != nil {
		return err
	}

	// Declaring is fire-and-forget: the record's Init runs the build.
	declared := make(chan error, 1)
	go func() {
		declared <- b.Worker.DeclareRevision(context.WithoutCancel(ctx), pipelineId, revisionId, revisionflow.Spec{
			PipelineId:     pipelineId,
			SourceLocation: sourceLocation,
			SourceDigest:   "sha256:" + digest,
			Runtime:        runtimeName,
		})
	}()

	started := time.Now()
	lastBeat := ""
	tick := time.NewTicker(pollEvery)
	defer tick.Stop()
	for {
		select {
		case err := <-declared:
			if err != nil {
				return status.Errorf(codes.FailedPrecondition, "revision %s: %v", revisionId, err)
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
		phase, _, st, err := b.Worker.DescribeRevision(ctx, pipelineId, revisionId)
		if err != nil {
			// The record may not be visible for a moment after declare.
			continue
		}
		if beat := b.Worker.RevisionProgress(ctx, pipelineId, revisionId); beat != "" && beat != lastBeat {
			lastBeat = beat
			stage, message, _ := strings.Cut(beat, ": ")
			if err := stream.Send(&managementv1.MaterializeEvent{Stage: stage, Message: message}); err != nil {
				return err
			}
		}
		switch phase {
		case entity.PhaseReady:
			manifest, err := m.blobBytes(ctx, b.Namespace, st.ManifestLocation)
			if err != nil {
				return status.Errorf(codes.Internal, "revision manifest: %v", err)
			}
			m.Log.Info("revision materialized",
				xlog.String("namespace", b.Namespace),
				xlog.String("pipeline", pipelineId),
				xlog.String("revision", revisionId))
			return stream.Send(&managementv1.MaterializeEvent{
				Stage:   "done",
				Message: fmt.Sprintf("revision %s in %s", revisionId, time.Since(started).Round(time.Second)),
				Result: &managementv1.MaterializeResult{
					RevisionId: revisionId,
					Image:      st.Image,
					Manifest:   manifest,
				},
			})
		case entity.PhaseCreateFailed:
			buildLog, _ := m.blobBytes(ctx, b.Namespace, st.LogLocation)
			return status.Errorf(codes.FailedPrecondition, "revision %s failed: %s\n%s", revisionId, st.Error, tailBytes(buildLog, 4000))
		case entity.PhaseDeleting, entity.PhaseDeleted, entity.PhaseDeleteFailed:
			return status.Errorf(codes.FailedPrecondition, "revision %s is going away (%s)", revisionId, phase)
		case entity.PhaseCreating:
			if time.Since(started) > buildDeadline {
				return status.Errorf(codes.DeadlineExceeded, "revision %s is still building; watch it with `graphenectl get revision %s.%s`",
					revisionId, pipelineId, revisionId)
			}
		}
	}
}

// buildDeadline bounds how long this STREAM waits; the build itself
// keeps going in its record.
const buildDeadline = 30 * time.Minute

// blobBytes reads one blob whole.
func (m *Management) blobBytes(ctx context.Context, namespace, location string) ([]byte, error) {
	if location == "" {
		return nil, nil
	}
	rc, err := m.Blobs.Get(ctx, namespace, location)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(rc)
}

func tailBytes(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return "…" + string(b[len(b)-n:])
}

// ListRevisions lists the pipeline's materialized revisions; the
// active one is whichever image the pipeline currently points at.
func (m *Management) ListRevisions(ctx context.Context, creq *connect.Request[managementv1.ListRevisionsRequest]) (*connect.Response[managementv1.ListRevisionsResponse], error) {
	b, err := m.allow(ctx, authz.VerbList, authz.KindRevision)
	if err != nil {
		return nil, err
	}
	pipelineId := creq.Msg.GetPipelineId()
	ids, err := b.Worker.ListRevisions(ctx, pipelineId)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	activeImage := ""
	if st, err := b.Worker.GetPipeline(ctx, pipelineId); err == nil {
		activeImage = st.Image
	}
	out := make([]*managementv1.ListRevisionsResponse_Revision, 0, len(ids))
	for _, rid := range ids {
		phase, spec, st, err := b.Worker.DescribeRevision(ctx, pipelineId, rid)
		if err != nil {
			continue
		}
		out = append(out, &managementv1.ListRevisionsResponse_Revision{
			Id:           rid,
			Image:        st.Image,
			SourceDigest: spec.SourceDigest,
			CreatedAt:    st.CreatedAt,
			Phase:        string(phase),
			Active:       st.Image != "" && st.Image == activeImage,
		})
	}
	// Newest first — a developer looks at what they just built.
	sort.Slice(out, func(i, j int) bool { return out[i].GetCreatedAt() > out[j].GetCreatedAt() })
	return connect.NewResponse(&managementv1.ListRevisionsResponse{Revisions: out}), nil
}

// RunRevision starts a DRAFT run of one explicit revision: validated
// against that revision's manifest, executed with that revision's
// image — active or not.
func (m *Management) RunRevision(ctx context.Context, creq *connect.Request[managementv1.RunRevisionRequest]) (*connect.Response[managementv1.RunRevisionResponse], error) {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbRun, authz.KindPipeline)
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
	if err := b.Runner.Start(ctx, runId, rev.Image, mintRunToken(b, runId)); err != nil {
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
	b, err := m.allow(ctx, authz.VerbActivate, authz.KindRevision)
	if err != nil {
		return nil, err
	}
	rev, manifest, err := m.revisionOf(ctx, b.Namespace, req.GetPipelineId(), req.GetRevisionId())
	if err != nil {
		return nil, err
	}
	// A workspace publishes ONE pipeline: activation records the
	// binding on both sides — the workspace refuses a second pipeline,
	// the pipeline joins the workspace's tree.
	workspaceId := req.GetWorkspaceId()
	if err := b.Worker.PublishManifestFromWorkspace(ctx, req.GetPipelineId(), manifest, rev.Image, workspaceId); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if workspaceId != "" {
		if err := b.Worker.BindWorkspacePipeline(ctx, workspaceId, req.GetPipelineId()); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "bind pipeline: %v", err)
		}
	}
	m.Log.Info("revision activated",
		xlog.String("namespace", b.Namespace),
		xlog.String("pipeline", req.GetPipelineId()),
		xlog.String("revision", req.GetRevisionId()))
	return connect.NewResponse(&managementv1.ActivateRevisionResponse{}), nil
}

// revisionOf resolves one revision record and loads its manifest.
func (m *Management) revisionOf(ctx context.Context, namespace, pipelineId, revisionId string) (revisionflow.State, json.RawMessage, error) {
	b, err := m.Bundles.Get(namespace)
	if err != nil {
		return revisionflow.State{}, nil, status.Error(codes.Internal, err.Error())
	}
	phase, _, st, err := b.Worker.DescribeRevision(ctx, pipelineId, revisionId)
	if err != nil {
		return revisionflow.State{}, nil, status.Errorf(codes.NotFound, "pipeline %s has no revision %s", pipelineId, revisionId)
	}
	if phase != entity.PhaseReady {
		return revisionflow.State{}, nil, status.Errorf(codes.FailedPrecondition, "revision %s is %s, not ready", revisionId, phase)
	}
	manifest, err := m.blobBytes(ctx, namespace, st.ManifestLocation)
	if err != nil {
		return revisionflow.State{}, nil, status.Errorf(codes.Internal, "revision manifest: %v", err)
	}
	return st, manifest, nil
}
