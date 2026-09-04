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
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/gopherex/xlog"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/authz"
	syslabels "github.com/graphene-ci/graphene/internal/labels"
	"github.com/graphene-ci/graphene/internal/nsbundle"
	"github.com/graphene-ci/graphene/internal/revisionflow"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/wire"
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
	if pipelineId == "" {
		return status.Error(codes.InvalidArgument, "pipeline_id is required")
	}
	sourceLocation, digest, runtimeName := "", "", ""
	// What the revision was built FROM, recorded so a run can be traced
	// back to the code without walking anything.
	builtFrom, commit := "", ""

	switch {
	case len(req.GetSource()) > 0:
		// A tree uploaded for THIS build — a developer's machine, or a
		// client that keeps the source itself.
		if len(req.GetSource()) > maxSourceBytes {
			return status.Errorf(codes.InvalidArgument, "source must be a tar.gz up to %d bytes", maxSourceBytes)
		}
		sum := sha256.Sum256(req.GetSource())
		digest = hex.EncodeToString(sum[:])
		sourceLocation = fmt.Sprintf("revisions/%s/%s/source.tgz", pipelineId, digest[:16])
		if _, err := m.Blobs.Put(ctx, b.Namespace, sourceLocation, bytes.NewReader(req.GetSource())); err != nil {
			return status.Errorf(codes.Internal, "store source: %v", err)
		}
	default:
		// Building a SOURCE record of the pipeline: nothing is uploaded
		// here, its tree already lives on the server.
		sourceRef, err := m.resolveSource(ctx, b, pipelineId, req.GetSourceRef())
		if err != nil {
			return err
		}
		raw, err := b.Worker.SourceArchive(ctx, sourceRef)
		if err != nil {
			return status.Errorf(codes.FailedPrecondition, "source %s: %v", sourceRef, err)
		}
		sum := sha256.Sum256(raw)
		digest = hex.EncodeToString(sum[:])
		sourceLocation = fmt.Sprintf("revisions/%s/%s/source.tgz", pipelineId, digest[:16])
		if _, err := m.Blobs.Put(ctx, b.Namespace, sourceLocation, bytes.NewReader(raw)); err != nil {
			return status.Errorf(codes.Internal, "store source: %v", err)
		}
		// The toolchain follows the CODE, so it comes from the source.
		if runtimeName, err = b.Worker.SourceRuntime(ctx, sourceRef); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		builtFrom = sourceRef
		if id, ok := strings.CutPrefix(sourceRef, "gitsource/"); ok {
			if _, _, st, gerr := b.Worker.DescribeGitSource(ctx, id); gerr == nil {
				commit = st.Commit
			}
		}
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
			SourceRef:      builtFrom,
			SourceLocation: sourceLocation,
			SourceDigest:   "sha256:" + digest,
			Runtime:        runtimeName,
			Commit:         commit,
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
			// A built revision proves the project exists (its binary's name
			// was checked against the id), so ensure the pipeline record is
			// there — create-or-attach, idempotent. Without this the
			// source-first contour never births pipeline/<id> (only the
			// push door did), and the first `activate` of a NEW pipeline
			// fails "workflow not found". The record starts empty; activate
			// fills its manifest, image and active revision.
			if _, err := b.Worker.Apply(ctx, "pipeline", pipelineId, json.RawMessage("{}"), nil); err != nil {
				return status.Errorf(codes.Internal, "ensure pipeline record: %v", err)
			}
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

// resolveSource picks which source to build: the named one, or the
// pipeline's only one. A pipeline with several sources must be told —
// guessing would build yesterday's fork by accident.
func (m *Management) resolveSource(ctx context.Context, b *nsbundle.Bundle, pipelineId, named string) (string, error) {
	if named != "" {
		return named, nil
	}
	sources, err := b.Worker.SourcesOf(ctx, pipelineId)
	if err != nil {
		return "", status.Error(codes.Internal, err.Error())
	}
	switch len(sources) {
	case 0:
		return "", status.Errorf(codes.FailedPrecondition,
			"pipeline %s has no source: declare a gitsource under it", pipelineId)
	case 1:
		return sources[0], nil
	}
	return "", status.Errorf(codes.InvalidArgument,
		"pipeline %s has %d sources (%s): name the one to build", pipelineId, len(sources), strings.Join(sources, ", "))
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
	_, rspec, _, _ := b.Worker.DescribeRevision(ctx, req.GetPipelineId(), req.GetRevisionId())
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
	// A draft run carries the SAME system markers an ordinary one
	// does: which revision it executes is exactly the question a draft
	// exists to answer.
	marked := syslabels.Merge(copyLabels(req.GetLabels()), map[string]string{
		syslabels.Pipeline:     req.GetPipelineId(),
		syslabels.Revision:     req.GetRevisionId(),
		syslabels.Source:       rspec.SourceRef,
		syslabels.SourceDigest: shortDigest(rspec.SourceDigest),
		syslabels.Image:        rev.Image,
		syslabels.Trigger:      "draft",
	})
	opts := client.StartWorkflowOptions{
		ID:                       "run/" + string(runId),
		TaskQueue:                wire.RunQueue(runId),
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		TypedSearchAttributes:    runAttributes(req.GetPipelineId(), "", marked),
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

// copyLabels clones the user's labels so system markers never mutate a
// request message.
func copyLabels(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
