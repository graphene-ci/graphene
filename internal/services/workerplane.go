package services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/gopherex/xlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/infrastructure/blob"
	syslabels "github.com/graphene-ci/graphene/internal/labels"
	"github.com/graphene-ci/graphene/internal/nsbundle"
	"github.com/graphene-ci/graphene/internal/secrets"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	manifestpb "github.com/graphene-ci/pipeline/pkg/proto/manifest/v1"
	workerplanev1 "github.com/graphene-ci/pipeline/pkg/proto/workerplane/v1"
)

// WorkerPlane serves the running user code: secrets at the point of
// use, capability publication, blob bytes. Run tokens only.
type WorkerPlane struct {
	workerplanev1.UnimplementedSecretsAPIServer
	workerplanev1.UnimplementedCapabilitiesAPIServer
	workerplanev1.UnimplementedBlobsAPIServer
	workerplanev1.UnimplementedEventsAPIServer
	workerplanev1.UnimplementedManifestAPIServer
	workerplanev1.UnimplementedRunsAPIServer

	Bundles *nsbundle.Manager
	Secrets *secrets.Namespaced
	Blobs   blob.Store
	Log     *xlog.Logger
}

// GetSecret resolves a name to its value — once, at the point of use.
func (w *WorkerPlane) GetSecret(ctx context.Context, req *workerplanev1.GetSecretRequest) (*workerplanev1.GetSecretResponse, error) {
	namespace, err := scope(ctx, auth.RoleRun, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	value, err := w.Secrets.Get(namespace, req.GetName())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &workerplanev1.GetSecretResponse{Value: value}, nil
}

// PublishCapability writes what the machine now CAN onto its record.
func (w *WorkerPlane) PublishCapability(ctx context.Context, req *workerplanev1.PublishCapabilityRequest) (*workerplanev1.PublishCapabilityResponse, error) {
	b, err := bundleFor(ctx, w.Bundles, auth.RoleRun, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	agentId, err := id.ParseAgentId(req.GetAgentId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	capability := req.GetCapability()
	if capability.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "capability needs a name")
	}
	if err := b.Worker.PublishCapability(ctx, agentId, pipeline.Capability{
		Name:      capability.GetName(),
		Version:   capability.GetVersion(),
		Labels:    capability.GetLabels(),
		BroughtBy: capability.GetBroughtBy(),
		Ready:     capability.GetReady(),
	}); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &workerplanev1.PublishCapabilityResponse{}, nil
}

// PutBlob receives the bytes, addresses them by content, and stores
// them: the digest is computed HERE — a client cannot lie about it.
func (w *WorkerPlane) PutBlob(stream workerplanev1.BlobsAPI_PutBlobServer) error {
	namespace, err := scope(stream.Context(), auth.RoleRun, auth.RoleAdmin)
	if err != nil {
		return err
	}
	// Spool to a temp file while hashing — the location is the digest,
	// unknown until the last byte.
	tmp, err := os.CreateTemp("", "graphene-blob-*")
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	h := sha256.New()
	var size int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		chunk := msg.GetChunk()
		if _, err := h.Write(chunk); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		n, err := tmp.Write(chunk)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		size += int64(n)
	}
	sum := hex.EncodeToString(h.Sum(nil))
	location := "blobs/" + sum
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if _, err := w.Blobs.Put(stream.Context(), namespace, location, tmp); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	return stream.SendAndClose(&workerplanev1.PutBlobResponse{
		Digest:   "sha256:" + sum,
		Location: location,
		Size:     size,
	})
}

// GetBlob streams a blob's bytes back.
func (w *WorkerPlane) GetBlob(req *workerplanev1.GetBlobRequest, stream workerplanev1.BlobsAPI_GetBlobServer) error {
	namespace, err := scope(stream.Context(), auth.RoleRun, auth.RoleAdmin)
	if err != nil {
		return err
	}
	r, err := w.Blobs.Get(stream.Context(), namespace, req.GetLocation())
	if errors.Is(err, blob.ErrNotFound) {
		return status.Error(codes.NotFound, fmt.Sprintf("blob %q", req.GetLocation()))
	}
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer func() { _ = r.Close() }()
	buf := make([]byte, 1<<20)
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			if err := stream.Send(&workerplanev1.GetBlobResponse{Chunk: buf[:n]}); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return status.Error(codes.Internal, readErr.Error())
		}
	}
}

// Emit puts a domain event into an entity's history — the plane of
// truth: a milestone, not a log line.
func (w *WorkerPlane) Emit(ctx context.Context, req *workerplanev1.EmitRequest) (*workerplanev1.EmitResponse, error) {
	b, err := bundleFor(ctx, w.Bundles, auth.RoleRun, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	if req.GetRef() == "" || req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "ref and name are required")
	}
	note := map[string]json.RawMessage{
		"name": mustJSON(req.GetName()),
	}
	if len(req.GetPayload()) > 0 {
		if !json.Valid(req.GetPayload()) {
			return nil, status.Error(codes.InvalidArgument, "payload must be JSON")
		}
		note["payload"] = req.GetPayload()
	}
	if err := b.Client.SignalWorkflow(ctx, req.GetRef(), "", entity.NoteSignalName, note); err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &workerplanev1.EmitResponse{}, nil
}

// PublishManifest records what a pipeline binary is.
func (w *WorkerPlane) PublishManifest(ctx context.Context, req *workerplanev1.PublishManifestRequest) (*workerplanev1.PublishManifestResponse, error) {
	b, err := bundleFor(ctx, w.Bundles, auth.RoleRun, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	var m manifestpb.Manifest
	if err := protojson.Unmarshal(req.GetManifest(), &m); err != nil {
		return nil, status.Error(codes.InvalidArgument, "manifest: "+err.Error())
	}
	if m.GetPipelineId() == "" {
		return nil, status.Error(codes.InvalidArgument, "manifest has no pipeline id")
	}
	// A run may publish the manifest of ITS OWN pipeline only. Without
	// this a run of pipeline A could declare pipeline B under a name it
	// never owned (a run under `baseline` minting `pipeline/minimal`).
	// The run's minted token names the run; the run's workflow type is
	// its pipeline id.
	if ident, ok := auth.IdentityFrom(ctx); ok {
		if runId, isRun := strings.CutPrefix(ident.Subject.Name, "run/"); isRun {
			desc, derr := b.Client.DescribeWorkflowExecution(ctx, "run/"+runId, "")
			if derr != nil {
				return nil, status.Error(codes.PermissionDenied, "run token does not resolve to a run")
			}
			if owner := desc.GetWorkflowExecutionInfo().GetType().GetName(); owner != m.GetPipelineId() {
				return nil, status.Errorf(codes.PermissionDenied,
					"a run of %q may not publish a manifest for %q", owner, m.GetPipelineId())
			}
		}
	}
	if err := b.Worker.PublishManifest(ctx, m.GetPipelineId(), req.GetManifest(), req.GetImage()); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &workerplanev1.PublishManifestResponse{}, nil
}

// GetPipeline reads the pipeline record: the current image and the last
// published manifest — a push compares against it.
func (w *WorkerPlane) GetPipeline(ctx context.Context, req *workerplanev1.GetPipelineRequest) (*workerplanev1.GetPipelineResponse, error) {
	b, err := bundleFor(ctx, w.Bundles, auth.RoleRun, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	if req.GetPipelineId() == "" {
		return nil, status.Error(codes.InvalidArgument, "pipeline id is required")
	}
	st, err := b.Worker.GetPipeline(ctx, req.GetPipelineId())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &workerplanev1.GetPipelineResponse{Image: st.Image, Manifest: st.Manifest, Digest: st.Digest}, nil
}

// StartRun starts a run of the caller's own pipeline — the binary's
// `run` subcommand comes through this door with its run token.
func (w *WorkerPlane) StartRun(ctx context.Context, req *workerplanev1.StartRunRequest) (*workerplanev1.StartRunResponse, error) {
	b, err := bundleFor(ctx, w.Bundles, auth.RoleRun, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	workflowId, temporalRunId, err := startRunCore(ctx, b, w.Log,
		req.GetRunId(), req.GetPipeline(), req.GetParams(), req.GetImage(), req.GetLabels(),
		syslabels.TriggerManual, "")
	if err != nil {
		return nil, err
	}
	return &workerplanev1.StartRunResponse{WorkflowId: workflowId, TemporalRunId: temporalRunId}, nil
}

// GetRun reports the run's status.
func (w *WorkerPlane) GetRun(ctx context.Context, req *workerplanev1.GetRunRequest) (*workerplanev1.GetRunResponse, error) {
	b, err := bundleFor(ctx, w.Bundles, auth.RoleRun, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	desc, err := b.Client.DescribeWorkflowExecution(ctx, "run/"+req.GetRunId(), "")
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &workerplanev1.GetRunResponse{Status: desc.GetWorkflowExecutionInfo().GetStatus().String()}, nil
}

// WatchRun streams status transitions until a terminal one.
func (w *WorkerPlane) WatchRun(req *workerplanev1.WatchRunRequest, stream workerplanev1.RunsAPI_WatchRunServer) error {
	b, err := bundleFor(stream.Context(), w.Bundles, auth.RoleRun, auth.RoleAdmin)
	if err != nil {
		return err
	}
	return watchRunCore(stream.Context(), b, req.GetRunId(), func(s string) error {
		return stream.Send(&workerplanev1.WatchRunEvent{Status: s})
	})
}

// RunResult waits for the run and returns its typed result as JSON.
func (w *WorkerPlane) RunResult(ctx context.Context, req *workerplanev1.RunResultRequest) (*workerplanev1.RunResultResponse, error) {
	b, err := bundleFor(ctx, w.Bundles, auth.RoleRun, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	var out json.RawMessage
	if err := b.Client.GetWorkflow(ctx, "run/"+req.GetRunId(), "").Get(ctx, &out); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &workerplanev1.RunResultResponse{Result: out}, nil
}

func mustJSON(s string) json.RawMessage {
	raw, _ := json.Marshal(s)
	return raw
}
