package services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/gopherex/xlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/infrastructure/blob"
	"github.com/graphene-ci/graphene/internal/nsbundle"
	"github.com/graphene-ci/graphene/internal/secrets"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	workerplanev1 "github.com/graphene-ci/pipeline/pkg/proto/workerplane/v1"
)

// WorkerPlane serves the running user code: secrets at the point of
// use, capability publication, blob bytes. Run tokens only.
type WorkerPlane struct {
	workerplanev1.UnimplementedSecretsAPIServer
	workerplanev1.UnimplementedCapabilitiesAPIServer
	workerplanev1.UnimplementedBlobsAPIServer

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
