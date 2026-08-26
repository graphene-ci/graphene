package services

// PipelinesAPI — the project's working area: one source (a Git
// checkout or an uploaded tree), one runtime, one working tree kept on
// the server, at most one pipeline. Listing, reading and deleting go
// through the ordinary ResourcesAPI: a pipeline is a record like any
// other, and its deletion is the end of the project.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"bytes"

	"connectrpc.com/connect"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/authz"
	"github.com/graphene-ci/graphene/internal/runtimes"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// downloadChunk is the size of one source-download frame.
const downloadChunk = 512 << 10

// UploadSource stores a source tree and hands back its reference —
// the channel bytes travel by, so that declarations and commands can
// carry a reference instead.
func (m *Management) UploadSource(ctx context.Context, creq *connect.Request[managementv1.UploadSourceRequest]) (*connect.Response[managementv1.UploadSourceResponse], error) {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbCreate, authz.KindPipeline)
	if err != nil {
		return nil, err
	}
	if len(req.GetSource()) == 0 || len(req.GetSource()) > maxSourceBytes {
		return nil, status.Errorf(codes.InvalidArgument, "source must be a tar.gz up to %d bytes", maxSourceBytes)
	}
	location, digest, err := m.storeTree(ctx, b.Namespace, req.GetPipelineId(), req.GetSource())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&managementv1.UploadSourceResponse{Location: location, Digest: digest}), nil
}

// DownloadSource streams a source's tree back as one archive — a
// checkout as it is, a managed tree packed from its files.
func (m *Management) DownloadSource(ctx context.Context, creq *connect.Request[managementv1.DownloadSourceRequest], stream *connect.ServerStream[managementv1.DownloadSourceChunk]) error {
	b, err := m.allow(ctx, authz.VerbGet, authz.KindOf(creq.Msg.GetSource()))
	if err != nil {
		return err
	}
	raw, err := b.Worker.SourceArchive(ctx, creq.Msg.GetSource())
	if err != nil {
		return status.Error(codes.NotFound, err.Error())
	}
	rc := bytes.NewReader(raw)
	buf := make([]byte, downloadChunk)
	for {
		n, err := rc.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&managementv1.DownloadSourceChunk{Data: buf[:n]}); sendErr != nil {
				return sendErr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "tree: %v", err)
		}
	}
}

// ListRuntimes answers which languages this installation carries.
func (m *Management) ListRuntimes(ctx context.Context, _ *connect.Request[managementv1.ListRuntimesRequest]) (*connect.Response[managementv1.ListRuntimesResponse], error) {
	if _, err := m.allow(ctx, authz.VerbList, authz.KindPipeline); err != nil {
		return nil, err
	}
	catalogue := m.Runtimes
	if catalogue == nil {
		catalogue = runtimes.New(nil)
	}
	out := make([]*managementv1.ListRuntimesResponse_Runtime, 0)
	for _, r := range catalogue.All() {
		out = append(out, &managementv1.ListRuntimesResponse_Runtime{
			Name: r.Name, Version: r.Version, Image: r.Image, IsDefault: r.Name == catalogue.Default,
		})
	}
	return connect.NewResponse(&managementv1.ListRuntimesResponse{Runtimes: out}), nil
}

// storeTree puts an uploaded tree into the blob store under the
// pipeline and returns its location and digest.
func (m *Management) storeTree(ctx context.Context, namespace, pipelineId string, tree []byte) (string, string, error) {
	if len(tree) == 0 {
		return "", "", status.Error(codes.InvalidArgument, "source is empty")
	}
	sum := sha256.Sum256(tree)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	location := fmt.Sprintf("uploads/%s/%s.tgz", pipelineId, hex.EncodeToString(sum[:])[:16])
	if _, err := m.Blobs.Put(ctx, namespace, location, bytes.NewReader(tree)); err != nil {
		return "", "", status.Errorf(codes.Internal, "store source: %v", err)
	}
	return location, digest, nil
}
