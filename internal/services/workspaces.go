package services

// WorkspacesAPI — the project's working area: one source (a Git
// checkout or an uploaded tree), one runtime, one working tree kept on
// the server, at most one pipeline. Listing, reading and deleting go
// through the ordinary ResourcesAPI: a workspace is a record like any
// other, and its deletion is the end of the project.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"bytes"

	"connectrpc.com/connect"
	"github.com/gopherex/xlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/runtimes"
	"github.com/graphene-ci/graphene/internal/workspaceflow"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// downloadChunk is the size of one source-download frame.
const downloadChunk = 512 << 10

// CreateWorkspace declares a workspace: its source resolves into a
// working tree as part of the record's own creation.
func (m *Management) CreateWorkspace(ctx context.Context, creq *connect.Request[managementv1.CreateWorkspaceRequest]) (*connect.Response[managementv1.CreateWorkspaceResponse], error) {
	req := creq.Msg
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	if req.GetWorkspaceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "workspace_id is required")
	}
	spec := workspaceflow.Spec{
		Runtime:    req.GetRuntime(),
		PipelineId: req.GetPipelineId(),
	}
	switch src := req.GetSource().(type) {
	case *managementv1.CreateWorkspaceRequest_Git:
		spec.Git = &workspaceflow.GitSource{
			Url:           src.Git.GetUrl(),
			Ref:           src.Git.GetRef(),
			Subdir:        src.Git.GetSubdir(),
			CredentialRef: src.Git.GetCredentialSecret(),
		}
	case *managementv1.CreateWorkspaceRequest_Snapshot:
		location, digest, err := m.storeTree(ctx, b.Namespace, req.GetWorkspaceId(), src.Snapshot.GetSource())
		if err != nil {
			return nil, err
		}
		spec.Snapshot = &workspaceflow.SnapshotSource{Location: location, Digest: digest}
	default:
		return nil, status.Error(codes.InvalidArgument, "a workspace needs a source: git or snapshot")
	}
	if err := spec.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := b.Worker.DeclareWorkspace(ctx, req.GetWorkspaceId(), spec); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "workspace %s: %v", req.GetWorkspaceId(), err)
	}
	_, _, st, err := b.Worker.DescribeWorkspace(ctx, req.GetWorkspaceId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	m.Log.Info("workspace created",
		xlog.String("namespace", b.Namespace),
		xlog.String("workspace", req.GetWorkspaceId()),
		xlog.String("commit", st.GitCommit))
	return connect.NewResponse(&managementv1.CreateWorkspaceResponse{
		WorkspaceId: req.GetWorkspaceId(),
		TreeDigest:  st.TreeDigest,
		GitCommit:   st.GitCommit,
	}), nil
}

// SyncWorkspace re-resolves the source, or adopts a freshly uploaded
// tree — the working tree is mutable, the source spec is not.
func (m *Management) SyncWorkspace(ctx context.Context, creq *connect.Request[managementv1.SyncWorkspaceRequest]) (*connect.Response[managementv1.SyncWorkspaceResponse], error) {
	req := creq.Msg
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, err
	}
	cmd := workspaceflow.SyncCmd{}
	if len(req.GetSource()) > 0 {
		if len(req.GetSource()) > maxSourceBytes {
			return nil, status.Errorf(codes.InvalidArgument, "source must be a tar.gz up to %d bytes", maxSourceBytes)
		}
		location, digest, err := m.storeTree(ctx, b.Namespace, req.GetWorkspaceId(), req.GetSource())
		if err != nil {
			return nil, err
		}
		cmd.Location, cmd.Digest = location, digest
	}
	res, err := b.Worker.SyncWorkspace(ctx, req.GetWorkspaceId(), cmd)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "sync %s: %v", req.GetWorkspaceId(), err)
	}
	return connect.NewResponse(&managementv1.SyncWorkspaceResponse{
		TreeDigest: res.TreeDigest,
		GitCommit:  res.GitCommit,
		Generation: res.Generation,
	}), nil
}

// DownloadSource streams the workspace's current working tree back —
// the source of any revision is recoverable, git or not.
func (m *Management) DownloadSource(ctx context.Context, creq *connect.Request[managementv1.DownloadSourceRequest], stream *connect.ServerStream[managementv1.DownloadSourceChunk]) error {
	b, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return err
	}
	_, _, st, err := b.Worker.DescribeWorkspace(ctx, creq.Msg.GetWorkspaceId())
	if err != nil {
		return status.Error(codes.NotFound, err.Error())
	}
	if st.TreeLocation == "" {
		return status.Error(codes.FailedPrecondition, "workspace has no working tree yet")
	}
	rc, err := m.Blobs.Get(ctx, b.Namespace, st.TreeLocation)
	if err != nil {
		return status.Errorf(codes.Internal, "tree: %v", err)
	}
	defer func() { _ = rc.Close() }()
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
	if _, err := bundleFor(ctx, m.Bundles, auth.RoleAdmin, auth.RoleRun); err != nil {
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
// workspace and returns its location and digest.
func (m *Management) storeTree(ctx context.Context, namespace, workspaceId string, tree []byte) (string, string, error) {
	if len(tree) == 0 {
		return "", "", status.Error(codes.InvalidArgument, "source is empty")
	}
	sum := sha256.Sum256(tree)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	location := fmt.Sprintf("workspaces/%s/%s.tgz", workspaceId, hex.EncodeToString(sum[:])[:16])
	if _, err := m.Blobs.Put(ctx, namespace, location, bytes.NewReader(tree)); err != nil {
		return "", "", status.Errorf(codes.Internal, "store source: %v", err)
	}
	return location, digest, nil
}
