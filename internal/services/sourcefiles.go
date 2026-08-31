package services

// The BYTES of a source. A Git checkout is readable file by file and
// writable not at all: it follows a commit, and editing it would fork
// the project silently.

import (
	"context"
	"sort"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/authz"
	"github.com/graphene-ci/graphene/internal/nsbundle"
	"github.com/graphene-ci/graphene/internal/sourceflow"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// ListFiles lists a source's tree.
func (m *Management) ListFiles(ctx context.Context, creq *connect.Request[managementv1.ListFilesRequest]) (*connect.Response[managementv1.ListFilesResponse], error) {
	b, err := m.allow(ctx, authz.VerbGet, authz.KindOf(creq.Msg.GetSource()))
	if err != nil {
		return nil, err
	}
	tree, digest, err := m.sourceTree(ctx, b, creq.Msg.GetSource())
	if err != nil {
		return nil, err
	}
	files := make([]*managementv1.ListFilesResponse_File, 0, len(tree))
	for path, content := range tree {
		files = append(files, &managementv1.ListFilesResponse_File{Path: path, Size: int64(len(content))})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].GetPath() < files[j].GetPath() })
	return connect.NewResponse(&managementv1.ListFilesResponse{Files: files, TreeDigest: digest}), nil
}

// ReadFile returns one file.
func (m *Management) ReadFile(ctx context.Context, creq *connect.Request[managementv1.ReadFileRequest]) (*connect.Response[managementv1.ReadFileResponse], error) {
	b, err := m.allow(ctx, authz.VerbGet, authz.KindOf(creq.Msg.GetSource()))
	if err != nil {
		return nil, err
	}
	clean, err := sourceflow.CleanPath(creq.Msg.GetPath())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	tree, _, err := m.sourceTree(ctx, b, creq.Msg.GetSource())
	if err != nil {
		return nil, err
	}
	content, ok := tree[clean]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no file %q in the source", clean)
	}
	return connect.NewResponse(&managementv1.ReadFileResponse{Content: content}), nil
}

// sourceTree loads a whole source tree — a checkout keeps its bytes in
// one archive.
func (m *Management) sourceTree(ctx context.Context, b *nsbundle.Bundle, ref string) (map[string][]byte, string, error) {
	files, err := b.Worker.SourceFiles(ctx, ref)
	if err != nil {
		return nil, "", status.Error(codes.NotFound, err.Error())
	}
	digest := ""
	if id, ok := strings.CutPrefix(ref, string(sourceflow.GitKind)+"/"); ok {
		if _, _, st, err := b.Worker.DescribeGitSource(ctx, id); err == nil {
			digest = st.TreeDigest
		}
	}
	return files, digest, nil
}
