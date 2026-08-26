package services

// The BYTES of a source. A managed tree is kept file by file, so a
// write stores one blob and one index — never a read-modify-write of
// the whole project. A Git checkout is readable the same way and
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

// maxFileBytes bounds one edited file.
const maxFileBytes = 8 << 20

// ListFiles lists a source's tree.
func (m *Management) ListFiles(ctx context.Context, creq *connect.Request[managementv1.ListFilesRequest]) (*connect.Response[managementv1.ListFilesResponse], error) {
	b, err := m.allow(ctx, authz.VerbGet, authz.KindOf(creq.Msg.GetSource()))
	if err != nil {
		return nil, err
	}
	ref := creq.Msg.GetSource()
	// A managed tree answers from its INDEX — no archive is read.
	if id, ok := managedId(ref); ok {
		ix, err := b.Worker.SourceIndex(ctx, id)
		if err != nil {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		_, _, st, _ := b.Worker.DescribeManagedSource(ctx, id)
		files := make([]*managementv1.ListFilesResponse_File, 0, len(ix))
		for path, e := range ix {
			files = append(files, &managementv1.ListFilesResponse_File{Path: path, Size: e.Size})
		}
		sort.Slice(files, func(i, j int) bool { return files[i].GetPath() < files[j].GetPath() })
		return connect.NewResponse(&managementv1.ListFilesResponse{Files: files, TreeDigest: st.TreeDigest}), nil
	}
	tree, digest, err := m.sourceTree(ctx, b, ref)
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
	// One blob, not the whole tree.
	if id, ok := managedId(creq.Msg.GetSource()); ok {
		content, err := b.Worker.ReadSourceFile(ctx, id, clean)
		if err != nil {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return connect.NewResponse(&managementv1.ReadFileResponse{Content: content}), nil
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

// WriteFile writes one file into a managed tree.
func (m *Management) WriteFile(ctx context.Context, creq *connect.Request[managementv1.WriteFileRequest]) (*connect.Response[managementv1.WriteFileResponse], error) {
	if len(creq.Msg.GetContent()) > maxFileBytes {
		return nil, status.Errorf(codes.InvalidArgument, "a file is at most %d bytes", maxFileBytes)
	}
	return m.editFile(ctx, creq.Msg.GetSource(), creq.Msg.GetPath(),
		func(b *nsbundle.Bundle, id, path string) (sourceflow.ManagedRes, error) {
			return b.Worker.WriteSourceFile(ctx, id, path, creq.Msg.GetContent())
		})
}

// DeleteFile drops one file from a managed tree.
func (m *Management) DeleteFile(ctx context.Context, creq *connect.Request[managementv1.DeleteFileRequest]) (*connect.Response[managementv1.WriteFileResponse], error) {
	return m.editFile(ctx, creq.Msg.GetSource(), creq.Msg.GetPath(),
		func(b *nsbundle.Bundle, id, path string) (sourceflow.ManagedRes, error) {
			return b.Worker.DeleteSourceFile(ctx, id, path)
		})
}

// editFile is the one path a managed tree changes by.
func (m *Management) editFile(ctx context.Context, ref, rawPath string,
	edit func(*nsbundle.Bundle, string, string) (sourceflow.ManagedRes, error),
) (*connect.Response[managementv1.WriteFileResponse], error) {
	b, err := m.allow(ctx, authz.VerbUpdate, authz.KindOf(ref))
	if err != nil {
		return nil, err
	}
	clean, err := sourceflow.CleanPath(rawPath)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	id, ok := managedId(ref)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition,
			"%s is not editable: a checkout follows its ref — copy it into a managed source to edit (`graphenectl apply managedsource <new> --spec '{\"pipelineId\":\"…\",\"from\":\"%s\"}'`)",
			ref, ref)
	}
	res, err := edit(b, id, clean)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return connect.NewResponse(&managementv1.WriteFileResponse{
		TreeDigest: res.TreeDigest,
		Generation: res.Generation,
	}), nil
}

// sourceTree loads a whole source tree — what a non-managed source
// needs, since its bytes live in one archive.
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

// managedId reports the id of a managed source reference.
func managedId(ref string) (string, bool) {
	return strings.CutPrefix(ref, string(sourceflow.ManagedKind)+"/")
}
