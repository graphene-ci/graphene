package services

// The workspace's working tree is EDITABLE: Studio reads and writes
// files straight into it. A tree is one tar.gz in the blob store, so a
// write is read-modify-write of that archive — atomic by construction,
// durable the moment it returns, and the record's generation counts
// every change.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/authz"
	"github.com/graphene-ci/graphene/internal/workspaceflow"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// maxFileBytes bounds one edited file.
const maxFileBytes = 8 << 20

// ListFiles lists the working tree.
func (m *Management) ListFiles(ctx context.Context, creq *connect.Request[managementv1.ListFilesRequest]) (*connect.Response[managementv1.ListFilesResponse], error) {
	b, err := m.allow(ctx, authz.VerbGet, authz.KindWorkspace)
	if err != nil {
		return nil, err
	}
	tree, st, err := m.workspaceTree(ctx, b.Namespace, creq.Msg.GetWorkspaceId())
	if err != nil {
		return nil, err
	}
	files := make([]*managementv1.ListFilesResponse_File, 0, len(tree))
	for p, content := range tree {
		files = append(files, &managementv1.ListFilesResponse_File{Path: p, Size: int64(len(content))})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].GetPath() < files[j].GetPath() })
	return connect.NewResponse(&managementv1.ListFilesResponse{Files: files, TreeDigest: st.TreeDigest}), nil
}

// ReadFile returns one file of the working tree.
func (m *Management) ReadFile(ctx context.Context, creq *connect.Request[managementv1.ReadFileRequest]) (*connect.Response[managementv1.ReadFileResponse], error) {
	b, err := m.allow(ctx, authz.VerbGet, authz.KindWorkspace)
	if err != nil {
		return nil, err
	}
	clean, err := cleanPath(creq.Msg.GetPath())
	if err != nil {
		return nil, err
	}
	tree, _, err := m.workspaceTree(ctx, b.Namespace, creq.Msg.GetWorkspaceId())
	if err != nil {
		return nil, err
	}
	content, ok := tree[clean]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no file %q in the workspace", clean)
	}
	return connect.NewResponse(&managementv1.ReadFileResponse{Content: content}), nil
}

// WriteFile writes one file into the working tree — durable when this
// call returns, no checkpoint needed.
func (m *Management) WriteFile(ctx context.Context, creq *connect.Request[managementv1.WriteFileRequest]) (*connect.Response[managementv1.WriteFileResponse], error) {
	req := creq.Msg
	if len(req.GetContent()) > maxFileBytes {
		return nil, status.Errorf(codes.InvalidArgument, "a file may be up to %d bytes", maxFileBytes)
	}
	return m.editTree(ctx, req.GetWorkspaceId(), req.GetPath(), func(tree map[string][]byte, clean string) error {
		tree[clean] = req.GetContent()
		return nil
	})
}

// DeleteFile removes one file from the working tree.
func (m *Management) DeleteFile(ctx context.Context, creq *connect.Request[managementv1.DeleteFileRequest]) (*connect.Response[managementv1.WriteFileResponse], error) {
	req := creq.Msg
	return m.editTree(ctx, req.GetWorkspaceId(), req.GetPath(), func(tree map[string][]byte, clean string) error {
		if _, ok := tree[clean]; !ok {
			return status.Errorf(codes.NotFound, "no file %q in the workspace", clean)
		}
		delete(tree, clean)
		return nil
	})
}

// editTree applies one change to the working tree and stores the
// result as the workspace's new tree.
func (m *Management) editTree(ctx context.Context, workspaceId, rawPath string, edit func(map[string][]byte, string) error) (*connect.Response[managementv1.WriteFileResponse], error) {
	b, err := m.allow(ctx, authz.VerbUpdate, authz.KindWorkspace)
	if err != nil {
		return nil, err
	}
	clean, err := cleanPath(rawPath)
	if err != nil {
		return nil, err
	}
	tree, _, err := m.workspaceTree(ctx, b.Namespace, workspaceId)
	if err != nil {
		return nil, err
	}
	if err := edit(tree, clean); err != nil {
		return nil, err
	}
	packed, err := packTree(tree)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "pack tree: %v", err)
	}
	sum := sha256.Sum256(packed)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	location := fmt.Sprintf("workspaces/%s/%s.tgz", workspaceId, hex.EncodeToString(sum[:])[:16])
	if _, err := m.Blobs.Put(ctx, b.Namespace, location, bytes.NewReader(packed)); err != nil {
		return nil, status.Errorf(codes.Internal, "store tree: %v", err)
	}
	// The record owns the tree: the edit lands through its own command,
	// so the generation and the digest stay in one history.
	res, err := b.Worker.SyncWorkspace(ctx, workspaceId, workspaceflow.SyncCmd{Location: location, Digest: digest})
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "workspace %s: %v", workspaceId, err)
	}
	return connect.NewResponse(&managementv1.WriteFileResponse{
		TreeDigest: res.TreeDigest,
		Generation: res.Generation,
	}), nil
}

// workspaceTree loads the working tree into memory.
func (m *Management) workspaceTree(ctx context.Context, namespace, workspaceId string) (map[string][]byte, workspaceflow.State, error) {
	b, err := m.Bundles.Get(namespace)
	if err != nil {
		return nil, workspaceflow.State{}, status.Error(codes.Internal, err.Error())
	}
	_, _, st, err := b.Worker.DescribeWorkspace(ctx, workspaceId)
	if err != nil {
		return nil, workspaceflow.State{}, status.Errorf(codes.NotFound, "workspace %s: %v", workspaceId, err)
	}
	if st.TreeLocation == "" {
		return nil, st, status.Error(codes.FailedPrecondition, "workspace has no working tree yet")
	}
	rc, err := m.Blobs.Get(ctx, namespace, st.TreeLocation)
	if err != nil {
		return nil, st, status.Errorf(codes.Internal, "tree: %v", err)
	}
	defer func() { _ = rc.Close() }()
	tree, err := unpackTree(rc)
	if err != nil {
		return nil, st, status.Errorf(codes.Internal, "tree: %v", err)
	}
	return tree, st, nil
}

// unpackTree reads a tar.gz into path -> content.
func unpackTree(r io.Reader) (map[string][]byte, error) {
	zr, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer func() { _ = zr.Close() }()
	out := map[string][]byte{}
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		clean, err := cleanPath(hdr.Name)
		if err != nil {
			continue // a tree from elsewhere may hold oddities; skip them
		}
		content, err := io.ReadAll(io.LimitReader(tr, maxFileBytes+1))
		if err != nil {
			return nil, err
		}
		out[clean] = content
	}
}

// packTree renders path -> content back into a tar.gz, deterministic
// in order so the same tree keeps the same digest.
func packTree(tree map[string][]byte) ([]byte, error) {
	paths := make([]string, 0, len(tree))
	for p := range tree {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	for _, p := range paths {
		if err := tw.WriteHeader(&tar.Header{Name: p, Mode: 0o644, Size: int64(len(tree[p]))}); err != nil {
			return nil, err
		}
		if _, err := tw.Write(tree[p]); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// cleanPath keeps an edit inside the workspace: no absolute paths, no
// climbing out, no empty names.
func cleanPath(p string) (string, error) {
	clean := path.Clean(strings.TrimPrefix(strings.TrimSpace(p), "./"))
	switch {
	case clean == "" || clean == "." || clean == "/":
		return "", status.Error(codes.InvalidArgument, "path is required")
	case path.IsAbs(clean):
		return "", status.Errorf(codes.InvalidArgument, "path %q must be relative to the workspace", p)
	case clean == ".." || strings.HasPrefix(clean, "../"):
		return "", status.Errorf(codes.InvalidArgument, "path %q escapes the workspace", p)
	}
	return clean, nil
}
