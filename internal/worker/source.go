package worker

// The server side of the two source kinds: fetching a Git checkout,
// laying a managed tree out file by file, and reading either one back
// as the archive a build wants.

import (
	"context"
	"fmt"
	"strings"

	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/activity"

	"github.com/graphene-ci/graphene/internal/materialize"
	"github.com/graphene-ci/graphene/internal/sourceflow"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// maxSourceFileBytes bounds one file of a source tree.
const maxSourceFileBytes = 8 << 20

// fetchGitSource resolves a Git source into a checkout.
func (s *Worker) fetchGitSource(ctx context.Context, req sourceflow.FetchReq) (sourceflow.FetchRes, error) {
	var res sourceflow.FetchRes
	if s.deps.Materializer == nil {
		return res, fmt.Errorf("a git checkout needs an execution backend on this installation")
	}
	credential := ""
	if name := req.Spec.CredentialRef; name != "" {
		v, err := s.deps.Secrets.Get(id.SecretId(name))
		if err != nil {
			return res, fmt.Errorf("git credential %q: %w", name, err)
		}
		credential = v
	}
	out, err := s.deps.Materializer.FetchGit(ctx, materialize.GitRequest{
		Url:        req.Spec.Url,
		Ref:        req.Spec.Ref,
		Subdir:     req.Spec.Subdir,
		Credential: credential,
		Location:   fmt.Sprintf("sources/%s/checkout.tgz", req.SourceId),
		Namespace:  s.deps.Namespace,
		Runtime:    req.Spec.Runtime,
	}, func(stage, message string) { activity.RecordHeartbeat(ctx, stage+": "+message) })
	if err != nil {
		return res, err
	}
	return sourceflow.FetchRes{
		TreeLocation: out.TreeLocation, TreeDigest: out.TreeDigest, Commit: out.Commit,
	}, nil
}

// adoptManagedSource lays the initial tree out file by file. A source
// that starts from nothing is legitimate — an empty project grows by
// writing files.
func (s *Worker) adoptManagedSource(ctx context.Context, req sourceflow.AdoptReq) (sourceflow.AdoptRes, error) {
	files := map[string][]byte{}
	switch {
	case req.Spec.From != "":
		raw, err := s.SourceArchive(ctx, req.Spec.From)
		if err != nil {
			return sourceflow.AdoptRes{}, fmt.Errorf("copy from %s: %w", req.Spec.From, err)
		}
		if files, err = sourceflow.UnpackTar(raw, maxSourceFileBytes); err != nil {
			return sourceflow.AdoptRes{}, err
		}
	case req.Spec.Upload != "":
		raw, err := s.blobBytes(ctx, req.Spec.Upload)
		if err != nil {
			return sourceflow.AdoptRes{}, err
		}
		if files, err = sourceflow.UnpackTar(raw, maxSourceFileBytes); err != nil {
			return sourceflow.AdoptRes{}, err
		}
	}
	return s.storeTreeFiles(ctx, req.SourceId, files)
}

// storeTreeFiles writes every file as its own blob and the index over
// them. Content-addressed: a file whose content is already stored is
// not written again.
func (s *Worker) storeTreeFiles(ctx context.Context, sourceId string, files map[string][]byte) (sourceflow.AdoptRes, error) {
	if s.deps.Blobs == nil {
		return sourceflow.AdoptRes{}, fmt.Errorf("this installation has no blob store")
	}
	ix := sourceflow.Index{}
	for path, content := range files {
		digest := sourceflow.FileDigest(content)
		blob := sourceflow.BlobPath(sourceId, digest)
		if _, err := s.deps.Blobs.Put(ctx, s.deps.Namespace, blob, strings.NewReader(string(content))); err != nil {
			return sourceflow.AdoptRes{}, fmt.Errorf("store %s: %w", path, err)
		}
		ix[path] = sourceflow.Entry{Blob: blob, Size: int64(len(content)), Digest: digest}
		activity.RecordHeartbeat(ctx, "stored "+path)
	}
	return s.storeIndex(ctx, sourceId, ix)
}

// storeIndex writes one version of the index and reports the tree.
func (s *Worker) storeIndex(ctx context.Context, sourceId string, ix sourceflow.Index) (sourceflow.AdoptRes, error) {
	digest, raw, err := ix.Digest()
	if err != nil {
		return sourceflow.AdoptRes{}, err
	}
	location := sourceflow.IndexPath(sourceId, digest)
	if _, err := s.deps.Blobs.Put(ctx, s.deps.Namespace, location, strings.NewReader(string(raw))); err != nil {
		return sourceflow.AdoptRes{}, fmt.Errorf("store index: %w", err)
	}
	return sourceflow.AdoptRes{IndexLocation: location, TreeDigest: digest, Files: len(ix)}, nil
}

// --- reading a source, whatever kind it is ---

// SourceFiles reads a source's whole tree into memory: a Git checkout
// is unpacked from its archive, a managed tree is gathered from its
// blobs.
func (s *Worker) SourceFiles(ctx context.Context, sourceRef string) (map[string][]byte, error) {
	kind, id, ok := strings.Cut(sourceRef, "/")
	if !ok {
		return nil, fmt.Errorf("source %q: want kind/id", sourceRef)
	}
	switch entity.KindName(kind) {
	case sourceflow.GitKind:
		_, _, st, err := s.DescribeGitSource(ctx, id)
		if err != nil {
			return nil, err
		}
		raw, err := s.blobBytes(ctx, st.TreeLocation)
		if err != nil {
			return nil, err
		}
		return sourceflow.UnpackTar(raw, maxSourceFileBytes)
	case sourceflow.ManagedKind:
		ix, err := s.SourceIndex(ctx, id)
		if err != nil {
			return nil, err
		}
		out := make(map[string][]byte, len(ix))
		for path, e := range ix {
			content, err := s.blobBytes(ctx, e.Blob)
			if err != nil {
				return nil, fmt.Errorf("file %s: %w", path, err)
			}
			out[path] = content
		}
		return out, nil
	}
	return nil, fmt.Errorf("%q is not a source", sourceRef)
}

// SourceArchive renders a source as the tar.gz a build receives.
func (s *Worker) SourceArchive(ctx context.Context, sourceRef string) ([]byte, error) {
	kind, id, ok := strings.Cut(sourceRef, "/")
	// A Git checkout IS an archive already — no unpack-repack round.
	if ok && entity.KindName(kind) == sourceflow.GitKind {
		_, _, st, err := s.DescribeGitSource(ctx, id)
		if err != nil {
			return nil, err
		}
		return s.blobBytes(ctx, st.TreeLocation)
	}
	files, err := s.SourceFiles(ctx, sourceRef)
	if err != nil {
		return nil, err
	}
	return sourceflow.PackTar(files)
}

// SourceIndex reads a managed source's index.
func (s *Worker) SourceIndex(ctx context.Context, sourceId string) (sourceflow.Index, error) {
	_, _, st, err := s.DescribeManagedSource(ctx, sourceId)
	if err != nil {
		return nil, err
	}
	if st.IndexLocation == "" {
		return sourceflow.Index{}, nil
	}
	raw, err := s.blobBytes(ctx, st.IndexLocation)
	if err != nil {
		return nil, err
	}
	return sourceflow.ParseIndex(raw)
}

// ReadSourceFile reads ONE file — one blob, not the whole tree.
func (s *Worker) ReadSourceFile(ctx context.Context, sourceId, path string) ([]byte, error) {
	ix, err := s.SourceIndex(ctx, sourceId)
	if err != nil {
		return nil, err
	}
	e, ok := ix[path]
	if !ok {
		return nil, fmt.Errorf("no file %q in the source", path)
	}
	return s.blobBytes(ctx, e.Blob)
}

// WriteSourceFile stores one file and the new index, then records the
// change on the source's own record. Nothing else in the tree is read
// or rewritten.
func (s *Worker) WriteSourceFile(ctx context.Context, sourceId, path string, content []byte) (sourceflow.ManagedRes, error) {
	ix, err := s.SourceIndex(ctx, sourceId)
	if err != nil {
		return sourceflow.ManagedRes{}, err
	}
	digest := sourceflow.FileDigest(content)
	blob := sourceflow.BlobPath(sourceId, digest)
	if _, err := s.deps.Blobs.Put(ctx, s.deps.Namespace, blob, strings.NewReader(string(content))); err != nil {
		return sourceflow.ManagedRes{}, fmt.Errorf("store %s: %w", path, err)
	}
	ix[path] = sourceflow.Entry{Blob: blob, Size: int64(len(content)), Digest: digest}
	return s.commitIndex(ctx, sourceId, ix)
}

// DeleteSourceFile drops one file from the index. The blob stays: it
// is content-addressed and may be shared with an older index.
func (s *Worker) DeleteSourceFile(ctx context.Context, sourceId, path string) (sourceflow.ManagedRes, error) {
	ix, err := s.SourceIndex(ctx, sourceId)
	if err != nil {
		return sourceflow.ManagedRes{}, err
	}
	if _, ok := ix[path]; !ok {
		return sourceflow.ManagedRes{}, fmt.Errorf("no file %q in the source", path)
	}
	delete(ix, path)
	return s.commitIndex(ctx, sourceId, ix)
}

// commitIndex stores the index and lands the change on the record.
func (s *Worker) commitIndex(ctx context.Context, sourceId string, ix sourceflow.Index) (sourceflow.ManagedRes, error) {
	res, err := s.storeIndex(ctx, sourceId, ix)
	if err != nil {
		return sourceflow.ManagedRes{}, err
	}
	sources := entclient.Bind(s.managedDef, s.deps.Client, wire.ServerQueue)
	return entclient.Exec(ctx, sources, entity.ResourceID(sourceId), sourceflow.WriteCmd{
		IndexLocation: res.IndexLocation, TreeDigest: res.TreeDigest, Files: res.Files,
	})
}

// --- record readers ---

// DescribeGitSource reads one Git source record.
func (s *Worker) DescribeGitSource(ctx context.Context, sourceId string) (entity.Phase, sourceflow.GitSpec, sourceflow.GitState, error) {
	sources := entclient.Bind(s.gitSourceDef, s.deps.Client, wire.ServerQueue)
	out, err := sources.Describe(ctx, entity.ResourceID(sourceId))
	if err != nil {
		return "", sourceflow.GitSpec{}, sourceflow.GitState{}, err
	}
	return out.Phase, out.Spec, out.State, nil
}

// DescribeManagedSource reads one managed source record.
func (s *Worker) DescribeManagedSource(ctx context.Context, sourceId string) (entity.Phase, sourceflow.ManagedSpec, sourceflow.ManagedState, error) {
	sources := entclient.Bind(s.managedDef, s.deps.Client, wire.ServerQueue)
	out, err := sources.Describe(ctx, entity.ResourceID(sourceId))
	if err != nil {
		return "", sourceflow.ManagedSpec{}, sourceflow.ManagedState{}, err
	}
	return out.Phase, out.Spec, out.State, nil
}

// SyncGitSource fetches a Git source's ref again.
func (s *Worker) SyncGitSource(ctx context.Context, sourceId string) (sourceflow.GitRes, error) {
	sources := entclient.Bind(s.gitSourceDef, s.deps.Client, wire.ServerQueue)
	return entclient.Exec(ctx, sources, entity.ResourceID(sourceId), sourceflow.SyncCmd{})
}

// SourcesOf lists the sources under one pipeline, by kind.
func (s *Worker) SourcesOf(ctx context.Context, pipelineId string) ([]string, error) {
	var out []string
	for _, kind := range []entity.KindName{sourceflow.GitKind, sourceflow.ManagedKind} {
		ids, err := s.listKind(ctx, string(kind))
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			owner := ""
			switch kind {
			case sourceflow.GitKind:
				if _, spec, _, err := s.DescribeGitSource(ctx, id); err == nil {
					owner = spec.PipelineId
				}
			case sourceflow.ManagedKind:
				if _, spec, _, err := s.DescribeManagedSource(ctx, id); err == nil {
					owner = spec.PipelineId
				}
			}
			if owner == pipelineId {
				out = append(out, string(kind)+"/"+id)
			}
		}
	}
	return out, nil
}

// SourceRuntime answers which toolchain a source is built with.
func (s *Worker) SourceRuntime(ctx context.Context, sourceRef string) (string, error) {
	kind, id, ok := strings.Cut(sourceRef, "/")
	if !ok {
		return "", fmt.Errorf("source %q: want kind/id", sourceRef)
	}
	switch entity.KindName(kind) {
	case sourceflow.GitKind:
		_, spec, _, err := s.DescribeGitSource(ctx, id)
		return spec.Runtime, err
	case sourceflow.ManagedKind:
		_, spec, _, err := s.DescribeManagedSource(ctx, id)
		return spec.Runtime, err
	}
	return "", fmt.Errorf("%q is not a source", sourceRef)
}
