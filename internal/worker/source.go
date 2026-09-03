package worker

// The server side of the source kind: fetching a Git checkout and
// reading it back as the archive a build wants.

import (
	"context"
	"fmt"
	"strings"

	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/activity"

	"github.com/graphene-ci/graphene/internal/materialize"
	"github.com/graphene-ci/graphene/internal/pipelineflow"
	"github.com/graphene-ci/graphene/internal/revisionflow"
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
		URL:        req.Spec.URL,
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

// --- reading a source ---

// SourceFiles reads a source's whole tree into memory, unpacked from
// its checkout archive.
func (s *Worker) SourceFiles(ctx context.Context, sourceRef string) (map[string][]byte, error) {
	kind, id, ok := strings.Cut(sourceRef, "/")
	if !ok {
		return nil, fmt.Errorf("source %q: want kind/id", sourceRef)
	}
	if entity.KindName(kind) != sourceflow.GitKind {
		return nil, fmt.Errorf("%q is not a source", sourceRef)
	}
	_, _, st, err := s.DescribeGitSource(ctx, id)
	if err != nil {
		return nil, err
	}
	raw, err := s.blobBytes(ctx, st.TreeLocation)
	if err != nil {
		return nil, err
	}
	return sourceflow.UnpackTar(raw, maxSourceFileBytes)
}

// SourceArchive renders a source as the tar.gz a build receives. A Git
// checkout IS an archive already — no unpack-repack round.
func (s *Worker) SourceArchive(ctx context.Context, sourceRef string) ([]byte, error) {
	kind, id, ok := strings.Cut(sourceRef, "/")
	if !ok || entity.KindName(kind) != sourceflow.GitKind {
		return nil, fmt.Errorf("%q is not a source", sourceRef)
	}
	_, _, st, err := s.DescribeGitSource(ctx, id)
	if err != nil {
		return nil, err
	}
	return s.blobBytes(ctx, st.TreeLocation)
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

// SyncGitSource fetches a Git source's ref again.
func (s *Worker) SyncGitSource(ctx context.Context, sourceId string) (sourceflow.GitRes, error) {
	sources := entclient.Bind(s.gitSourceDef, s.deps.Client, wire.ServerQueue)
	return entclient.Exec(ctx, sources, entity.ResourceID(sourceId), sourceflow.SyncCmd{})
}

// SourcesOf lists the sources under one pipeline.
func (s *Worker) SourcesOf(ctx context.Context, pipelineId string) ([]string, error) {
	var out []string
	ids, err := s.listKind(ctx, string(sourceflow.GitKind))
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if _, spec, _, err := s.DescribeGitSource(ctx, id); err == nil && spec.PipelineId == pipelineId {
			out = append(out, string(sourceflow.GitKind)+"/"+id)
		}
	}
	return out, nil
}

// SourceRuntime answers which toolchain a source is built with.
func (s *Worker) SourceRuntime(ctx context.Context, sourceRef string) (string, error) {
	kind, id, ok := strings.Cut(sourceRef, "/")
	if !ok || entity.KindName(kind) != sourceflow.GitKind {
		return "", fmt.Errorf("%q is not a source", sourceRef)
	}
	_, spec, _, err := s.DescribeGitSource(ctx, id)
	return spec.Runtime, err
}

// sweepBlobs erases everything under one prefix — the bytes of a
// record that has just been deleted. Absence is success: the sweep is
// a finalizer and must be safe to retry.
func (s *Worker) sweepBlobs(ctx context.Context, prefix string) error {
	if s.deps.Blobs == nil || prefix == "" {
		return nil
	}
	locations, err := s.deps.Blobs.List(ctx, s.deps.Namespace, prefix)
	if err != nil {
		return fmt.Errorf("list %s: %w", prefix, err)
	}
	for _, loc := range locations {
		if err := s.deps.Blobs.Delete(ctx, s.deps.Namespace, loc); err != nil {
			return fmt.Errorf("delete %s: %w", loc, err)
		}
		activity.RecordHeartbeat(ctx, "swept "+loc)
	}
	return nil
}

// sweepSource erases a deleted source's blobs.
func (s *Worker) sweepSource(ctx context.Context, req sourceflow.SweepReq) error {
	return s.sweepBlobs(ctx, req.Prefix)
}

// sweepRevision erases a deleted revision's blobs.
func (s *Worker) sweepRevision(ctx context.Context, req revisionflow.SweepReq) error {
	return s.sweepBlobs(ctx, req.Prefix)
}

// sweepPipeline erases what belongs to a deleted pipeline and to
// nothing under it: the upload area its sources started from.
func (s *Worker) sweepPipeline(ctx context.Context, req pipelineflow.SweepReq) error {
	return s.sweepBlobs(ctx, req.Prefix)
}
