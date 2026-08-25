// Package materialize is the server side of the source-first contour:
// it turns ONE source tree into a runnable pipeline revision — build in
// an ephemeral runtime container, manifest from the binary's own
// describe mode, ko-style OCI assembly pushed to the installation's
// registry. The server process never runs a Go toolchain and never
// executes user code in itself.
package materialize

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/gopherex/xlog"

	"github.com/graphene-ci/graphene/internal/infrastructure/blob"
	"github.com/graphene-ci/pipeline/pkg/selfbuild"
)

// DefaultRuntime builds Go pipelines; mirror.gcr.io serves regions
// docker.io does not.
const DefaultRuntime = "mirror.gcr.io/library/golang:1.26"

// modCacheVolume persists the module cache across materializations.
const modCacheVolume = "graphene-materialize-go-mod"

// Materializer runs source builds on the installation's execution
// backend (the same docker host the managed contour drives).
type Materializer struct {
	Docker   *dockerclient.Client
	Runtime  string
	Registry string // the installation's /v2 door, host:port
	Token    string // a run-scoped token for the registry and blobs
	Insecure bool
	Blobs    blob.Store
	Log      *xlog.Logger
}

// Progress receives one progress line; stage is "runtime", "build",
// "describe" or "publish".
type Progress func(stage, message string)

// labelKey marks materialization containers so orphans of a killed
// request are collectable.
const labelKey = "graphene.materialize"

// Result is one materialized revision.
type Result struct {
	RevisionId       string
	ImageRef         string
	ManifestLocation string
	Manifest         []byte
	SourceDigest     string
	BuildLog         string
}

// Materialize builds one source tree (a tar.gz) into a revision,
// reporting progress as it goes — a build takes minutes and a silent
// request dies on the first idle NAT.
func (m *Materializer) Materialize(ctx context.Context, namespace, pipelineId string, srcTarGz []byte, progress Progress) (Result, error) {
	var res Result
	if progress == nil {
		progress = func(string, string) {}
	}
	m.reapOrphans(ctx)
	sum := sha256.Sum256(srcTarGz)
	res.SourceDigest = "sha256:" + hex.EncodeToString(sum[:])

	runtimeImage := m.Runtime
	if runtimeImage == "" {
		runtimeImage = DefaultRuntime
	}
	if err := m.ensureImage(ctx, runtimeImage, progress); err != nil {
		return res, err
	}
	progress("runtime", "runtime container starting")

	// One ephemeral container per materialization: the source goes in
	// through the docker API (no shared host paths), the binary comes
	// back the same way.
	cont, err := m.Docker.ContainerCreate(ctx, &container.Config{
		Image:      runtimeImage,
		Entrypoint: []string{"sleep", "3600"},
		Env:        []string{"CGO_ENABLED=0", "GOOS=linux", "GOARCH=amd64", "GOFLAGS=-mod=mod"},
		WorkingDir: "/src",
		Labels:     map[string]string{labelKey: pipelineId},
	}, &container.HostConfig{
		Mounts: []mount.Mount{{Type: mount.TypeVolume, Source: modCacheVolume, Target: "/go/pkg/mod"}},
	}, nil, nil, "")
	if err != nil {
		return res, fmt.Errorf("materialize container: %w", err)
	}
	defer func() {
		_ = m.Docker.ContainerRemove(context.WithoutCancel(ctx), cont.ID, container.RemoveOptions{Force: true})
	}()
	if err := m.Docker.ContainerStart(ctx, cont.ID, container.StartOptions{}); err != nil {
		return res, fmt.Errorf("materialize start: %w", err)
	}

	srcTar, err := gunzip(srcTarGz)
	if err != nil {
		return res, fmt.Errorf("source is not a tar.gz: %w", err)
	}
	if err := m.Docker.CopyToContainer(ctx, cont.ID, "/src", bytes.NewReader(srcTar), container.CopyToContainerOptions{}); err != nil {
		return res, fmt.Errorf("copy source: %w", err)
	}

	// The fixed layout: the workspace root IS the entrypoint —
	// `go build .`, one package main, no entrypoint discovery. The
	// download runs first so its progress is visible on its own.
	depLog, err := m.exec(ctx, cont.ID, nil, "go mod download -x 2>&1 | tail -200",
		func(line string) { progress("build", line) })
	if err != nil {
		return res, fmt.Errorf("dependencies failed: %w\n%s", err, tail(depLog, 4000))
	}
	buildLog, err := m.exec(ctx, cont.ID, nil,
		"go build -trimpath -ldflags '-s -w -buildid=' -o /tmp/app . 2>&1",
		func(line string) { progress("build", line) })
	res.BuildLog = depLog + buildLog
	if err != nil {
		return res, fmt.Errorf("build failed: %w\n%s", err, tail(buildLog, 4000))
	}
	progress("describe", "reading the manifest from the binary")

	// The manifest comes from the binary itself — the same recording
	// pass the local CLI used, now in describe mode on the server.
	manifestOut, err := m.exec(ctx, cont.ID, []string{"GRAPHENE_MANIFEST=1"}, "/tmp/app", nil)
	if err != nil {
		return res, fmt.Errorf("describe failed: %w\n%s", err, tail(manifestOut, 4000))
	}
	manifest := []byte(strings.TrimSpace(manifestOut))
	if len(manifest) == 0 || manifest[0] != '{' {
		return res, fmt.Errorf("describe produced no manifest:\n%s", tail(manifestOut, 2000))
	}
	res.Manifest = manifest

	binPath, err := m.copyOut(ctx, cont.ID, "/tmp/app")
	if err != nil {
		return res, err
	}
	defer func() { _ = os.RemoveAll(filepath.Dir(binPath)) }()

	progress("publish", "assembling and pushing the worker image")
	imageRef, _, err := selfbuild.PushBinary(ctx, binPath, selfbuild.Options{
		Registry:   m.Registry,
		Namespace:  namespace,
		PipelineId: pipelineId,
		Token:      m.Token,
		Insecure:   m.Insecure,
		Log: func(format string, args ...any) {
			line := fmt.Sprintf(format, args...)
			m.Log.Info("materialize: " + line)
			progress("publish", line)
		},
	})
	if err != nil {
		return res, fmt.Errorf("publish image: %w", err)
	}
	res.ImageRef = imageRef
	// The revision id is the image's content tag: same source — same
	// binary — same revision.
	res.RevisionId = imageRef[strings.LastIndex(imageRef, ":")+1:]

	res.ManifestLocation = fmt.Sprintf("revisions/%s/%s/manifest.json", pipelineId, res.RevisionId)
	if _, err := m.Blobs.Put(ctx, namespace, res.ManifestLocation, bytes.NewReader(manifest)); err != nil {
		return res, fmt.Errorf("store manifest: %w", err)
	}
	return res, nil
}

// reapOrphans removes materialization containers left by killed
// requests — a client that hangs up must not leak a runtime container.
func (m *Materializer) reapOrphans(ctx context.Context) {
	list, err := m.Docker.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: filters.NewArgs(filters.Arg("label", labelKey)),
	})
	if err != nil {
		return
	}
	for _, c := range list {
		if time.Since(time.Unix(c.Created, 0)) < time.Hour {
			continue
		}
		_ = m.Docker.ContainerRemove(ctx, c.ID, container.RemoveOptions{Force: true})
	}
}

// ensureImage pulls the runtime image on first use.
func (m *Materializer) ensureImage(ctx context.Context, ref string, progress Progress) error {
	if _, err := m.Docker.ImageInspect(ctx, ref); err == nil {
		return nil
	}
	progress("runtime", "pulling "+ref)
	m.Log.Info("materialize: pulling runtime image", xlog.String("image", ref))
	rc, err := m.Docker.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("pull %s: %w", ref, err)
	}
	defer func() { _ = rc.Close() }()
	_, err = io.Copy(io.Discard, rc)
	return err
}

// exec runs one command in the container, streaming each output line
// to onLine as it appears, and returns the collected output; a
// non-zero exit is an error.
func (m *Materializer) exec(ctx context.Context, containerId string, env []string, cmd string, onLine func(string)) (string, error) {
	execId, err := m.Docker.ContainerExecCreate(ctx, containerId, container.ExecOptions{
		Cmd:          []string{"sh", "-c", cmd},
		Env:          env,
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		return "", err
	}
	att, err := m.Docker.ContainerExecAttach(ctx, execId.ID, container.ExecStartOptions{})
	if err != nil {
		return "", err
	}
	defer att.Close()
	var out bytes.Buffer
	var sink io.Writer = &out
	if onLine != nil {
		sink = io.MultiWriter(&out, &lineWriter{emit: onLine})
	}
	if _, err := stdcopy.StdCopy(sink, sink, att.Reader); err != nil {
		return out.String(), err
	}
	insp, err := m.Docker.ContainerExecInspect(ctx, execId.ID)
	if err != nil {
		return out.String(), err
	}
	if insp.ExitCode != 0 {
		return out.String(), fmt.Errorf("exit code %d", insp.ExitCode)
	}
	return out.String(), nil
}

// copyOut fetches one file from the container into a temp dir.
func (m *Materializer) copyOut(ctx context.Context, containerId, path string) (string, error) {
	rc, _, err := m.Docker.CopyFromContainer(ctx, containerId, path)
	if err != nil {
		return "", err
	}
	defer func() { _ = rc.Close() }()
	dir, err := os.MkdirTemp("", "graphene-materialize-")
	if err != nil {
		return "", err
	}
	tr := tar.NewReader(rc)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		out := filepath.Join(dir, filepath.Base(hdr.Name))
		f, err := os.OpenFile(out, os.O_CREATE|os.O_WRONLY, 0o755) //nolint:gosec // our own temp output
		if err != nil {
			return "", err
		}
		if _, err := io.Copy(f, tr); err != nil { //nolint:gosec // bounded by the built binary's size
			_ = f.Close()
			return "", err
		}
		_ = f.Close()
		return out, nil
	}
	return "", fmt.Errorf("%s not found in container", path)
}

// lineWriter turns a byte stream into progress lines.
type lineWriter struct {
	buf  bytes.Buffer
	emit func(string)
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.buf.Write(p)
	for {
		line, err := w.buf.ReadString('\n')
		if err != nil {
			w.buf.WriteString(line) // partial line waits for the rest
			break
		}
		if trimmed := strings.TrimRight(line, "\r\n"); trimmed != "" {
			w.emit(trimmed)
		}
	}
	return len(p), nil
}

func gunzip(b []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer func() { _ = zr.Close() }()
	out, err := io.ReadAll(zr) //nolint:gosec // dev PoC; size-cap the upload at the API instead
	if err != nil {
		return nil, err
	}
	return out, nil
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "…" + s[len(s)-n:]
}

// Timestamp renders the revision's creation time.
func Timestamp() string { return time.Now().UTC().Format(time.RFC3339) }
