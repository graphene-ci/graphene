package materialize

// Source resolution: a workspace's source becomes a working tree.
// Graphene does not implement Git — it runs the ordinary tool in an
// ephemeral container and takes the checkout as a tar.gz.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/gopherex/xlog"
)

// DefaultGitRuntime carries git; mirror.gcr.io serves regions
// docker.io does not.
const DefaultGitRuntime = "mirror.gcr.io/library/alpine/git:latest"

// GitRequest is one clone.
type GitRequest struct {
	Url string
	Ref string
	// Subdir narrows the tree to a monorepo subdirectory.
	Subdir string
	// Credential is the resolved secret VALUE (token or key); it never
	// reaches the tree, the log or the record.
	Credential string
	// Location is where the resulting tar.gz goes in the blob store.
	Location  string
	Namespace string
}

// GitResult is the resolved checkout.
type GitResult struct {
	TreeLocation string
	TreeDigest   string
	Commit       string
}

// FetchGit clones one repository into the blob store as a tar.gz. The
// credential lives only in the container's env for the duration of the
// clone: it is not written into the repository config, the tree, or
// any log line.
func (m *Materializer) FetchGit(ctx context.Context, req GitRequest, progress Progress) (GitResult, error) {
	var res GitResult
	if progress == nil {
		progress = func(string, string) {}
	}
	runtimeImage := m.GitRuntime
	if runtimeImage == "" {
		runtimeImage = DefaultGitRuntime
	}
	if err := m.ensureImage(ctx, runtimeImage, progress); err != nil {
		return res, err
	}
	cloneUrl, err := authenticatedUrl(req.Url, req.Credential)
	if err != nil {
		return res, err
	}

	cont, err := m.Docker.ContainerCreate(ctx, &container.Config{
		Image:      runtimeImage,
		Entrypoint: []string{"sleep", "900"},
		Env: []string{
			// Never prompt: a bad credential must fail, not hang.
			"GIT_TERMINAL_PROMPT=0",
			"GIT_URL=" + cloneUrl,
		},
		WorkingDir: "/work",
		Labels:     map[string]string{labelKey: "git"},
	}, nil, nil, nil, "")
	if err != nil {
		return res, fmt.Errorf("git container: %w", err)
	}
	defer func() {
		_ = m.Docker.ContainerRemove(context.WithoutCancel(ctx), cont.ID, container.RemoveOptions{Force: true})
	}()
	if err := m.Docker.ContainerStart(ctx, cont.ID, container.StartOptions{}); err != nil {
		return res, fmt.Errorf("git start: %w", err)
	}

	progress("source", "cloning "+redact(req.Url))
	// A shallow single-branch clone: a workspace needs the tree, not
	// the history. "$GIT_URL" stays in the environment, so the token
	// never appears in a command line or a log.
	clone := `set -e; git clone --depth 1 --single-branch`
	if req.Ref != "" {
		clone += ` --branch ` + shellQuote(req.Ref)
	}
	clone += ` "$GIT_URL" repo 2>&1`
	if out, err := m.exec(ctx, cont.ID, nil, clone, func(line string) { progress("source", redact(line)) }); err != nil {
		return res, fmt.Errorf("git clone: %w\n%s", err, redact(tail(out, 2000)))
	}
	commit, err := m.exec(ctx, cont.ID, nil, "git -C repo rev-parse HEAD", nil)
	if err != nil {
		return res, fmt.Errorf("git rev-parse: %w", err)
	}
	res.Commit = strings.TrimSpace(commit)

	// The pipeline's root is the subdirectory when one is named; the
	// history and the rest of a monorepo stay behind.
	root := "repo"
	if req.Subdir != "" {
		root = "repo/" + strings.Trim(req.Subdir, "/")
	}
	if _, err := m.exec(ctx, cont.ID, nil,
		fmt.Sprintf("test -d %s || { echo 'subdir not found'; exit 1; }", shellQuote(root)), nil); err != nil {
		return res, fmt.Errorf("git subdir %q: not found in %s", req.Subdir, redact(req.Url))
	}
	if _, err := m.exec(ctx, cont.ID, nil,
		fmt.Sprintf("tar czf /tmp/tree.tgz --exclude .git -C %s .", shellQuote(root)), nil); err != nil {
		return res, fmt.Errorf("pack checkout: %w", err)
	}

	treeGz, err := m.readFile(ctx, cont.ID, "/tmp/tree.tgz")
	if err != nil {
		return res, err
	}
	sum := sha256.Sum256(treeGz)
	res.TreeDigest = "sha256:" + hex.EncodeToString(sum[:])
	res.TreeLocation = req.Location
	if _, err := m.Blobs.Put(ctx, req.Namespace, req.Location, bytes.NewReader(treeGz)); err != nil {
		return res, fmt.Errorf("store tree: %w", err)
	}
	m.Log.Info("workspace source fetched",
		xlog.String("url", redact(req.Url)), xlog.String("commit", res.Commit))
	return res, nil
}

// readFile copies one file out of a container into memory.
func (m *Materializer) readFile(ctx context.Context, containerId, path string) ([]byte, error) {
	local, err := m.copyOut(ctx, containerId, path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = removeAll(local) }()
	f, err := openFile(local)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return io.ReadAll(f)
}

// authenticatedUrl folds a credential into an https URL; an ssh URL
// takes the credential as a key elsewhere (not supported yet).
func authenticatedUrl(raw, credential string) (string, error) {
	if credential == "" {
		return raw, nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("git url %q: %w", redact(raw), err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("credentials over %s are not supported yet; use an https url", u.Scheme)
	}
	// A token as the username is what every hosted forge accepts.
	u.User = url.UserPassword(credential, "x-oauth-basic")
	return u.String(), nil
}

// redact removes any credential embedded in a URL before it reaches a
// log line or an error.
func redact(s string) string {
	for {
		at := strings.Index(s, "@")
		if at < 0 {
			return s
		}
		scheme := strings.LastIndex(s[:at], "://")
		if scheme < 0 {
			return s
		}
		s = s[:scheme+3] + "***@" + s[at+1:]
		if !strings.Contains(s[at:], "@") {
			return s
		}
	}
}

// shellQuote makes one argument safe for the container's shell.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
