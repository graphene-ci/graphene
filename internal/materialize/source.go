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

// The checkout runs in the SAME toolchain image as the build: the Go
// image already carries git, so an installation pulls one image, not
// two (and community images are not mirrored where docker.io is
// blocked).

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
	// Runtime names the toolchain image that does the checkout (it
	// carries git); empty takes the installation's default.
	Runtime string
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
	rt, err := m.runtime(req.Runtime)
	if err != nil {
		return res, err
	}
	runtimeImage := rt.Image
	if err := m.ensureImage(ctx, runtimeImage, progress); err != nil {
		return res, err
	}
	// The URL and the ref go to git as data, not as options: git's
	// ext:: transport executes an arbitrary command, file:// reads the
	// server's own disk, and any value starting with "-" becomes a
	// flag. All three are rejected before the container starts.
	if err := validateGitUrl(req.Url); err != nil {
		return res, err
	}
	if err := validateGitRef(req.Ref); err != nil {
		return res, err
	}
	if err := validateSubdir(req.Subdir); err != nil {
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
		// "--branch=<ref>" keeps a ref that survived validation from
		// ever being read as a separate option.
		clone += ` --branch=` + shellQuote(req.Ref)
	}
	// "--" ends the options: whatever the URL holds is a URL.
	clone += ` -- "$GIT_URL" repo 2>&1`
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

// validateGitUrl accepts only the transports a workspace may use.
// git's own ext:: and file:: transports run commands and read local
// paths — a workspace source must never reach them.
func validateGitUrl(raw string) error {
	if raw == "" {
		return fmt.Errorf("git source needs a url")
	}
	if strings.HasPrefix(raw, "-") {
		return fmt.Errorf("git url %q looks like an option", raw)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("git url %q: %w", redact(raw), err)
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		if u.Host == "" {
			return fmt.Errorf("git url %q has no host", redact(raw))
		}
		return nil
	case "":
		return fmt.Errorf("git url %q needs an https:// scheme", raw)
	default:
		return fmt.Errorf("git transport %q is not allowed; use https", u.Scheme)
	}
}

// validateGitRef keeps a ref from becoming an option or a path trick.
func validateGitRef(ref string) error {
	if ref == "" {
		return nil
	}
	if strings.HasPrefix(ref, "-") {
		return fmt.Errorf("git ref %q looks like an option", ref)
	}
	if strings.Contains(ref, "..") || strings.ContainsAny(ref, " \t\n\r~^:?*[\\") {
		return fmt.Errorf("git ref %q is not a valid ref name", ref)
	}
	return nil
}

// validateSubdir keeps the pipeline root inside the checkout.
func validateSubdir(subdir string) error {
	if subdir == "" {
		return nil
	}
	clean := strings.Trim(subdir, "/")
	if strings.HasPrefix(clean, "-") {
		return fmt.Errorf("subdir %q looks like an option", subdir)
	}
	for _, part := range strings.Split(clean, "/") {
		if part == ".." {
			return fmt.Errorf("subdir %q escapes the checkout", subdir)
		}
	}
	if strings.ContainsAny(clean, "\n\r") {
		return fmt.Errorf("subdir %q is not a path", subdir)
	}
	return nil
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
// log line or an error: everything between "scheme://" and the "@" of
// the authority is replaced.
func redact(s string) string {
	var b strings.Builder
	rest := s
	for {
		i := strings.Index(rest, "://")
		if i < 0 {
			b.WriteString(rest)
			return b.String()
		}
		b.WriteString(rest[:i+3])
		rest = rest[i+3:]
		authority := rest
		tail := ""
		if end := strings.IndexByte(rest, '/'); end >= 0 {
			authority, tail = rest[:end], rest[end:]
		}
		if at := strings.LastIndex(authority, "@"); at >= 0 {
			b.WriteString("***")
			b.WriteString(authority[at:])
		} else {
			b.WriteString(authority)
		}
		if tail == "" {
			return b.String()
		}
		rest = tail
	}
}

// shellQuote makes one argument safe for the container's shell.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
