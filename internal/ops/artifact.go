package ops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// ArtifactOps implements artifact.Ops over a filesystem blob store —
// the dev contour's byte storage (S3 replaces it, the semantics stay:
// Location is a path inside the store, never an absolute file path).
type ArtifactOps struct {
	dir string
}

// NewArtifactOps roots the store.
func NewArtifactOps(dir string) *ArtifactOps {
	return &ArtifactOps{dir: dir}
}

// Stat verifies the blob exists and, for sha256-pinned refs, that the
// bytes match the digest.
func (o *ArtifactOps) Stat(_ context.Context, _ id.ArtifactId, blob ref.BlobRef) (bool, error) {
	path, err := o.path(blob)
	if err != nil {
		return false, err
	}
	f, err := os.Open(path) //nolint:gosec // path is confined by o.path
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	if digest, ok := strings.CutPrefix(blob.Digest, "sha256:"); ok {
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			return false, err
		}
		if hex.EncodeToString(h.Sum(nil)) != digest {
			return false, fmt.Errorf("blob at %q does not match digest %s", blob.Location, blob.Digest)
		}
	}
	return true, nil
}

// Delete removes the bytes; not-found is not an error.
func (o *ArtifactOps) Delete(_ context.Context, _ id.ArtifactId, blob ref.BlobRef) error {
	path, err := o.path(blob)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (o *ArtifactOps) path(blob ref.BlobRef) (string, error) {
	loc := filepath.Clean(strings.TrimPrefix(blob.Location, "/"))
	if loc == "." || !filepath.IsLocal(loc) {
		return "", fmt.Errorf("blob location %q escapes the store", blob.Location)
	}
	return filepath.Join(o.dir, loc), nil
}
