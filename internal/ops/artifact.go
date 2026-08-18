package ops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/graphene-ci/graphene/internal/infrastructure/blob"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// ArtifactOps implements artifact.Ops over the blob store, bound to one
// namespace.
type ArtifactOps struct {
	namespace string
	store     blob.Store
}

// NewArtifactOps binds the store to a namespace.
func NewArtifactOps(namespace string, store blob.Store) *ArtifactOps {
	return &ArtifactOps{namespace: namespace, store: store}
}

// Stat verifies the blob exists and, for sha256-pinned refs, that the
// bytes match the digest.
func (o *ArtifactOps) Stat(ctx context.Context, _ id.ArtifactId, blobRef ref.BlobRef) (bool, error) {
	digest, pinned := strings.CutPrefix(blobRef.Digest, "sha256:")
	if !pinned {
		return o.store.Exists(ctx, o.namespace, blobRef.Location)
	}
	r, err := o.store.Get(ctx, o.namespace, blobRef.Location)
	if errors.Is(err, blob.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() { _ = r.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return false, err
	}
	if hex.EncodeToString(h.Sum(nil)) != digest {
		return false, fmt.Errorf("blob at %q does not match digest %s", blobRef.Location, blobRef.Digest)
	}
	return true, nil
}

// Delete removes the bytes; not-found is not an error.
func (o *ArtifactOps) Delete(ctx context.Context, _ id.ArtifactId, blobRef ref.BlobRef) error {
	return o.store.Delete(ctx, o.namespace, blobRef.Location)
}
