package fs_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/graphene-ci/graphene/internal/core/blob"
	blobfs "github.com/graphene-ci/graphene/internal/infrastructure/blob/fs"
)

// Ids arrive from the wire: anything but the minted hex shape must be
// rejected as unknown BEFORE any path is built — no traversal, no panic
// on the fan-out slice.
func TestRejectsForeignIDs(t *testing.T) {
	t.Parallel()

	st, err := blobfs.Open(filepath.Join(t.TempDir(), "blobs"))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = st.Close() })

	ctx := context.Background()

	for _, id := range []string{
		"../../../etc/passwd",
		"..",
		"/absolute/path",
		"a",
		"",
		"DEADBEEFDEADBEEFDEADBEEFDEADBEEF", // uppercase
		"deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef", // wrong length
	} {
		if _, err := st.Stat(ctx, id); !errors.Is(err, blob.ErrNotFound) {
			t.Errorf("stat %q: want ErrNotFound, got %v", id, err)
		}

		if _, _, err := st.Open(ctx, id, 0); !errors.Is(err, blob.ErrNotFound) {
			t.Errorf("open %q: want ErrNotFound, got %v", id, err)
		}

		if err := st.Delete(ctx, id); !errors.Is(err, blob.ErrNotFound) {
			t.Errorf("delete %q: want ErrNotFound, got %v", id, err)
		}
	}
}
