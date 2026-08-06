package fs_test

import (
	"testing"

	"github.com/graphene-ci/graphene/internal/blob"
	"github.com/graphene-ci/graphene/internal/blob/blobtest"
	"github.com/graphene-ci/graphene/internal/infrastructure/blob/fs"
)

func TestConformance(t *testing.T) {
	t.Parallel()

	blobtest.Run(t, blobtest.Factory{
		Open: func(t *testing.T) blob.Store {
			t.Helper()

			store, err := fs.Open(t.TempDir())
			if err != nil {
				t.Fatalf("open: %v", err)
			}

			return store
		},
	})
}
