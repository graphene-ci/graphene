package bbolt_test

import (
	"path/filepath"
	"testing"

	"github.com/graphene-ci/graphene/internal/core/store"
	"github.com/graphene-ci/graphene/internal/infrastructure/store/bbolt"
	"github.com/graphene-ci/graphene/internal/infrastructure/store/storetest"
)

func TestConformance(t *testing.T) {
	t.Parallel()

	storetest.Run(t, func(t *testing.T) store.Store {
		t.Helper()

		s, err := bbolt.Open(filepath.Join(t.TempDir(), "store.db"))
		if err != nil {
			t.Fatalf("open: %v", err)
		}

		t.Cleanup(func() { _ = s.Close() })

		return s
	})
}
