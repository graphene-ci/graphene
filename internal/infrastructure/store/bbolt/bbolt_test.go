package bbolt_test

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

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

// A store belongs to one kernel. Without saying so, a second one waits on
// the file lock forever — no log, no error, and a process that looks like
// it is running and simply does nothing.
func TestSecondKernelOnOneStoreIsTold(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "store.db")

	first, err := bbolt.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	t.Cleanup(func() { _ = first.Close() })

	started := time.Now()

	_, err = bbolt.Open(path)
	if !errors.Is(err, bbolt.ErrStoreInUse) {
		t.Fatalf("second open: got %v, want ErrStoreInUse", err)
	}

	// And it says so rather than hanging: the message is only useful if
	// it arrives while someone is still looking at the terminal.
	if waited := time.Since(started); waited > 10*time.Second {
		t.Fatalf("took %v to say the store was busy", waited)
	}
}
