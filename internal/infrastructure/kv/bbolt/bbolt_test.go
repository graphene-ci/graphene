package bbolt_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/graphene-ci/graphene/internal/infrastructure/kv/bbolt"
	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/store/kv/kvtest"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

func open(t *testing.T, options ...bbolt.Option) *bbolt.Store {
	t.Helper()

	store, err := bbolt.Open(filepath.Join(t.TempDir(), "store.db"), options...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	return store
}

// The same suite the in-memory store passes. That is what "is a store"
// means: the port is an interface, and an interface only says which
// methods exist.
func TestConformance(t *testing.T) {
	t.Parallel()

	kvtest.Run(t, kvtest.Factory{
		Open: func(t *testing.T) kv.Store {
			return open(t)
		},
		OpenShallow: func(t *testing.T, history int) kv.Store {
			return open(t, bbolt.WithHistory(history))
		},
	})
}

// The counter and the records outlive the process. This is the whole
// difference from the store in memory, so it is the one thing the shared
// suite cannot check.
func TestAStoreReopensWhereItLeftOff(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "store.db")
	ctx := context.Background()

	first, err := bbolt.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	at, err := first.Put(ctx, kv.Key("a"), []byte("kept"), revision.Absent)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	second, err := bbolt.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	defer func() { _ = second.Close() }()

	entry, err := second.Get(ctx, kv.Key("a"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if string(entry.Value) != "kept" || !entry.Revision.Eq(at) {
		t.Fatalf("came back as %q at %s, written at %s", entry.Value, entry.Revision, at)
	}

	// The counter carries on rather than starting again, which it must:
	// a revision that repeated would make one number name two writes, and
	// every cursor in the system is that number.
	next, err := second.Put(ctx, kv.Key("b"), []byte("1"), revision.Absent)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	if !next.After(at) {
		t.Fatalf("after reopening, a write landed at %s; the last one was at %s", next, at)
	}
}

// A writer never waits for a reader. A watcher that stops draining fills
// its backlog and is dropped where it stands.
func TestAWatcherThatStopsReadingIsDropped(t *testing.T) {
	t.Parallel()

	store := open(t, bbolt.WithBacklog(2))
	defer func() { _ = store.Close() }()

	ctx := context.Background()

	stream, err := store.Watch(ctx, kv.Key(""), revision.Beginning)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	defer func() { _ = stream.Close() }()

	for _, key := range []string{"a", "b", "c", "d"} {
		if _, err := store.Put(ctx, kv.Key(key), []byte("1"), revision.Absent); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
	}

	if _, err := stream.Next(ctx); !errors.Is(err, kv.ErrLagged) {
		t.Fatalf("want ErrLagged, got %v", err)
	}
}

// A write that fails leaves nothing behind: no record, no revision spent,
// and above all no event, because an event nobody committed is a change
// nobody made.
func TestARefusedWriteChangesNothing(t *testing.T) {
	t.Parallel()

	store := open(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()

	at, err := store.Put(ctx, kv.Key("a"), []byte("first"), revision.Absent)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	stream, err := store.Watch(ctx, kv.Key(""), at)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	defer func() { _ = stream.Close() }()

	// Stale, so it is refused.
	if _, err := store.Put(ctx, kv.Key("a"), []byte("second"), 99); !errors.Is(err, revision.ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}

	now, err := store.Revision(ctx)
	if err != nil {
		t.Fatalf("revision: %v", err)
	}

	if !now.Eq(at) {
		t.Fatalf("a refused write moved the store from %s to %s", at, now)
	}

	entry, err := store.Get(ctx, kv.Key("a"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if string(entry.Value) != "first" {
		t.Fatalf("a refused write reached the record: %q", entry.Value)
	}

	deadline, cancel := context.WithTimeout(ctx, 50_000_000)
	defer cancel()

	if _, err := stream.Next(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a refused write was delivered: %v", err)
	}
}
