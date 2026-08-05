package kvtest

import (
	"context"
	"errors"
	"testing"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

func testClose(t *testing.T, factory Factory) {
	t.Helper()

	t.Run("a closed store says so instead of answering", func(t *testing.T) {
		store := factory.Open(t)

		put(t, store, "a", "1", revision.Absent)

		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		ctx := context.Background()

		if _, err := store.Get(ctx, kv.Key("a")); !errors.Is(err, kv.ErrClosed) {
			t.Fatalf("get: want ErrClosed, got %v", err)
		}

		if _, err := store.Put(ctx, kv.Key("a"), []byte("2"), revision.Absent); !errors.Is(err, kv.ErrClosed) {
			t.Fatalf("put: want ErrClosed, got %v", err)
		}

		if _, err := store.Delete(ctx, kv.Key("a"), 1); !errors.Is(err, kv.ErrClosed) {
			t.Fatalf("delete: want ErrClosed, got %v", err)
		}

		if _, err := store.Revision(ctx); !errors.Is(err, kv.ErrClosed) {
			t.Fatalf("revision: want ErrClosed, got %v", err)
		}

		if _, err := store.Watch(ctx, kv.Key(""), revision.Beginning); !errors.Is(err, kv.ErrClosed) {
			t.Fatalf("watch: want ErrClosed, got %v", err)
		}

		var failed error

		for _, err := range store.Scan(ctx, kv.Key("")) {
			failed = err

			break
		}

		if !errors.Is(failed, kv.ErrClosed) {
			t.Fatalf("scan: want ErrClosed, got %v", failed)
		}
	})

	t.Run("closing twice is not an error", func(t *testing.T) {
		store := factory.Open(t)

		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		if err := store.Close(); err != nil {
			t.Fatalf("closing twice: %v", err)
		}
	})

	// A stream handed out before the store shut is dead afterwards, and
	// says so through Next rather than blocking forever.
	t.Run("a stream outlives nothing", func(t *testing.T) {
		store := factory.Open(t)

		stream, err := store.Watch(context.Background(), kv.Key(""), revision.Beginning)
		if err != nil {
			t.Fatalf("watch: %v", err)
		}

		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		if _, err := next(stream); !errors.Is(err, kv.ErrClosed) {
			t.Fatalf("want ErrClosed, got %v", err)
		}
	})
}
