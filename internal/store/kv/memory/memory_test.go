package memory_test

import (
	"context"
	"errors"
	"testing"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/store/kv/kvtest"
	"github.com/graphene-ci/graphene/internal/store/kv/memory"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// The suite is what decides whether this is a store. Everything below it
// is about the parts only this implementation can reach.
func TestConformance(t *testing.T) {
	t.Parallel()

	kvtest.Run(t, kvtest.Factory{
		Open: func(*testing.T) kv.Store {
			return memory.New()
		},
		OpenShallow: func(_ *testing.T, history int) kv.Store {
			return memory.New(memory.WithHistory(history))
		},
	})
}

// A writer never waits for a reader. A watcher that stops draining fills
// its backlog and is dropped where it stands — told so, and told the same
// thing however many times it asks, because the events are gone.
func TestAWatcherThatStopsReadingIsDropped(t *testing.T) {
	t.Parallel()

	store := memory.New(memory.WithBacklog(2))
	defer func() { _ = store.Close() }()

	stream, err := store.Watch(context.Background(), kv.Key(""), revision.Beginning)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	defer func() { _ = stream.Close() }()

	// Nobody is reading, and the writer does not care.
	for _, key := range []string{"a", "b", "c", "d"} {
		if _, err := store.Put(context.Background(), kv.Key(key), []byte("1"), revision.Absent); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
	}

	if _, err := stream.Next(context.Background()); !errors.Is(err, kv.ErrLagged) {
		t.Fatalf("want ErrLagged, got %v", err)
	}

	// Asking again does not help and must not pretend to: the answer is a
	// fresh snapshot, not a retry.
	if _, err := stream.Next(context.Background()); !errors.Is(err, kv.ErrLagged) {
		t.Fatalf("asking twice: want ErrLagged, got %v", err)
	}
}

// A watcher that keeps up is not dropped, however many writes go past it.
func TestAWatcherThatKeepsUpIsNotDropped(t *testing.T) {
	t.Parallel()

	store := memory.New(memory.WithBacklog(2))
	defer func() { _ = store.Close() }()

	stream, err := store.Watch(context.Background(), kv.Key(""), revision.Beginning)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	defer func() { _ = stream.Close() }()

	for _, key := range []string{"a", "b", "c", "d", "e"} {
		if _, err := store.Put(context.Background(), kv.Key(key), []byte("1"), revision.Absent); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}

		event, err := stream.Next(context.Background())
		if err != nil {
			t.Fatalf("next after %q: %v", key, err)
		}

		if string(event.Entry.Key) != key {
			t.Fatalf("delivered %s, wrote %q", event.Entry.Key, key)
		}
	}
}
