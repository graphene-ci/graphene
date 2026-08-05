package kvtest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// patience is how long a Next is given before it counts as "nothing was
// pending". It is a deadline and not a sleep: delivery is synchronous, so
// an event that is coming is already queued before Next is called, and
// this only bounds the case where nothing is.
const patience = 50 * time.Millisecond

func testWatch(t *testing.T, factory Factory) {
	t.Helper()

	// A watch delivers CHANGES and never a snapshot. That is the whole
	// reason it looks the way it does: a snapshot delivered as synthetic
	// events carries each key's last-write revision, so those revisions
	// are not in order, and resuming from one silently loses whatever
	// sorted after it.
	t.Run("a watch delivers no snapshot", func(t *testing.T) {
		store := open(t, factory)

		put(t, store, "a", "1", revision.Absent)

		at, err := store.Revision(context.Background())
		if err != nil {
			t.Fatalf("revision: %v", err)
		}

		stream := watch(t, store, "", at)

		if _, err := next(stream); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("a fresh watch handed back %v", err)
		}
	})

	// Delivery is synchronous with the write, which is what makes this
	// testable without concurrency: write, then read, and the answer is
	// there. Nothing sleeps waiting for another goroutine to wake up.
	t.Run("a write is delivered to a watch already open", func(t *testing.T) {
		store := open(t, factory)

		stream := watch(t, store, "", revision.Beginning)

		at := put(t, store, "a", "1", revision.Absent)

		event, err := next(stream)
		if err != nil {
			t.Fatalf("next: %v", err)
		}

		if event.Kind != kv.EventPut || !event.Entry.Revision.Eq(at) {
			t.Fatalf("delivered %s", event)
		}

		if string(event.Entry.Value) != "1" {
			t.Fatalf("value came through as %q", event.Entry.Value)
		}
	})

	// A delete carries the value the entry LAST had. Whoever filters a
	// stream — a grant, a controller — has to be able to ask what it was
	// that went away, and afterwards there is nowhere else to ask.
	t.Run("a delete carries what it removed", func(t *testing.T) {
		store := open(t, factory)

		at := put(t, store, "a", "gone", revision.Absent)
		stream := watch(t, store, "", at)

		if _, err := store.Delete(context.Background(), kv.Key("a"), at); err != nil {
			t.Fatalf("delete: %v", err)
		}

		event, err := next(stream)
		if err != nil {
			t.Fatalf("next: %v", err)
		}

		if event.Kind != kv.EventDelete {
			t.Fatalf("delivered %s", event)
		}

		if string(event.Entry.Value) != "gone" {
			t.Fatalf("the departed value came through as %q", event.Entry.Value)
		}
	})

	t.Run("a watch is confined to its prefix", func(t *testing.T) {
		store := open(t, factory)

		stream := watch(t, store, "a\x1f", revision.Beginning)

		put(t, store, "z\x1f", "1", revision.Absent)
		put(t, store, "a\x1fb\x1f", "1", revision.Absent)

		event, err := next(stream)
		if err != nil {
			t.Fatalf("next: %v", err)
		}

		if string(event.Entry.Key) != "a\x1fb\x1f" {
			t.Fatalf("delivered %s, which is not under the prefix", event.Entry.Key)
		}
	})

	// The three lines that take a snapshot, and the reason for their
	// order: a write between the cursor and the scan is seen TWICE, which
	// a reconciling consumer is built for. The other order loses it.
	t.Run("cursor first, then snapshot, then changes", func(t *testing.T) {
		store := open(t, factory)

		put(t, store, "a", "1", revision.Absent)

		at, err := store.Revision(context.Background())
		if err != nil {
			t.Fatalf("revision: %v", err)
		}

		// Somebody writes while the snapshot is being taken.
		put(t, store, "b", "1", revision.Absent)

		snapshot := keys(t, store, "")
		if !equal(snapshot, []string{"a", "b"}) {
			t.Fatalf("snapshot saw %q", snapshot)
		}

		stream := watch(t, store, "", at)

		event, err := next(stream)
		if err != nil {
			t.Fatalf("next: %v", err)
		}

		if string(event.Entry.Key) != "b" {
			t.Fatalf("replayed %s, expected the write that raced the snapshot", event.Entry.Key)
		}
	})

	// A watch that asks for history the store no longer keeps is told so
	// AT THE CALL, not somewhere in the middle of the stream, because the
	// answer is not to retry — it is to take a fresh snapshot.
	t.Run("history that is gone is refused at the call", func(t *testing.T) {
		if factory.OpenShallow == nil {
			t.Skip("implementation does not say how to make a shallow store")
		}

		store := factory.OpenShallow(t, 2)
		t.Cleanup(func() { _ = store.Close() })

		first := put(t, store, "a", "1", revision.Absent)

		for _, key := range []string{"b", "c", "d"} {
			put(t, store, key, "1", revision.Absent)
		}

		_, err := store.Watch(context.Background(), kv.Key(""), first)
		if !errors.Is(err, revision.ErrCompacted) {
			t.Fatalf("want ErrCompacted, got %v", err)
		}

		// What the store DOES still hold is served as usual.
		now, err := store.Revision(context.Background())
		if err != nil {
			t.Fatalf("revision: %v", err)
		}

		if _, err := store.Watch(context.Background(), kv.Key(""), now); err != nil {
			t.Fatalf("watching from now: %v", err)
		}
	})

	t.Run("a closed stream says so", func(t *testing.T) {
		store := open(t, factory)

		stream := watch(t, store, "", revision.Beginning)

		if err := stream.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		// Twice is not an error: a consumer unwinding does not have to
		// remember whether it already did.
		if err := stream.Close(); err != nil {
			t.Fatalf("closing twice: %v", err)
		}

		if _, err := next(stream); !errors.Is(err, kv.ErrClosed) {
			t.Fatalf("want ErrClosed, got %v", err)
		}
	})
}

// watch opens a stream and closes it when the test ends.
func watch(t *testing.T, store kv.Store, prefix string, after revision.Revision) kv.Stream {
	t.Helper()

	stream, err := store.Watch(context.Background(), kv.Key(prefix), after)
	if err != nil {
		t.Fatalf("watch %q: %v", prefix, err)
	}

	t.Cleanup(func() { _ = stream.Close() })

	return stream
}

// next pulls one event, giving up after patience.
//
// The deadline is how a caller does anything else at the same time — the
// context IS the select, so holding a timer needs no channel and no
// goroutine. That is the pattern a real consumer uses for its re-sync
// tick, and the tests use it for the same reason.
func next(stream kv.Stream) (kv.Event, error) {
	ctx, cancel := context.WithTimeout(context.Background(), patience)
	defer cancel()

	return stream.Next(ctx)
}
