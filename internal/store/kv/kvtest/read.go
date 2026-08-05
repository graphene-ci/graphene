package kvtest

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

func testReads(t *testing.T, factory Factory) {
	t.Helper()

	t.Run("a missing key is not found", func(t *testing.T) {
		store := open(t, factory)

		if _, err := store.Get(context.Background(), kv.Key("nothing")); !errors.Is(err, kv.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})

	t.Run("a stored entry carries both revisions", func(t *testing.T) {
		store := open(t, factory)

		at := put(t, store, "a", "first", revision.Absent)

		entry := get(t, store, "a")
		if !entry.Revision.Eq(at) || !entry.CreatedRevision.Eq(at) {
			t.Fatalf("a fresh entry is at %s, created %s, written at %s",
				entry.Revision, entry.CreatedRevision, at)
		}

		if !bytes.Equal(entry.Value, []byte("first")) {
			t.Fatalf("value came back as %q", entry.Value)
		}

		if !entry.Key.Equal(kv.Key("a")) {
			t.Fatalf("key came back as %s", entry.Key)
		}
	})

	// The birth revision is what tells a record from the different one
	// that later took its key, so it has to survive every update and NOT
	// survive a delete.
	t.Run("the birth revision survives updates and not deletes", func(t *testing.T) {
		store := open(t, factory)

		born := put(t, store, "a", "first", revision.Absent)
		updated := put(t, store, "a", "second", born)

		entry := get(t, store, "a")
		if !entry.CreatedRevision.Eq(born) || !entry.Revision.Eq(updated) {
			t.Fatalf("after an update: created %s, at %s", entry.CreatedRevision, entry.Revision)
		}

		if _, err := store.Delete(context.Background(), kv.Key("a"), updated); err != nil {
			t.Fatalf("delete: %v", err)
		}

		again := put(t, store, "a", "third", revision.Absent)

		reborn := get(t, store, "a")
		if reborn.CreatedRevision.Eq(born) {
			t.Fatal("a recreated key kept the birth revision of the one it replaced")
		}

		if !reborn.CreatedRevision.Eq(again) {
			t.Fatalf("recreated at %s, born %s", again, reborn.CreatedRevision)
		}
	})

	// A store hands back memory it owns. Editing what it handed back must
	// not reach inside it — the bug that causes does not look like a bug,
	// it looks like a value that was right and later was not.
	t.Run("what comes out does not alias what is stored", func(t *testing.T) {
		store := open(t, factory)

		put(t, store, "a", "first", revision.Absent)

		entry := get(t, store, "a")
		entry.Value[0] = 'X'
		entry.Key[0] = 'X'

		again := get(t, store, "a")
		if !bytes.Equal(again.Value, []byte("first")) {
			t.Fatalf("editing a returned value reached the store: %q", again.Value)
		}

		if !again.Key.Equal(kv.Key("a")) {
			t.Fatalf("editing a returned key reached the store: %s", again.Key)
		}
	})

	// The counter is store-wide and not per key. That is what makes one
	// number enough for a watcher to resume from.
	t.Run("the revision counts every write in the store", func(t *testing.T) {
		store := open(t, factory)

		empty, err := store.Revision(context.Background())
		if err != nil {
			t.Fatalf("revision: %v", err)
		}

		first := put(t, store, "a", "1", revision.Absent)
		second := put(t, store, "b", "1", revision.Absent)

		if !first.After(empty) || !second.After(first) {
			t.Fatalf("revisions did not move: %s → %s → %s", empty, first, second)
		}

		now, err := store.Revision(context.Background())
		if err != nil {
			t.Fatalf("revision: %v", err)
		}

		if !now.Eq(second) {
			t.Fatalf("the store is at %s after writing at %s", now, second)
		}
	})
}
