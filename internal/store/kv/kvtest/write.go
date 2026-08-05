package kvtest

import (
	"context"
	"errors"
	"testing"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

func testWrites(t *testing.T, factory Factory) {
	t.Helper()

	// Absent is the expectation "nothing is there yet". It is what makes a
	// create a create, and two writers racing to create the same key
	// resolve without anything between them.
	t.Run("absent means create, and only once", func(t *testing.T) {
		store := open(t, factory)

		put(t, store, "a", "first", revision.Absent)

		_, err := store.Put(context.Background(), kv.Key("a"), []byte("again"), revision.Absent)
		if !errors.Is(err, revision.ErrConflict) {
			t.Fatalf("creating twice: want ErrConflict, got %v", err)
		}
	})

	t.Run("an update needs the revision it read", func(t *testing.T) {
		store := open(t, factory)

		first := put(t, store, "a", "first", revision.Absent)
		second := put(t, store, "a", "second", first)

		// The revision it had before somebody else wrote: stale, and this
		// is the whole of what compare-and-swap is for.
		_, err := store.Put(context.Background(), kv.Key("a"), []byte("third"), first)
		if !errors.Is(err, revision.ErrConflict) {
			t.Fatalf("writing from a stale read: want ErrConflict, got %v", err)
		}

		if got := get(t, store, "a"); !got.Revision.Eq(second) {
			t.Fatalf("the refused write moved the entry to %s", got.Revision)
		}
	})

	t.Run("updating something that is not there is a conflict", func(t *testing.T) {
		store := open(t, factory)

		_, err := store.Put(context.Background(), kv.Key("a"), []byte("first"), 7)
		if !errors.Is(err, revision.ErrConflict) {
			t.Fatalf("want ErrConflict, got %v", err)
		}
	})

	// Deleting what is not there and deleting the wrong version are
	// different mistakes, and a caller does different things about them:
	// one is already done, the other has to be re-read and decided again.
	t.Run("a delete tells missing apart from stale", func(t *testing.T) {
		store := open(t, factory)

		_, err := store.Delete(context.Background(), kv.Key("a"), revision.Absent)
		if !errors.Is(err, kv.ErrNotFound) {
			t.Fatalf("deleting nothing: want ErrNotFound, got %v", err)
		}

		first := put(t, store, "a", "first", revision.Absent)
		second := put(t, store, "a", "second", first)

		_, err = store.Delete(context.Background(), kv.Key("a"), first)
		if !errors.Is(err, revision.ErrConflict) {
			t.Fatalf("deleting a stale version: want ErrConflict, got %v", err)
		}

		at, err := store.Delete(context.Background(), kv.Key("a"), second)
		if err != nil {
			t.Fatalf("delete: %v", err)
		}

		if !at.After(second) {
			t.Fatalf("the delete happened at %s, the write at %s", at, second)
		}

		if _, err := store.Get(context.Background(), kv.Key("a")); !errors.Is(err, kv.ErrNotFound) {
			t.Fatalf("after a delete: want ErrNotFound, got %v", err)
		}
	})

	// What was written must not follow the caller's copy of it afterwards.
	t.Run("what goes in does not alias the caller's memory", func(t *testing.T) {
		store := open(t, factory)

		key := kv.Key("a")
		value := []byte("first")

		if _, err := store.Put(context.Background(), key, value, revision.Absent); err != nil {
			t.Fatalf("put: %v", err)
		}

		key[0] = 'X'
		value[0] = 'X'

		entry, err := store.Get(context.Background(), kv.Key("a"))
		if err != nil {
			t.Fatalf("get: %v", err)
		}

		if string(entry.Value) != "first" {
			t.Fatalf("editing the caller's value reached the store: %q", entry.Value)
		}
	})
}
