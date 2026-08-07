package kvtest

import (
	"context"
	"errors"
	"testing"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// testTransactions is the part of being a store that an invariant
// spanning more than one key depends on.
//
// Every claim here is one a caller is entitled to make: what it wrote is
// all there or none of it, what it read is what its writes will land in,
// and a watcher never sees a change that was undone.
func testTransactions(t *testing.T, factory Factory) {
	t.Helper()

	t.Run("all or none", func(t *testing.T) { transactionIsAllOrNone(t, factory) })
	t.Run("reads its own writes", func(t *testing.T) { transactionReadsItsOwnWrites(t, factory) })
	t.Run("a failure consumes no revisions", func(t *testing.T) { rollbackKeepsRevision(t, factory) })
	t.Run("watchers see a commit whole", func(t *testing.T) { watchersSeeTheWholeCommit(t, factory) })
	t.Run("a conflict inside fails the whole", func(t *testing.T) { conflictInsideFailsAll(t, factory) })
}

// errRefused is a work function saying no, which is the ordinary reason a
// transaction is rolled back: a check inside it refused.
var errRefused = errors.New("refused")

// What a transaction is for: two keys written as one change.
func transactionIsAllOrNone(t *testing.T, factory Factory) {
	t.Parallel()

	ctx := context.Background()
	store := open(t, factory)

	// The failing half is written second, so a store that applied writes
	// as they came would leave the first one behind.
	err := store.Do(ctx, func(tx kv.Tx) error {
		if _, err := tx.Put(ctx, kv.Key("a"), []byte("1"), revision.Absent); err != nil {
			return err
		}

		if _, err := tx.Put(ctx, kv.Key("b"), []byte("2"), revision.Absent); err != nil {
			return err
		}

		return errRefused
	})
	if !errors.Is(err, errRefused) {
		t.Fatalf("want errRefused, got %v", err)
	}

	for _, key := range []string{"a", "b"} {
		if _, err := store.Get(ctx, kv.Key(key)); !errors.Is(err, kv.ErrNotFound) {
			t.Fatalf("%s survived a rolled-back transaction: %v", key, err)
		}
	}

	// And the same two writes, committed.
	if err := store.Do(ctx, func(tx kv.Tx) error {
		if _, err := tx.Put(ctx, kv.Key("a"), []byte("1"), revision.Absent); err != nil {
			return err
		}

		_, err := tx.Put(ctx, kv.Key("b"), []byte("2"), revision.Absent)

		return err
	}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	for _, key := range []string{"a", "b"} {
		if _, err := store.Get(ctx, kv.Key(key)); err != nil {
			t.Fatalf("%s did not survive a committed transaction: %v", key, err)
		}
	}
}

// A check inside a transaction has to see the transaction's own writes,
// or the check is about a world that is already gone.
func transactionReadsItsOwnWrites(t *testing.T, factory Factory) {
	t.Parallel()

	ctx := context.Background()
	store := open(t, factory)

	if err := store.Do(ctx, func(tx kv.Tx) error {
		at, err := tx.Put(ctx, kv.Key("a"), []byte("1"), revision.Absent)
		if err != nil {
			return err
		}

		read, err := tx.Get(ctx, kv.Key("a"))
		if err != nil {
			return err
		}

		if string(read.Value) != "1" || !read.Revision.Eq(at) {
			t.Errorf("read back %q at %s, wrote %q at %s", read.Value, read.Revision, "1", at)
		}

		// And a scan sees it too, which is what a check walking a subtree
		// depends on.
		seen := 0

		for entry, err := range tx.Scan(ctx, kv.Key("a")) {
			if err != nil {
				return err
			}

			if string(entry.Key) == "a" {
				seen++
			}
		}

		if seen != 1 {
			t.Errorf("a scan inside the transaction saw its own write %d times", seen)
		}

		return nil
	}); err != nil {
		t.Fatalf("transaction: %v", err)
	}
}

// A rolled-back transaction consumes nothing. A store that let its
// revision run on would leave gaps a watcher resumes into and finds
// nothing at.
func rollbackKeepsRevision(t *testing.T, factory Factory) {
	t.Parallel()

	ctx := context.Background()
	store := open(t, factory)

	before, err := store.Revision(ctx)
	if err != nil {
		t.Fatalf("revision: %v", err)
	}

	err = store.Do(ctx, func(tx kv.Tx) error {
		if _, err := tx.Put(ctx, kv.Key("a"), []byte("1"), revision.Absent); err != nil {
			return err
		}

		return errRefused
	})
	if !errors.Is(err, errRefused) {
		t.Fatalf("want errRefused, got %v", err)
	}

	after, err := store.Revision(ctx)
	if err != nil {
		t.Fatalf("revision: %v", err)
	}

	if !after.Eq(before) {
		t.Fatalf("a rolled-back transaction moved the revision from %s to %s", before, after)
	}
}

// A watcher sees every write of a committed transaction, and none of a
// rolled-back one. Delivery from inside would hand out a change that the
// rollback then un-made.
func watchersSeeTheWholeCommit(t *testing.T, factory Factory) {
	t.Parallel()

	ctx := context.Background()
	store := open(t, factory)

	at, err := store.Revision(ctx)
	if err != nil {
		t.Fatalf("revision: %v", err)
	}

	stream, err := store.Watch(ctx, kv.Key(""), at)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	defer func() { _ = stream.Close() }()

	// One that fails, then one that does not. The watcher must see only
	// the second, and both of its writes.
	_ = store.Do(ctx, func(tx kv.Tx) error {
		if _, err := tx.Put(ctx, kv.Key("gone"), []byte("x"), revision.Absent); err != nil {
			return err
		}

		return errRefused
	})

	if err := store.Do(ctx, func(tx kv.Tx) error {
		if _, err := tx.Put(ctx, kv.Key("a"), []byte("1"), revision.Absent); err != nil {
			return err
		}

		_, err := tx.Put(ctx, kv.Key("b"), []byte("2"), revision.Absent)

		return err
	}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	for _, want := range []string{"a", "b"} {
		event, err := stream.Next(ctx)
		if err != nil {
			t.Fatalf("next: %v", err)
		}

		if string(event.Entry.Key) != want {
			t.Fatalf("watched %q, wanted %q — a rolled-back write reached a watcher",
				event.Entry.Key, want)
		}
	}
}

// A guarded write that loses inside a transaction takes the whole
// transaction with it. Otherwise a caller could not tell "my check
// passed" from "my check passed and somebody else's write did not".
func conflictInsideFailsAll(t *testing.T, factory Factory) {
	t.Parallel()

	ctx := context.Background()
	store := open(t, factory)

	if _, err := store.Put(ctx, kv.Key("taken"), []byte("1"), revision.Absent); err != nil {
		t.Fatalf("put: %v", err)
	}

	err := store.Do(ctx, func(tx kv.Tx) error {
		if _, err := tx.Put(ctx, kv.Key("fresh"), []byte("1"), revision.Absent); err != nil {
			return err
		}

		// Absent, and something is there: the conflict every guarded
		// write can lose.
		_, err := tx.Put(ctx, kv.Key("taken"), []byte("2"), revision.Absent)

		return err
	})
	if !errors.Is(err, revision.ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}

	if _, err := store.Get(ctx, kv.Key("fresh")); !errors.Is(err, kv.ErrNotFound) {
		t.Fatal("the write before the conflict survived")
	}
}
