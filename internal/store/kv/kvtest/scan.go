package kvtest

import (
	"context"
	"testing"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

func testScan(t *testing.T, factory Factory) {
	t.Helper()

	// The one property everything above the byte layer is built out of. A
	// scan, a watch and a grant confined to a subtree are each this and
	// nothing else, so an implementation that gets it subtly wrong breaks
	// all three at once and none of them visibly.
	t.Run("a prefix covers what is beneath it and nothing else", func(t *testing.T) {
		store := open(t, factory)

		for _, key := range []string{"a\x1f", "a\x1fb\x1f", "a\x1fc\x1f", "a2\x1f", "z\x1f"} {
			put(t, store, key, key, revision.Absent)
		}

		got := keys(t, store, "a\x1f")

		want := []string{"a\x1f", "a\x1fb\x1f", "a\x1fc\x1f"}
		if !equal(got, want) {
			t.Fatalf("scanning under a: got %q, want %q", got, want)
		}

		// "a2" begins with "a", and it must not be under it. This is why
		// every path value ends in a separator.
		for _, key := range got {
			if key == "a2\x1f" {
				t.Fatal("a2 counted as being under a")
			}
		}
	})

	t.Run("the empty prefix is everything", func(t *testing.T) {
		store := open(t, factory)

		for _, key := range []string{"b", "a", "c"} {
			put(t, store, key, key, revision.Absent)
		}

		got := keys(t, store, "")
		if len(got) != 3 {
			t.Fatalf("the whole store is %q", got)
		}
	})

	// Key order and not insertion order: a consumer paging through a
	// subtree relies on the walk being the same every time.
	t.Run("entries come back in key order", func(t *testing.T) {
		store := open(t, factory)

		for _, key := range []string{"c", "a", "b"} {
			put(t, store, key, key, revision.Absent)
		}

		got := keys(t, store, "")
		if !equal(got, []string{"a", "b", "c"}) {
			t.Fatalf("walked as %q", got)
		}
	})

	// An iterator and not a page-and-cursor pair, which means stopping is
	// the caller's business and costs nothing to say.
	t.Run("breaking out stops the walk", func(t *testing.T) {
		store := open(t, factory)

		for _, key := range []string{"a", "b", "c"} {
			put(t, store, key, key, revision.Absent)
		}

		seen := 0

		for _, err := range store.Scan(context.Background(), kv.Key("")) {
			if err != nil {
				t.Fatalf("scan: %v", err)
			}

			seen++

			break
		}

		if seen != 1 {
			t.Fatalf("the walk ran %d times after a break", seen)
		}
	})

	t.Run("a scan of nothing is empty and not an error", func(t *testing.T) {
		store := open(t, factory)

		if got := keys(t, store, "nothing"); len(got) != 0 {
			t.Fatalf("got %q", got)
		}
	})

	// A cancelled context stops the walk with its own error rather than
	// ending it quietly, which would look identical to "there was nothing
	// left".
	t.Run("a cancelled context stops the walk loudly", func(t *testing.T) {
		store := open(t, factory)

		for _, key := range []string{"a", "b", "c"} {
			put(t, store, key, key, revision.Absent)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		var failed error

		for _, err := range store.Scan(ctx, kv.Key("")) {
			if err != nil {
				failed = err

				break
			}
		}

		if failed == nil {
			t.Fatal("a cancelled scan ended as though it had finished")
		}
	})
}

// keys walks a prefix and collects what it found.
func keys(t *testing.T, store kv.Store, prefix string) []string {
	t.Helper()

	var found []string

	for entry, err := range store.Scan(context.Background(), kv.Key(prefix)) {
		if err != nil {
			t.Fatalf("scan %q: %v", prefix, err)
		}

		found = append(found, string(entry.Key))
	}

	return found
}

func equal(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}

	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}

	return true
}
