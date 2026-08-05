// Package kvtest is what decides whether something is a kv.Store.
//
// The port is an interface, and an interface only says which methods
// exist. Everything that actually matters about a store — that a shorter
// key is a prefix of what is beneath it, that a guarded write refuses a
// stale one, that a watch never invents a snapshot — is behaviour, and
// behaviour is only pinned by running it.
//
// So this suite is written against the PORT and never against any
// implementation. Whatever passes it is a store: the one in memory, the
// one on bbolt, and the cache that wraps either of them. That last one is
// the reason this exists in a package of its own — a cache that passes
// the same suite as the store it wraps is a correct cache, and there is
// no other cheap way to know.
package kvtest

import (
	"context"
	"testing"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Factory opens stores for the suite to work on.
type Factory struct {
	// Open returns an empty store with the implementation's usual limits.
	// Every subtest gets a fresh one.
	Open func(t *testing.T) kv.Store

	// OpenShallow returns an empty store that keeps at most the given
	// number of events for watchers catching up.
	//
	// Compaction is part of the contract and cannot be reached on a store
	// with the usual limits without writing thousands of records first, so
	// an implementation says here how to make a small one. Leave it nil
	// and the compaction subtests are skipped rather than quietly passed.
	OpenShallow func(t *testing.T, history int) kv.Store
}

// Run puts a store through the whole port.
func Run(t *testing.T, factory Factory) {
	t.Helper()

	if factory.Open == nil {
		t.Fatal("kvtest: Factory.Open is required")
	}

	t.Run("reads", func(t *testing.T) { testReads(t, factory) })
	t.Run("writes", func(t *testing.T) { testWrites(t, factory) })
	t.Run("scan", func(t *testing.T) { testScan(t, factory) })
	t.Run("watch", func(t *testing.T) { testWatch(t, factory) })
	t.Run("close", func(t *testing.T) { testClose(t, factory) })
}

// open makes a store and closes it when the test ends.
func open(t *testing.T, factory Factory) kv.Store {
	t.Helper()

	store := factory.Open(t)
	t.Cleanup(func() { _ = store.Close() })

	return store
}

// put writes and fails the test if it could not.
func put(t *testing.T, store kv.Store, key string, value string, expect revision.Revision) revision.Revision {
	t.Helper()

	at, err := store.Put(context.Background(), kv.Key(key), []byte(value), expect)
	if err != nil {
		t.Fatalf("put %q: %v", key, err)
	}

	return at
}

// get reads and fails the test if it could not.
func get(t *testing.T, store kv.Store, key string) kv.Entry {
	t.Helper()

	entry, err := store.Get(context.Background(), kv.Key(key))
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}

	return entry
}
