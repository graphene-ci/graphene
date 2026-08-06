package cache_test

import (
	"context"
	"testing"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/store/kv/cache"
	"github.com/graphene-ci/graphene/internal/store/kv/memory"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// gated is a store that can be stopped in the middle of a call, so that
// two of them can be interleaved on purpose rather than hoped into place.
type gated struct {
	kv.Store

	putEntered chan struct{}
	putRelease chan struct{}
	getRead    chan struct{}
	getRelease chan struct{}
}

func (g *gated) Put(
	ctx context.Context,
	key kv.Key,
	value []byte,
	expect revision.Revision,
) (revision.Revision, error) {
	g.putEntered <- struct{}{}

	<-g.putRelease

	return g.Store.Put(ctx, key, value, expect)
}

func (g *gated) Get(ctx context.Context, key kv.Key) (kv.Entry, error) {
	entry, err := g.Store.Get(ctx, key)

	g.getRead <- struct{}{}

	<-g.getRelease

	return entry, err
}

// A read that missed goes away to the store and comes back with what it
// found. If a write lands while it is away, what it found is already old
// — and filling the cache with it would leave a stale answer that nothing
// later would correct, because nothing later knows it is wrong.
//
// The conformance suite cannot catch this: it runs one call at a time,
// and there is no interleaving to get wrong. So the interleaving is built
// here, by hand, deterministically.
func TestAReadThatWasAwayDuringAWriteDoesNotFillTheCache(t *testing.T) {
	t.Parallel()

	under := &gated{
		Store:      memory.New(),
		putEntered: make(chan struct{}),
		putRelease: make(chan struct{}),
		getRead:    make(chan struct{}),
		getRelease: make(chan struct{}),
	}

	cached := cache.New(under)
	ctx := context.Background()

	// Seed, and let the gate through for it.
	go func() { <-under.putEntered; under.putRelease <- struct{}{} }()

	at, err := cached.Put(ctx, kv.Key("a"), []byte("first"), revision.Absent)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	// A write starts: the cache has already forgotten the key, and the
	// store has not written yet.
	written := make(chan error, 1)

	go func() {
		_, err := cached.Put(ctx, kv.Key("a"), []byte("second"), at)
		written <- err
	}()

	<-under.putEntered

	// A read misses, goes to the store, and reads the OLD value — the
	// write above has not landed.
	read := make(chan kv.Entry, 1)

	go func() {
		entry, err := cached.Get(ctx, kv.Key("a"))
		if err != nil {
			t.Errorf("get: %v", err)
		}

		read <- entry
	}()

	<-under.getRead

	// Now the write lands.
	under.putRelease <- struct{}{}

	if err := <-written; err != nil {
		t.Fatalf("write: %v", err)
	}

	// And only now does the read come back, holding what is already old.
	under.getRelease <- struct{}{}

	if got := <-read; string(got.Value) != "first" {
		t.Fatalf("the read was supposed to be holding the old value, got %q", got.Value)
	}

	// The next read must not be answered from what that one left behind.
	go func() { <-under.getRead; under.getRelease <- struct{}{} }()

	entry, err := cached.Get(ctx, kv.Key("a"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if string(entry.Value) != "second" {
		t.Fatalf("a read that was away during a write left %q in the cache", entry.Value)
	}
}
