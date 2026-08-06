package cache_test

import (
	"context"
	"testing"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/store/kv/cache"
	"github.com/graphene-ci/graphene/internal/store/kv/kvtest"
	"github.com/graphene-ci/graphene/internal/store/kv/memory"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// The point of the whole package, asserted rather than argued: a cache
// that passes the suite its subject passes is a correct cache. Every
// behavior the port promises is checked THROUGH the cache — stale reads,
// missed invalidations and remembered absences all show up as conformance
// failures rather than as something noticed months later.
func TestConformance(t *testing.T) {
	t.Parallel()

	kvtest.Run(t, kvtest.Factory{
		Open: func(*testing.T) kv.Store {
			return cache.New(memory.New())
		},
		OpenShallow: func(_ *testing.T, history int) kv.Store {
			return cache.New(memory.New(memory.WithHistory(history)))
		},
	})
}

// counting is a store that says how many reads reached it.
type counting struct {
	kv.Store

	gets int
}

func (c *counting) Get(ctx context.Context, key kv.Key) (kv.Entry, error) {
	c.gets++

	return c.Store.Get(ctx, key)
}

// A cache that never hits is only a cost. This is the one thing the
// conformance suite cannot check, because a correct store and a correct
// cache are indistinguishable from the outside — which is the point of
// the suite and the reason this test exists beside it.
func TestReadsAreAnsweredFromMemory(t *testing.T) {
	t.Parallel()

	counted := &counting{Store: memory.New()}
	cached := cache.New(counted)

	defer func() { _ = cached.Close() }()

	ctx := context.Background()

	at, err := cached.Put(ctx, kv.Key("a"), []byte("1"), revision.Absent)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	for range 5 {
		if _, err := cached.Get(ctx, kv.Key("a")); err != nil {
			t.Fatalf("get: %v", err)
		}
	}

	if counted.gets != 1 {
		t.Fatalf("five reads reached the store %d times", counted.gets)
	}

	// Absence is remembered too: a reference check asks "is this there"
	// about keys that usually are not.
	for range 5 {
		if _, err := cached.Get(ctx, kv.Key("missing")); err == nil {
			t.Fatal("a missing key was found")
		}
	}

	if counted.gets != 2 {
		t.Fatalf("five reads of a missing key reached the store %d times", counted.gets-1)
	}

	// A write forgets what was known, and the next read pays for it.
	if _, err := cached.Put(ctx, kv.Key("a"), []byte("2"), at); err != nil {
		t.Fatalf("put: %v", err)
	}

	entry, err := cached.Get(ctx, kv.Key("a"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if string(entry.Value) != "2" {
		t.Fatalf("a stale value survived the write: %q", entry.Value)
	}

	if counted.gets != 3 {
		t.Fatalf("the read after a write was answered from memory")
	}
}

// What the cache hands out must not alias what it holds, or one caller
// editing a value it read changes what the next one reads.
func TestWhatIsHandedOutDoesNotAliasWhatIsHeld(t *testing.T) {
	t.Parallel()

	cached := cache.New(memory.New())
	defer func() { _ = cached.Close() }()

	ctx := context.Background()

	if _, err := cached.Put(ctx, kv.Key("a"), []byte("first"), revision.Absent); err != nil {
		t.Fatalf("put: %v", err)
	}

	first, err := cached.Get(ctx, kv.Key("a"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	first.Value[0] = 'X'

	second, err := cached.Get(ctx, kv.Key("a"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if string(second.Value) != "first" {
		t.Fatalf("editing a read value reached the cache: %q", second.Value)
	}
}
