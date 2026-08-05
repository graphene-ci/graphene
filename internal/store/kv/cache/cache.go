// Package cache is a kv.Store that remembers what another one said.
//
// It is a Store wrapping a Store, which is the whole design: it goes
// where the store went, nothing above it knows it is there, and it is
// held to exactly the same conformance suite. A cache that passes the
// suite its subject passes is a correct cache, and there is no other
// cheap way to know that.
//
// It caches single reads and nothing else. A scan or a watch goes
// straight through — a scan's answer depends on every key under a prefix,
// so keeping one would mean invalidating it on every write anywhere near
// it, which is most of the cost of doing the scan.
//
// ONE CACHE PER STORE, wired where the process is assembled. Two of them
// over one store would each be blind to the other's writes, and blind in
// the way that looks like working.
package cache

import (
	"context"
	"errors"
	"iter"
	"sync"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// defaultSize is how many keys are remembered before one is forgotten.
const defaultSize = 4096

// Cache is a kv.Store in front of a kv.Store.
var _ kv.Store = (*Cache)(nil)

// Cache remembers what single reads returned.
//
// Correctness rests on one thing: every write goes through here, so the
// cache is told about each one by the caller doing it, under a lock, with
// no window in between. That holds because a store is opened once and
// handed to one kernel — and it is why two caches over one store would be
// wrong.
type Cache struct {
	under kv.Store

	mu      sync.RWMutex
	entries map[string]remembered
	// generation counts writes per key. It is what closes the race
	// between a read that missed and a write that landed while it was
	// away: a read only fills the cache if nothing happened to the key
	// while it was reading.
	generation map[string]uint64
	size       int
	closed     bool
}

// remembered is what a read found, including finding nothing.
//
// Absence is remembered too, on purpose. A reference check asks "is this
// there" about keys that are often not, and a cache that only remembered
// answers would miss every time on exactly the question being asked most.
type remembered struct {
	entry kv.Entry
	found bool
}

// Option configures a cache.
type Option func(*Cache)

// WithSize sets how many keys are remembered before one is forgotten.
func WithSize(keys int) Option {
	return func(c *Cache) {
		if keys > 0 {
			c.size = keys
		}
	}
}

// New puts a cache in front of a store.
//
// The cache TAKES the store: closing the cache closes what it wraps.
// Anything else would leave two owners of one lifetime and a way to shut
// the store while a cache in front of it went on answering.
func New(under kv.Store, options ...Option) *Cache {
	cache := &Cache{
		under:      under,
		entries:    map[string]remembered{},
		generation: map[string]uint64{},
		size:       defaultSize,
	}

	for _, option := range options {
		option(cache)
	}

	return cache
}

// Get returns the entry under key, or kv.ErrNotFound — from memory when
// it can.
func (c *Cache) Get(ctx context.Context, key kv.Key) (kv.Entry, error) {
	if err := ctx.Err(); err != nil {
		return kv.Entry{}, err
	}

	held, generation, ok := c.remembered(key)
	if ok {
		if !held.found {
			return kv.Entry{}, kv.ErrNotFound
		}

		return held.entry.Clone(), nil
	}

	entry, err := c.under.Get(ctx, key)

	switch {
	case err == nil:
		c.remember(key, generation, remembered{entry: entry.Clone(), found: true})
	case errors.Is(err, kv.ErrNotFound):
		c.remember(key, generation, remembered{})
	default:
		return kv.Entry{}, err
	}

	return entry, err
}

// Put writes through, forgetting what it knew both before and after.
//
// Before AND after, which looks like one too many. It is not: a read that
// missed may be away in the store while this write lands, and it decides
// whether to fill by comparing the generation it saw on the way out. One
// bump on either side of the write means no read can span it and still
// think nothing happened.
func (c *Cache) Put(
	ctx context.Context,
	key kv.Key,
	value []byte,
	expect revision.Revision,
) (revision.Revision, error) {
	c.forget(key)

	at, err := c.under.Put(ctx, key, value, expect)

	c.forget(key)

	return at, err
}

// Delete writes through, under the same rule as Put.
func (c *Cache) Delete(ctx context.Context, key kv.Key, expect revision.Revision) (revision.Revision, error) {
	c.forget(key)

	at, err := c.under.Delete(ctx, key, expect)

	c.forget(key)

	return at, err
}

// Scan goes straight through. A scan's answer depends on every key under
// a prefix, so remembering one would mean forgetting it again on every
// write anywhere near it.
func (c *Cache) Scan(ctx context.Context, prefix kv.Key) iter.Seq2[kv.Entry, error] {
	return c.under.Scan(ctx, prefix)
}

// Watch goes straight through: a stream is not an answer to remember.
func (c *Cache) Watch(ctx context.Context, prefix kv.Key, after revision.Revision) (kv.Stream, error) {
	return c.under.Watch(ctx, prefix, after)
}

// Revision goes straight through. It moves on every write in the store,
// including ones to keys this cache has never seen, so there is nothing
// here that could know when to forget it.
func (c *Cache) Revision(ctx context.Context) (revision.Revision, error) {
	return c.under.Revision(ctx)
}

// Close forgets everything and closes what it wraps.
func (c *Cache) Close() error {
	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()

		return nil
	}

	c.closed = true

	clear(c.entries)
	clear(c.generation)
	c.mu.Unlock()

	return c.under.Close()
}

// remembered reads what is held for a key, and the generation to compare
// against if it is not held.
func (c *Cache) remembered(key kv.Key) (remembered, uint64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	held, ok := c.entries[string(key)]

	return held, c.generation[string(key)], ok
}

// remember fills the cache, unless the key moved while the read was away.
func (c *Cache) remember(key kv.Key, generation uint64, held remembered) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed || c.generation[string(key)] != generation {
		return
	}

	c.evict()

	c.entries[string(key)] = held
}

// forget drops a key and counts the write against it.
func (c *Cache) forget(key kv.Key) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.entries, string(key))
	c.generation[string(key)]++
}

// evict makes room, forgetting an arbitrary key.
//
// Arbitrary and not least-recently-used: an LRU needs an order maintained
// on every read, which is a write on the read path and a lock upgrade to
// go with it. Random eviction is within a small factor of LRU on the
// access patterns a kernel has — a working set of definitions read over
// and over — and it costs one map iteration that stops immediately.
func (c *Cache) evict() {
	if len(c.entries) < c.size {
		return
	}

	for key := range c.entries {
		delete(c.entries, key)

		break
	}
}
