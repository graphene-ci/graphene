// Package memory is a kv.Store that keeps everything in memory.
//
// It is the reference implementation and the one every test of anything
// above the byte layer runs on. Being the reference is a job: it is the
// store the conformance suite is written against, so it has to be
// obviously correct rather than fast, and it has nothing that could be
// accidentally right — no pages, no cursors, no transactions to hide a
// mistake behind.
//
// It starts no goroutines. Events are handed to watchers by whoever
// WRITES them, under the same lock that commits the write, into a queue
// each watcher drains at its own pace. A writer therefore never waits for
// a reader: a watcher that falls too far behind is dropped where it
// stands and told so.
package memory

import (
	"context"
	"iter"
	"maps"
	"slices"
	"sync"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Store is a kv.Store. Asserted here rather than discovered at the one
// place it is wired, so that a method drifting out of the port fails in
// this package.
var _ kv.Store = (*Store)(nil)

// defaultHistory is how many events a store keeps for watchers that are
// catching up. Beyond it the oldest are forgotten and a watcher asking
// for them is told to take a fresh snapshot.
const defaultHistory = 1024

// Store keeps entries in a map and the changes to them in a slice.
//
// A pointer type, and one of the few: it has a mutex, a counter and a set
// of live watchers, all of which are state something else observes.
type Store struct {
	mu sync.Mutex

	revision revision.Revision
	entries  map[string]kv.Entry

	// history is the change log watchers replay from, oldest first, and
	// oldest is what falls off when it is full. from is the revision the
	// remaining log still reaches back to; anything older is compacted.
	history []kv.Event
	from    revision.Revision
	keep    int
	backlog int

	watchers map[*watcher]struct{}
	closed   bool
}

// Option configures a store at construction. There are few and they are
// all about limits, which is why they are options rather than a struct
// somebody has to fill in to say "the usual".
type Option func(*Store)

// WithHistory sets how many events are kept for watchers catching up.
//
// Small values are how a test reaches the compaction path without
// writing a thousand records first; it is not otherwise a knob worth
// turning.
func WithHistory(events int) Option {
	return func(s *Store) {
		if events > 0 {
			s.keep = events
		}
	}
}

// WithBacklog sets how many events one watcher may fall behind by before
// it is dropped.
func WithBacklog(events int) Option {
	return func(s *Store) {
		if events > 0 {
			s.backlog = events
		}
	}
}

// New opens an empty store.
func New(options ...Option) *Store {
	store := &Store{
		entries:  map[string]kv.Entry{},
		from:     revision.Beginning,
		keep:     defaultHistory,
		backlog:  defaultBacklog,
		watchers: map[*watcher]struct{}{},
	}

	for _, option := range options {
		option(store)
	}

	return store
}

// Get returns the entry under key, or kv.ErrNotFound.
func (s *Store) Get(ctx context.Context, key kv.Key) (kv.Entry, error) {
	if err := ctx.Err(); err != nil {
		return kv.Entry{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return kv.Entry{}, kv.ErrClosed
	}

	entry, found := s.entries[string(key)]
	if !found {
		return kv.Entry{}, kv.ErrNotFound
	}

	return entry.Clone(), nil
}

// Put writes value under key if the entry is at the expected revision.
func (s *Store) Put(
	ctx context.Context,
	key kv.Key,
	value []byte,
	expect revision.Revision,
) (revision.Revision, error) {
	return s.one(ctx, func(tx *txn) (revision.Revision, error) {
		return tx.Put(ctx, key, value, expect)
	})
}

// Delete removes the entry under the same guard as Put.
//
// A key that is not there is ErrNotFound and not a conflict: the caller
// asked to remove something that does not exist, which is a different
// mistake from asking to remove the version somebody else replaced.
func (s *Store) Delete(
	ctx context.Context,
	key kv.Key,
	expect revision.Revision,
) (revision.Revision, error) {
	return s.one(ctx, func(tx *txn) (revision.Revision, error) {
		return tx.Delete(ctx, key, expect)
	})
}

// one runs a single guarded write as the transaction it already is.
//
// Every write this store makes goes through Do, which is what keeps the
// guarded-write rules — the revision comparison, the history, the event —
// in one place rather than in one place per method.
func (s *Store) one(
	ctx context.Context,
	write func(tx *txn) (revision.Revision, error),
) (revision.Revision, error) {
	var at revision.Revision

	err := s.Do(ctx, func(tx kv.Tx) error {
		inside, ok := tx.(*txn)
		if !ok {
			// Do hands back its own transaction. Anything else is a
			// store that has been wrapped in a way this cannot see
			// through, and guessing would write through a wrapper that
			// exists to be in the way.
			return kv.ErrClosed
		}

		var err error

		at, err = write(inside)

		return err
	})
	if err != nil {
		return revision.None, err
	}

	return at, nil
}

// Scan walks every entry under prefix, in key order.
//
// The matching entries are collected under the lock and walked after it
// is released, so a scan is a consistent snapshot and a slow consumer
// blocks nobody. That costs a copy of what matched, which for a store
// that holds everything in memory anyway is not a cost worth avoiding.
func (s *Store) Scan(ctx context.Context, prefix kv.Key) iter.Seq2[kv.Entry, error] {
	return func(yield func(kv.Entry, error) bool) {
		matched, err := s.snapshot(prefix)
		if err != nil {
			yield(kv.Entry{}, err)

			return
		}

		for _, entry := range matched {
			if err := ctx.Err(); err != nil {
				yield(kv.Entry{}, err)

				return
			}

			if !yield(entry, nil) {
				return
			}
		}
	}
}

// snapshot copies out everything under prefix, in key order.
func (s *Store) snapshot(prefix kv.Key) ([]kv.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, kv.ErrClosed
	}

	return s.walk(prefix), nil
}

// walk is snapshot with the lock already held, which is how it is reached
// from inside a transaction. Splitting it is not a nicety: Do holds the
// lock for the whole of the work, and a scan that took it again would
// deadlock on the store's own transaction.
func (s *Store) walk(prefix kv.Key) []kv.Entry {
	keys := slices.Sorted(maps.Keys(s.entries))
	matched := make([]kv.Entry, 0, len(keys))

	for _, key := range keys {
		entry := s.entries[key]
		if entry.Key.HasPrefix(prefix) {
			matched = append(matched, entry.Clone())
		}
	}

	return matched
}

// Revision is the store-wide revision as of now.
func (s *Store) Revision(ctx context.Context) (revision.Revision, error) {
	if err := ctx.Err(); err != nil {
		return revision.None, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return revision.None, kv.ErrClosed
	}

	return s.revision, nil
}

// Close shuts the store and every watcher it handed out.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	for w := range s.watchers {
		w.wake()
	}

	clear(s.watchers)

	return nil
}

// record appends an event to the log and hands it to every watcher it
// belongs to.
//
// Called with the lock held, by whoever wrote it. That is the whole of
// the delivery machinery: no goroutine, no channel of events, and no
// window in which a writer has committed but a watcher has not been told.
func (s *Store) record(event kv.Event) {
	s.history = append(s.history, event)

	if len(s.history) > s.keep {
		dropped := len(s.history) - s.keep
		s.history = slices.Delete(s.history, 0, dropped)
		s.from = s.history[0].Entry.Revision
	}

	for w := range s.watchers {
		w.offer(event)
	}
}
