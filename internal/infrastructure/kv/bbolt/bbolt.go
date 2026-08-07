// Package bbolt is a kv.Store on a single file.
//
// It passes the same conformance suite the in-memory store does, which is
// what "is a store" means here: the port is an interface and an interface
// only says which methods exist, so everything that matters about one is
// pinned by running it.
//
// Two things about bbolt shape this code, and both are about memory it
// owns. A value handed back by a read is only valid while the transaction
// that read it is open — so everything that leaves is cloned, and the
// clone is not politeness, it is the difference between a value that is
// right and a value that was right. And only one write transaction runs
// at a time, which means a writer never races another writer and the
// revision counter needs no atomics.
//
// It starts no goroutines. Events reach watchers from whoever wrote them,
// after the commit that made them true.
package bbolt

import (
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"sync"

	bolt "go.etcd.io/bbolt"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// The three buckets a store keeps.
//
//	entries  key       → the record under it
//	history  revision  → what happened at it, for watchers catching up
//	meta     name      → the counter, and how far back the log reaches
//
//nolint:gochecknoglobals // a validated value cannot be a const; treated as one
var (
	entriesBucket = []byte("entries")
	historyBucket = []byte("history")
	metaBucket    = []byte("meta")
)

// What the meta bucket holds.
//
//nolint:gochecknoglobals // a validated value cannot be a const; treated as one
var (
	revisionKeyName = []byte("revision")
	fromKeyName     = []byte("from")
)

// defaultHistory is how many events are kept for watchers catching up.
// storeMode is what a kernel's store is: readable by the user that runs
// it and by nobody else. Everything in it — identities, grants, what
// every machine was told to run — is decided by whoever can read it.
const storeMode = 0o600

const defaultHistory = 4096

// Store is a kv.Store on a bbolt file.
var _ kv.Store = (*Store)(nil)

// Store keeps records in a file and the changes to them in a log beside
// it.
type Store struct {
	db *bolt.DB

	// write serializes a write with the delivery that follows it. bbolt
	// already allows one writer at a time; this extends that to cover
	// handing the event out, so two commits cannot reach a watcher in the
	// other order.
	write sync.Mutex

	// mu guards the watcher set and everything inside each watcher.
	mu       sync.Mutex
	watchers map[*watcher]struct{}
	closed   bool

	keep    int
	backlog int
}

// Option configures a store at Open.
type Option func(*Store)

// WithHistory sets how many events are kept for watchers catching up.
func WithHistory(events int) Option {
	return func(s *Store) {
		if events > 0 {
			s.keep = events
		}
	}
}

// WithBacklog sets how many events one watcher may fall behind by before
// it is dropped rather than made a writer's problem.
func WithBacklog(events int) Option {
	return func(s *Store) {
		if events > 0 {
			s.backlog = events
		}
	}
}

// Open opens or creates a store at path.
func Open(path string, options ...Option) (*Store, error) {
	opened, err := bolt.Open(path, storeMode, nil)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}

	store := &Store{
		db:       opened,
		watchers: map[*watcher]struct{}{},
		keep:     defaultHistory,
		backlog:  defaultBacklog,
	}

	for _, option := range options {
		option(store)
	}

	if err := opened.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{entriesBucket, historyBucket, metaBucket} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		_ = opened.Close()

		return nil, fmt.Errorf("prepare %s: %w", path, err)
	}

	return store, nil
}

// Get returns the entry under key, or kv.ErrNotFound.
func (s *Store) Get(ctx context.Context, key kv.Key) (kv.Entry, error) {
	if err := ctx.Err(); err != nil {
		return kv.Entry{}, err
	}

	if s.isClosed() {
		return kv.Entry{}, kv.ErrClosed
	}

	var entry kv.Entry

	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(entriesBucket).Get(key)
		if raw == nil {
			return kv.ErrNotFound
		}

		// Decoded inside the transaction and cloned by the decoder: the
		// bytes above are a page and stop being ours the moment this
		// returns.
		decoded, err := decodeEntry(key, raw)
		if err != nil {
			return err
		}

		entry = decoded

		return nil
	})
	if err != nil {
		return kv.Entry{}, err
	}

	return entry, nil
}

// Put writes value under key if the entry is at the expected revision.
func (s *Store) Put(
	ctx context.Context,
	key kv.Key,
	value []byte,
	expect revision.Revision,
) (revision.Revision, error) {
	return s.commit(ctx, func(tx *bolt.Tx, at revision.Revision) (kv.Event, error) {
		entries := tx.Bucket(entriesBucket)

		current, found, err := lookup(entries, key)
		if err != nil {
			return kv.Event{}, err
		}

		// Absent means "nothing is there yet"; anything else means "still
		// exactly this". One comparison serves both.
		if found != !expect.IsZero() || (found && !current.Revision.Eq(expect)) {
			return kv.Event{}, conflict(key, expect, current, found)
		}

		created := at
		if found {
			created = current.CreatedRevision
		}

		entry := kv.Entry{
			Key:             key.Clone(),
			Value:           append([]byte(nil), value...),
			Revision:        at,
			CreatedRevision: created,
		}

		if err := entries.Put(key, encodeEntry(entry)); err != nil {
			return kv.Event{}, err
		}

		return kv.Event{Kind: kv.EventPut, Entry: entry}, nil
	})
}

// Delete removes the entry under the same guard as Put.
//
// A key that is not there is ErrNotFound and not a conflict: removing
// something that does not exist is a different mistake from removing the
// version somebody else replaced.
func (s *Store) Delete(ctx context.Context, key kv.Key, expect revision.Revision) (revision.Revision, error) {
	return s.commit(ctx, func(tx *bolt.Tx, at revision.Revision) (kv.Event, error) {
		entries := tx.Bucket(entriesBucket)

		current, found, err := lookup(entries, key)
		if err != nil {
			return kv.Event{}, err
		}

		if !found {
			return kv.Event{}, kv.ErrNotFound
		}

		if !current.Revision.Eq(expect) {
			return kv.Event{}, conflict(key, expect, current, found)
		}

		if err := entries.Delete(key); err != nil {
			return kv.Event{}, err
		}

		// The event carries the value the entry LAST had. Whoever filters
		// a stream has to be able to ask what went away, and afterwards
		// there is nowhere else to ask.
		departed := current
		departed.Revision = at

		return kv.Event{Kind: kv.EventDelete, Entry: departed}, nil
	})
}

// Scan walks every entry under prefix, in key order.
//
// The walk happens inside a read transaction, which is what makes it a
// consistent view of the store rather than a series of unrelated reads.
// The cost is that a consumer taking its time holds that transaction
// open, and while bbolt lets writers carry on regardless, the pages the
// scan is reading cannot be reclaimed until it lets go. Break out early
// and the transaction ends with the loop.
func (s *Store) Scan(ctx context.Context, prefix kv.Key) iter.Seq2[kv.Entry, error] {
	return func(yield func(kv.Entry, error) bool) {
		if s.isClosed() {
			yield(kv.Entry{}, kv.ErrClosed)

			return
		}

		tx, err := s.db.Begin(false)
		if err != nil {
			yield(kv.Entry{}, err)

			return
		}

		defer func() { _ = tx.Rollback() }()

		cursor := tx.Bucket(entriesBucket).Cursor()

		for key, raw := cursor.Seek(prefix); key != nil && kv.Key(key).HasPrefix(prefix); key, raw = cursor.Next() {
			if err := ctx.Err(); err != nil {
				yield(kv.Entry{}, err)

				return
			}

			entry, err := decodeEntry(kv.Key(key), raw)
			if err != nil {
				yield(kv.Entry{}, err)

				return
			}

			if !yield(entry, nil) {
				return
			}
		}
	}
}

// Revision is the store-wide revision as of now.
func (s *Store) Revision(ctx context.Context) (revision.Revision, error) {
	if err := ctx.Err(); err != nil {
		return revision.None, err
	}

	if s.isClosed() {
		return revision.None, kv.ErrClosed
	}

	var at revision.Revision

	err := s.db.View(func(tx *bolt.Tx) error {
		at = readRevision(tx.Bucket(metaBucket), revisionKeyName)

		return nil
	})
	if err != nil {
		return revision.None, err
	}

	return at, nil
}

// Close shuts the store and every watcher it handed out.
func (s *Store) Close() error {
	s.mu.Lock()

	if s.closed {
		s.mu.Unlock()

		return nil
	}

	s.closed = true

	for w := range s.watchers {
		w.wake()
	}

	clear(s.watchers)
	s.mu.Unlock()

	return s.db.Close()
}

// commit runs one guarded write and hands the event it produced to every
// watcher it belongs to.
//
// Delivery happens AFTER the commit, never inside it: an event handed out
// from inside a transaction that then rolled back would be a change
// nobody made. The lock spans both so that two commits cannot reach a
// watcher in the other order.
func (s *Store) commit(
	ctx context.Context,
	apply func(tx *bolt.Tx, at revision.Revision) (kv.Event, error),
) (revision.Revision, error) {
	if err := ctx.Err(); err != nil {
		return revision.None, err
	}

	s.write.Lock()
	defer s.write.Unlock()

	if s.isClosed() {
		return revision.None, kv.ErrClosed
	}

	var event kv.Event

	err := s.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket(metaBucket)
		at := readRevision(meta, revisionKeyName).Next()

		produced, err := apply(tx, at)
		if err != nil {
			return err
		}

		if err := meta.Put(revisionKeyName, revisionKey(at)); err != nil {
			return err
		}

		if err := s.log(tx, produced); err != nil {
			return err
		}

		event = produced

		return nil
	})
	if err != nil {
		return revision.None, err
	}

	s.deliver(event)

	return event.Entry.Revision, nil
}

// log appends an event and forgets the oldest once the log is full.
//
// Revisions are contiguous — every write bumps by one — so how many
// events are held is the distance between the newest and the oldest, and
// trimming is one delete per write rather than a count.
func (s *Store) log(tx *bolt.Tx, event kv.Event) error {
	history := tx.Bucket(historyBucket)
	meta := tx.Bucket(metaBucket)

	at := event.Entry.Revision

	if err := history.Put(revisionKey(at), encodeEvent(event)); err != nil {
		return err
	}

	from := readRevision(meta, fromKeyName)
	if from.IsZero() {
		from = at
	}

	// keep is positive: the option refuses anything else and the default
	// is a constant, so there is no negative to become an enormous
	// unsigned number here.
	for at.Uint64()-from.Uint64()+1 > uint64(s.keep) { //nolint:gosec // positive by construction
		if err := history.Delete(revisionKey(from)); err != nil {
			return err
		}

		from = from.Next()
	}

	return meta.Put(fromKeyName, revisionKey(from))
}

// isClosed reports a store that has been shut.
func (s *Store) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closed
}

// lookup reads one entry out of a bucket, saying whether it was there.
func lookup(entries *bolt.Bucket, key kv.Key) (kv.Entry, bool, error) {
	raw := entries.Get(key)
	if raw == nil {
		return kv.Entry{}, false, nil
	}

	entry, err := decodeEntry(key, raw)
	if err != nil {
		return kv.Entry{}, false, err
	}

	return entry, true, nil
}

// readRevision reads a counter out of the meta bucket, treating a missing
// one as the beginning — which is what an empty store is.
func readRevision(meta *bolt.Bucket, name []byte) revision.Revision {
	raw := meta.Get(name)
	if len(raw) < revisionBytes {
		return revision.None
	}

	return revision.Revision(binary.BigEndian.Uint64(raw[:revisionBytes]))
}

// conflict says which of the two ways a guarded write failed. Both are
// revision.ErrConflict; the text is for whoever has to work out why the
// read was stale.
func conflict(key kv.Key, expect revision.Revision, current kv.Entry, found bool) error {
	switch {
	case !found:
		return fmt.Errorf("%w: %s does not exist, expected it at %s",
			revision.ErrConflict, key, expect)

	case expect.IsZero():
		return fmt.Errorf("%w: %s already exists, at %s",
			revision.ErrConflict, key, current.Revision)
	}

	return fmt.Errorf("%w: %s is at %s, expected %s",
		revision.ErrConflict, key, current.Revision, expect)
}
