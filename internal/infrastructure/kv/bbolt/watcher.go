package bbolt

import (
	"context"

	bolt "go.etcd.io/bbolt"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// defaultBacklog is how many events one watcher may fall behind by before
// it is dropped rather than made a writer's problem.
const defaultBacklog = 256

// watcher is one live watch: a queue somebody else fills and this one
// drains.
//
// Its fields are guarded by the STORE's mutex, not one of its own. A
// writer touches every watcher under a lock it is already holding, so a
// second lock would buy nothing and would introduce an order to get
// wrong.
type watcher struct {
	store  *Store
	prefix kv.Key

	queue  []kv.Event
	signal chan struct{}

	lagged bool
	closed bool
}

// Watch follows changes under prefix, starting after the given revision.
//
// It delivers no snapshot. Take the revision first, scan second, watch
// third: a write in between is then seen twice rather than not at all,
// and a reconciling consumer is built to be told twice.
//
// The replay reads the log inside one transaction and copies out what it
// needs, so the watcher owns its queue and holds nothing of the file.
func (s *Store) Watch(
	ctx context.Context,
	prefix kv.Key,
	after revision.Revision,
) (kv.Stream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if s.isClosed() {
		return nil, kv.ErrClosed
	}

	w := &watcher{
		store:  s,
		prefix: prefix.Clone(),
		signal: make(chan struct{}, 1),
	}

	// Under the write lock, so that the replay and the registration cannot
	// straddle a commit: an event landing between the two would otherwise
	// be in neither.
	s.write.Lock()
	defer s.write.Unlock()

	if err := s.db.View(func(tx *bolt.Tx) error {
		history := tx.Bucket(historyBucket)

		// The log reaches back to `from`. The first event this watcher
		// needs is the one after `after`, so it can be served only if that
		// one is still there.
		from := readRevision(tx.Bucket(metaBucket), fromKeyName)
		if !from.IsZero() && after.Next().Before(from) {
			return revision.ErrCompacted
		}

		cursor := history.Cursor()

		for at, raw := cursor.Seek(revisionKey(after.Next())); at != nil; at, raw = cursor.Next() {
			event, err := decodeEvent(raw)
			if err != nil {
				return err
			}

			if event.Entry.Key.HasPrefix(prefix) {
				w.queue = append(w.queue, event)
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, kv.ErrClosed
	}

	s.watchers[w] = struct{}{}

	return w, nil
}

// deliver hands one committed event to every watcher it belongs to.
//
// Called after the commit and never inside it: an event handed out from a
// transaction that then rolled back would be a change nobody made.
func (s *Store) deliver(event kv.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for w := range s.watchers {
		w.offer(event)
	}
}

// Next blocks until the next event, until ctx is done, or until the
// stream dies.
//
// Nothing runs on this watcher's behalf while it waits. The caller's own
// goroutine sits on a signal, and a caller that also wants a timer gives
// ctx a deadline and treats the expiry as its tick.
func (w *watcher) Next(ctx context.Context) (kv.Event, error) {
	for {
		event, err, wait := w.take()
		if !wait {
			return event, err
		}

		select {
		case <-w.signal:
		case <-ctx.Done():
			return kv.Event{}, ctx.Err()
		}
	}
}

// take pops one event, or reports that there is nothing to pop yet.
//
// Split out so the lock is released before the wait: holding it across a
// select would stop every writer until somebody read.
func (w *watcher) take() (kv.Event, error, bool) {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()

	switch {
	case w.closed:
		return kv.Event{}, kv.ErrClosed, false

	case w.lagged:
		// Said once and then again on every call: the events are gone and
		// no amount of asking brings them back.
		return kv.Event{}, kv.ErrLagged, false

	case len(w.queue) > 0:
		// Drained before the store's own closure is reported, so a watch
		// that was keeping up loses nothing to a shutdown.
		event := w.queue[0]
		w.queue = w.queue[1:]

		return event, nil, false

	case w.store.closed:
		return kv.Event{}, kv.ErrClosed, false
	}

	return kv.Event{}, nil, true
}

// offer hands one event to this watcher, or drops the watcher.
//
// A queue past the backlog means this one is not keeping up, and the
// choice is then to make the writer wait or to let the watcher go. It
// goes: a writer that waits for a reader is a store one slow client can
// stop.
func (w *watcher) offer(event kv.Event) {
	if w.closed || w.lagged || !event.Entry.Key.HasPrefix(w.prefix) {
		return
	}

	if len(w.queue) >= w.store.backlog {
		w.lagged = true
		w.queue = nil
	} else {
		w.queue = append(w.queue, event)
	}

	w.wake()
}

// wake nudges a waiting Next without ever blocking. The signal carries no
// information — the queue does — so one pending nudge is as good as ten.
func (w *watcher) wake() {
	select {
	case w.signal <- struct{}{}:
	default:
	}
}

// Close releases the watcher. Calling it twice is not an error.
func (w *watcher) Close() error {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true
	w.queue = nil

	delete(w.store.watchers, w)
	w.wake()

	return nil
}
