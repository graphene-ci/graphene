package memory

import (
	"context"
	"iter"
	"slices"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Do runs reads and writes as one change.
//
// The lock is held across the whole of it, which is the whole mechanism:
// nothing else can read or write the maps while the work runs, so what it
// sees is what it is about to change.
//
// Failure is undone rather than prevented. Every write remembers what was
// there before, and an error walks that back — including the revisions,
// which are rewound so a rolled-back transaction consumes none. Copying
// the store to restore it would be simpler to write and would cost the
// size of the store on every write.
//
// Events reach watchers only after the work has returned without error,
// for the reason they do everywhere: a change handed out and then undone
// is a change nobody made.
func (s *Store) Do(ctx context.Context, work func(tx kv.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return kv.ErrClosed
	}

	inside := &txn{store: s, was: s.revision}

	if err := work(inside); err != nil {
		inside.undo()

		return err
	}

	for _, event := range inside.events {
		s.record(event)
	}

	return nil
}

// txn is the store as it looks from inside one transaction.
type txn struct {
	store  *Store
	events []kv.Event
	// undone is what to put back, newest first, if the work fails.
	undone []undo
	// was is the revision to rewind to.
	was revision.Revision
}

// undo is one key as it was before this transaction touched it.
type undo struct {
	key   string
	entry kv.Entry
	found bool
}

func (t *txn) Get(ctx context.Context, key kv.Key) (kv.Entry, error) {
	if err := ctx.Err(); err != nil {
		return kv.Entry{}, err
	}

	entry, found := t.store.entries[string(key)]
	if !found {
		return kv.Entry{}, kv.ErrNotFound
	}

	return entry.Clone(), nil
}

func (t *txn) Scan(ctx context.Context, prefix kv.Key) iter.Seq2[kv.Entry, error] {
	return func(yield func(kv.Entry, error) bool) {
		for _, entry := range t.store.walk(prefix) {
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

func (t *txn) Revision(context.Context) (revision.Revision, error) {
	return t.store.revision, nil
}

func (t *txn) Put(
	ctx context.Context,
	key kv.Key,
	value []byte,
	expect revision.Revision,
) (revision.Revision, error) {
	if err := ctx.Err(); err != nil {
		return revision.None, err
	}

	current, found := t.store.entries[string(key)]

	// Absent means "nothing is there yet"; anything else means "still
	// exactly this". Both are the same question asked of different
	// answers, so both are one comparison.
	if found != !expect.IsZero() || (found && !current.Revision.Eq(expect)) {
		return revision.None, conflict(key, expect, current, found)
	}

	t.remember(string(key), current, found)
	t.store.revision = t.store.revision.Next()

	created := t.store.revision
	if found {
		created = current.CreatedRevision
	}

	entry := kv.Entry{
		Key:             key.Clone(),
		Value:           slices.Clone(value),
		Revision:        t.store.revision,
		CreatedRevision: created,
	}

	t.store.entries[string(key)] = entry
	t.events = append(t.events, kv.Event{Kind: kv.EventPut, Entry: entry})

	return t.store.revision, nil
}

func (t *txn) Delete(
	ctx context.Context,
	key kv.Key,
	expect revision.Revision,
) (revision.Revision, error) {
	if err := ctx.Err(); err != nil {
		return revision.None, err
	}

	current, found := t.store.entries[string(key)]
	if !found {
		return revision.None, kv.ErrNotFound
	}

	if !current.Revision.Eq(expect) {
		return revision.None, conflict(key, expect, current, found)
	}

	t.remember(string(key), current, found)
	t.store.revision = t.store.revision.Next()

	delete(t.store.entries, string(key))

	// The event carries the value the entry LAST had. Whoever filters a
	// stream has to be able to ask what went away, and afterwards there
	// is nowhere else to ask.
	departed := current.Clone()
	departed.Revision = t.store.revision

	t.events = append(t.events, kv.Event{Kind: kv.EventDelete, Entry: departed})

	return t.store.revision, nil
}

// remember writes down what a key was, once. Only the FIRST state matters:
// undoing a key written twice puts back what was there before the
// transaction, not what it was in the middle of it.
func (t *txn) remember(key string, entry kv.Entry, found bool) {
	if slices.ContainsFunc(t.undone, func(seen undo) bool { return seen.key == key }) {
		return
	}

	t.undone = append(t.undone, undo{key: key, entry: entry.Clone(), found: found})
}

// undo puts the store back the way it was.
func (t *txn) undo() {
	for _, was := range t.undone {
		if was.found {
			t.store.entries[was.key] = was.entry

			continue
		}

		delete(t.store.entries, was.key)
	}

	t.store.revision = t.was
}
