package bbolt

import (
	"context"
	"iter"

	bolt "go.etcd.io/bbolt"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// txn is the store as it looks from inside one transaction.
//
// EVERY WRITE THIS STORE MAKES GOES THROUGH ONE OF THESE. A single Put is
// a transaction of one write, which is what keeps the guarded-write rules
// — the revision comparison, the history, the event — in one place rather
// than in one place per method.
//
// It is not safe for concurrent use and does not have to be: bbolt allows
// one writer, the store holds a lock across the whole transaction, and
// the work inside is the caller's own straight-line code.
type txn struct {
	store *Store
	tx    *bolt.Tx
	// events are what happened, in order, to be delivered once the
	// transaction has committed. Delivering from inside would hand out a
	// change that a rollback then un-made.
	events []kv.Event
}

// Get returns the entry under key, or kv.ErrNotFound.
func (t *txn) Get(ctx context.Context, key kv.Key) (kv.Entry, error) {
	if err := ctx.Err(); err != nil {
		return kv.Entry{}, err
	}

	raw := t.tx.Bucket(entriesBucket).Get(key)
	if raw == nil {
		return kv.Entry{}, kv.ErrNotFound
	}

	return decodeEntry(key, raw)
}

// Scan walks every entry under prefix, in key order.
func (t *txn) Scan(ctx context.Context, prefix kv.Key) iter.Seq2[kv.Entry, error] {
	return func(yield func(kv.Entry, error) bool) {
		cursor := t.tx.Bucket(entriesBucket).Cursor()

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

// Revision is the store-wide revision as of this transaction.
//
// As of the transaction and not as of now: a check inside one is about
// the world the writes will land in, and a number read from outside it
// would already be somebody else's.
func (t *txn) Revision(context.Context) (revision.Revision, error) {
	return readRevision(t.tx.Bucket(metaBucket), revisionKeyName), nil
}

// Put writes value under key if the entry is at the expected revision.
func (t *txn) Put(
	ctx context.Context,
	key kv.Key,
	value []byte,
	expect revision.Revision,
) (revision.Revision, error) {
	if err := ctx.Err(); err != nil {
		return revision.None, err
	}

	entries := t.tx.Bucket(entriesBucket)

	current, found, err := lookup(entries, key)
	if err != nil {
		return revision.None, err
	}

	// Absent means "nothing is there yet"; anything else means "still
	// exactly this". One comparison serves both.
	if found != !expect.IsZero() || (found && !current.Revision.Eq(expect)) {
		return revision.None, conflict(key, expect, current, found)
	}

	at, err := t.next()
	if err != nil {
		return revision.None, err
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
		return revision.None, err
	}

	return at, t.record(kv.Event{Kind: kv.EventPut, Entry: entry})
}

// Delete removes the entry under the same guard as Put.
func (t *txn) Delete(
	ctx context.Context,
	key kv.Key,
	expect revision.Revision,
) (revision.Revision, error) {
	if err := ctx.Err(); err != nil {
		return revision.None, err
	}

	entries := t.tx.Bucket(entriesBucket)

	current, found, err := lookup(entries, key)
	if err != nil {
		return revision.None, err
	}

	if !found {
		return revision.None, kv.ErrNotFound
	}

	if !current.Revision.Eq(expect) {
		return revision.None, conflict(key, expect, current, found)
	}

	at, err := t.next()
	if err != nil {
		return revision.None, err
	}

	if err := entries.Delete(key); err != nil {
		return revision.None, err
	}

	// The event carries the value the entry LAST had. Whoever filters a
	// stream has to be able to ask what went away, and afterwards there
	// is nowhere else to ask.
	departed := current
	departed.Revision = at

	return at, t.record(kv.Event{Kind: kv.EventDelete, Entry: departed})
}

// next takes the revision this write lands at and writes it down.
//
// One per write rather than one per transaction. A transaction spanning
// several revisions is still atomic — a watcher sees all of them or none
// — and the alternative would put several events under one history key,
// which holds one.
func (t *txn) next() (revision.Revision, error) {
	meta := t.tx.Bucket(metaBucket)
	at := readRevision(meta, revisionKeyName).Next()

	return at, meta.Put(revisionKeyName, revisionKey(at))
}

// record logs the event and remembers it for delivery after the commit.
func (t *txn) record(event kv.Event) error {
	if err := t.store.log(t.tx, event); err != nil {
		return err
	}

	t.events = append(t.events, event)

	return nil
}
