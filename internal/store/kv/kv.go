// Package kv is the byte layer: keys, values, and the one property
// everything above is built out of — a shorter key is a byte prefix of
// everything beneath it.
//
// It knows nothing of kinds, paths or schemas, and that is what makes it
// swappable: bbolt on one machine, etcd across several, and the same
// conformance suite decides whether either of them is a store. A cache is
// a Store wrapping a Store, so a cache that passes that suite is a
// correct cache.
//
// Nothing here starts a goroutine. A watcher is pulled rather than
// pushed, which is the difference between "the store spawns one goroutine
// per watcher, somewhere in the middle of a call" and "every goroutine in
// the process was started where the process was assembled". The second is
// debuggable in one place.
package kv

import (
	"context"
	"iter"

	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Store is the port. All methods are safe for concurrent use.
type Store interface {
	// Get returns the entry under key, or ErrNotFound.
	Get(ctx context.Context, key Key) (Entry, error)

	// Put writes value under key if the entry is at the expected
	// revision — revision.Absent to demand that nothing is there yet.
	// It returns the revision of the write, or revision.ErrConflict.
	Put(ctx context.Context, key Key, value []byte, expect revision.Revision) (revision.Revision, error)

	// Delete removes the entry under the same guard as Put.
	Delete(ctx context.Context, key Key, expect revision.Revision) (revision.Revision, error)

	// Scan walks every entry whose key starts with prefix, in key order.
	//
	// An iterator and not a page-and-cursor pair: a scan is finite and
	// does not block, so paging is the implementation's business and has
	// no reason to be in the signature. Stop early by breaking out.
	Scan(ctx context.Context, prefix Key) iter.Seq2[Entry, error]

	// Watch follows changes under prefix, starting after the given
	// revision.
	//
	// It does NOT deliver a snapshot, and this is the whole reason it
	// looks like this. A watch that begins with a snapshot has to deliver
	// it as synthetic events, whose revisions are each key's last write
	// and therefore NOT in order — so resuming from a delivered event's
	// revision silently loses whatever sorted after it. That trap used to
	// live in a comment. Here it cannot be written:
	//
	//	at, _ := store.Revision(ctx)              // the cursor FIRST
	//	for entry := range store.Scan(ctx, p) {}  // then the snapshot
	//	stream, _ := store.Watch(ctx, p, at)      // then the changes
	//
	// Taking the revision before the scan means a write in between is
	// seen twice rather than not at all, and a reconciling consumer is
	// built to be told twice.
	//
	// It returns revision.ErrCompacted if history no longer reaches back
	// that far — at the call, not somewhere in the middle of the stream.
	Watch(ctx context.Context, prefix Key, after revision.Revision) (Stream, error)

	// Revision is the store-wide revision as of now: the cursor to take
	// before a snapshot.
	Revision(ctx context.Context) (revision.Revision, error)

	// Close releases the store. Streams handed out before it are dead
	// afterwards and say so through Next.
	Close() error
}
