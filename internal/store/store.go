// Package store is the typed layer over the byte layer: it is where a
// value becomes bytes and an id becomes a key, and it is the only place
// either of those happens.
//
// Above it nothing knows what a key looks like. Below it nothing knows
// what a resource is. The translation between the two lives here once,
// which is what lets the byte layer stay swappable and the domain stay
// ignorant of storage.
package store

import (
	"context"
	"fmt"
	"iter"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Store keeps values of one type.
//
// It is a value: it holds a byte store and a codec, neither of which it
// ever replaces, so copying one costs two words and there is no nil to
// guard against at every method.
//
// There is no Close, on purpose. Several typed stores sit on one byte
// store — resources and definitions at least — and a Close here would let
// whichever of them finished first shut the ground out from under the
// others. The byte store is closed by whoever opened it.
type Store[T any] struct {
	// bytes is a Tx and not a Store, which is what lets the same typed
	// store be built over a transaction: inside one, a typed view is the
	// same code reading and writing the same way, through the moment
	// rather than through the store.
	bytes kv.Tx
	codec Codec[T]
}

// New puts a codec on top of a byte store, or on one transaction of it.
func New[T any](bytes kv.Tx, codec Codec[T]) Store[T] {
	return Store[T]{bytes: bytes, codec: codec}
}

// Get reads one value, or ErrNotFound.
func (s Store[T]) Get(ctx context.Context, id resource.Id) (Value[T], error) {
	entry, err := s.bytes.Get(ctx, KeyOf(id))
	if err != nil {
		return Value[T]{}, fmt.Errorf("%s: %w", id, err)
	}

	return s.value(entry)
}

// Put writes a value if the record is at the expected revision —
// revision.Absent to demand that nothing is there yet.
//
// The id is taken from the value rather than passed alongside it, so
// there is no way to write one thing under another thing's key.
func (s Store[T]) Put(ctx context.Context, value T, expect revision.Revision) (revision.Revision, error) {
	id := s.codec.Id(value)
	if id.IsZero() || !id.IsExact() {
		return revision.None, fmt.Errorf("%w: %s", ErrNotStorable, id)
	}

	raw, err := s.codec.Encode(value)
	if err != nil {
		return revision.None, fmt.Errorf("%s: encode: %w", id, err)
	}

	at, err := s.bytes.Put(ctx, KeyOf(id), raw, expect)
	if err != nil {
		return revision.None, fmt.Errorf("%s: %w", id, err)
	}

	return at, nil
}

// Delete removes a record under the same guard as Put.
func (s Store[T]) Delete(ctx context.Context, id resource.Id, expect revision.Revision) (revision.Revision, error) {
	at, err := s.bytes.Delete(ctx, KeyOf(id), expect)
	if err != nil {
		return revision.None, fmt.Errorf("%s: %w", id, err)
	}

	return at, nil
}

// Scan walks everything under an id, in key order. An id with fewer path
// values than its shape has positions is a subtree, which is what makes
// this the same call as reading one thing.
//
// A value that will not decode stops the walk rather than being skipped:
// a store that quietly hides records it cannot read is a store that
// answers "no" to a question it did not understand.
func (s Store[T]) Scan(ctx context.Context, prefix resource.Id) iter.Seq2[Value[T], error] {
	return func(yield func(Value[T], error) bool) {
		for entry, err := range s.bytes.Scan(ctx, KeyOf(prefix)) {
			if err != nil {
				yield(Value[T]{}, fmt.Errorf("%s: %w", prefix, err))

				return
			}

			if !yield(s.value(entry)) {
				return
			}
		}
	}
}

// Watch follows changes under an id. It delivers no snapshot; see
// kv.Store.Watch for why, and for the three lines that take one.
// A watch cannot be opened on a store bound to a transaction, and the
// refusal says so: a transaction is a moment, and a watch is the stream
// of moments after one.
func (s Store[T]) Watch(
	ctx context.Context,
	prefix resource.Id,
	after revision.Revision,
) (Stream[T], error) {
	watchable, can := s.bytes.(kv.Store)
	if !can {
		return Stream[T]{}, ErrNoWatchInTransaction
	}

	stream, err := watchable.Watch(ctx, KeyOf(prefix), after)
	if err != nil {
		return Stream[T]{}, fmt.Errorf("%s: %w", prefix, err)
	}

	return Stream[T]{stream: stream, codec: s.codec}, nil
}

// Revision is the store-wide revision as of now: the cursor to take
// before a snapshot.
func (s Store[T]) Revision(ctx context.Context) (revision.Revision, error) {
	return s.bytes.Revision(ctx)
}

// value decodes one entry into the value plus the two stamps the store
// keeps about it.
func (s Store[T]) value(entry kv.Entry) (Value[T], error) {
	decoded, err := s.codec.Decode(entry.Value)
	if err != nil {
		return Value[T]{}, fmt.Errorf("%s: decode: %w", entry.Key, err)
	}

	return Value[T]{
		Value:           decoded,
		Revision:        entry.Revision,
		CreatedRevision: entry.CreatedRevision,
	}, nil
}
