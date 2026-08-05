package store

import (
	"context"
	"fmt"

	"github.com/graphene-ci/graphene/internal/store/kv"
)

// Stream is a watch with a codec on it, pulled one event at a time.
type Stream[T any] struct {
	stream kv.Stream
	codec  Codec[T]
}

// Next blocks until the next event, until ctx is done, or until the
// stream dies. Give it a deadline to do anything else at the same time:
// the context is the select, so holding a timer needs no goroutine.
func (s Stream[T]) Next(ctx context.Context) (Event[T], error) {
	event, err := s.stream.Next(ctx)
	if err != nil {
		return Event[T]{}, err
	}

	value, err := s.codec.Decode(event.Entry.Value)
	if err != nil {
		return Event[T]{}, fmt.Errorf("%s: decode: %w", event.Entry.Key, err)
	}

	return Event[T]{
		Kind: event.Kind,
		Value: Value[T]{
			Value:           value,
			Revision:        event.Entry.Revision,
			CreatedRevision: event.Entry.CreatedRevision,
		},
	}, nil
}

// Close releases the watcher. Calling it twice is not an error.
func (s Stream[T]) Close() error { return s.stream.Close() }
