package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// WatchLoop is THE watch loop: catch up, follow, resume after a reset.
// Everything that follows a kind uses it — the controllers, the token
// index — so the cursor rules exist once.
//
// The rules it encodes, and why they are not obvious:
//
//   - the resume cursor is the SYNC revision, never a delivered event's:
//     catch-up events arrive in key order, so their revisions are not
//     monotonic, and resuming from the last delivered one would skip
//     every entry whose revision happened to be lower;
//   - before the first sync, an interrupted catch-up is redone in full
//     (cursor stays where it was);
//   - a handler error does not end the loop: one poison record must not
//     freeze a whole index. It is reported and the loop moves on.
type WatchLoop struct {
	Store Store
	// Prefix selects what to follow (an encoded key prefix).
	Prefix []byte
	// Handle consumes one event. Errors are reported to OnError and the
	// loop continues.
	Handle func(ctx context.Context, event Event) error
	// OnSync is called every time the catch-up phase completes, with the
	// revision caught up to. Optional.
	OnSync func(revision uint64)
	// OnError observes handler and stream failures. Optional; without it
	// they are silent.
	OnError func(err error)
	// Backoff between re-watches. Zero means one second.
	Backoff time.Duration
	// From starts the first watch at this revision; zero starts with a
	// full snapshot.
	From uint64
}

// Run follows the prefix until ctx is done.
func (l *WatchLoop) Run(ctx context.Context) error {
	cursor := l.From

	for {
		if err := l.follow(ctx, &cursor); err != nil {
			return err
		}

		if ctx.Err() != nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(l.backoff()):
		}
	}
}

// follow consumes one watch stream to its end, advancing the cursor only
// on sync markers and live events.
func (l *WatchLoop) follow(ctx context.Context, cursor *uint64) error {
	events, err := l.Store.Watch(ctx, l.Prefix, *cursor)

	switch {
	case errors.Is(err, ErrCompacted):
		// The log no longer reaches back that far: start over from a full
		// snapshot rather than continuing with a hole.
		l.report(fmt.Errorf("watch resumed from scratch: %w", err))
		*cursor = 0

		return nil
	case err != nil && ctx.Err() != nil:
		return nil
	case err != nil:
		return fmt.Errorf("store: watch: %w", err)
	}

	for event := range events {
		if event.Type == EventSync {
			*cursor = event.StoreRevision

			if l.OnSync != nil {
				l.OnSync(event.StoreRevision)
			}

			continue
		}

		if err := l.Handle(ctx, event); err != nil {
			l.report(err)

			continue
		}

		if event.StoreRevision > *cursor {
			*cursor = event.StoreRevision
		}
	}

	return nil
}

func (l *WatchLoop) report(err error) {
	if l.OnError != nil {
		l.OnError(err)
	}
}

func (l *WatchLoop) backoff() time.Duration {
	if l.Backoff <= 0 {
		return time.Second
	}

	return l.Backoff
}
