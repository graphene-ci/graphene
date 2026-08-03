package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/store"
)

// ErrRestart tells the loop that the cursor it holds is no longer usable
// and the whole prefix must be read again from scratch. A stream reports
// it however its own world says so — a compacted store revision here, a
// gRPC status a link away — and the loop treats both the same.
var ErrRestart = errors.New("controller: watch must restart from scratch")

// Event is one change to one resource. Deletes carry the record's final
// state (prev_kv semantics), so a controller always has something to
// clean up after.
type Event struct {
	Type     store.EventType
	Resource *graphenepbv1.Resource
	// StoreRevision is the cursor space; on a sync marker it is the
	// revision everything up to has been delivered.
	StoreRevision uint64
}

// Stream opens a watch at a revision and delivers events until it ends.
//
// This is the seam that lets a controller run anywhere. The truth may be
// in this process's store or a link away on another machine; the loop
// below cannot tell, and neither can whoever writes the controller.
type Stream func(ctx context.Context, from uint64) (<-chan Event, error)

// Loop follows a stream: catch up, follow, resume after a reset.
// Everything that watches uses it, so the cursor rules exist once.
//
// The rules, and why they are not obvious:
//
//   - the resume cursor is the SYNC revision, never a delivered event's:
//     catch-up events arrive in key order, so their revisions are not
//     monotonic, and resuming from the last delivered one would skip
//     every entry whose revision happened to be lower;
//   - before the first sync, an interrupted catch-up is redone in full
//     (the cursor stays where it was);
//   - a handler error does not end the loop: one poison record must not
//     freeze a whole controller. It is reported and the loop moves on.
type Loop struct {
	Stream Stream
	// Handle consumes one event, sequentially, in revision order.
	Handle func(ctx context.Context, event Event) error
	// OnSync is called whenever the catch-up phase completes, with the
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

// Run follows the stream until ctx is done.
func (l *Loop) Run(ctx context.Context) error {
	cursor := l.From

	for {
		if err := l.follow(ctx, &cursor); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(l.backoff()):
		}
	}
}

// follow consumes one stream to its end, advancing the cursor only on
// sync markers and live events.
func (l *Loop) follow(ctx context.Context, cursor *uint64) error {
	events, err := l.Stream(ctx, *cursor)

	switch {
	case errors.Is(err, ErrRestart):
		// What we hold no longer reaches back far enough: start over from
		// a full snapshot rather than continue with a hole. Reported, not
		// returned — the loop recovers by itself.
		l.report(err)

		*cursor = 0

		return nil
	case err != nil && ctx.Err() != nil:
		return nil //nolint:nilerr // the watch was cancelled
	case err != nil:
		return fmt.Errorf("controller: watch: %w", err)
	}

	for event := range events {
		if event.Type == store.EventSync {
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

func (l *Loop) report(err error) {
	if l.OnError != nil {
		l.OnError(err)
	}
}

func (l *Loop) backoff() time.Duration {
	if l.Backoff <= 0 {
		return time.Second
	}

	return l.Backoff
}
