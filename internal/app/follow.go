package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// resync is how often Follow looks again when nothing has happened.
//
// Not a poll: the watch delivers changes as they land, and this only
// bounds how long a MISSED one could go unnoticed — after a stream is
// dropped, or after a write that arrived while the kernel was starting.
// It is expressed as a deadline on Next rather than as a timer beside it,
// which is what lets the whole loop be one goroutine and no channels.
const resync = 30 * time.Second

// Follow keeps the kernel's configuration up to date with its own record.
//
// A kernel watching its own store is the first real consumer of the
// watch, and it exercises the shape the watch was given: the cursor is
// taken FIRST, the snapshot second, the changes third. A write that lands
// between the cursor and the snapshot is seen twice, and applying the
// same configuration twice is nothing.
//
// It runs until ctx is done and returns why. It is a LOOP and not a
// goroutine: nothing below main starts one, so this is something main
// runs, and the concurrency of a kernel stays a list somebody can read.
func (a *App) Follow(ctx context.Context) error {
	for {
		if err := a.follow(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			// A dropped stream is not a failure worth stopping for. The
			// answer to every reason a watch ends — compacted history, a
			// watcher that fell behind, a store that closed under it — is
			// the same: take a fresh cursor and a fresh snapshot.
			if errors.Is(err, store.ErrClosed) {
				return err
			}

			continue
		}
	}
}

// follow takes one cursor, one snapshot, and then reads until the stream
// ends.
func (a *App) follow(ctx context.Context) error {
	id, err := KernelId(a.bootstrap.Name)
	if err != nil {
		return err
	}

	at, err := a.kernel.Revision(ctx)
	if err != nil {
		return err
	}

	stored, err := a.kernel.Get(ctx, id)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}

	if err == nil {
		a.hold(ConfigFrom(stored.Value.Spec()))
	}

	stream, err := a.kernel.Watch(ctx, id, at)
	if err != nil {
		return fmt.Errorf("watch %s: %w", id, err)
	}

	defer func() { _ = stream.Close() }()

	return a.read(ctx, stream)
}

// read pulls events until the stream ends or the context does.
//
// The re-sync is a DEADLINE on the read rather than a timer running
// beside it. That is what the pulled watch bought: a consumer that wants
// to do something else at the same time says so in the context, and needs
// neither a second channel nor a second goroutine to hold it.
func (a *App) read(ctx context.Context, stream store.Stream[resource.Resource]) error {
	for {
		waiting, cancel := context.WithTimeout(ctx, resync)
		event, err := stream.Next(waiting)

		cancel()

		switch {
		case err == nil:
			a.apply(event)

		case errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil:
			// Nothing happened for a while. Start over from a fresh
			// cursor, which costs one read and catches anything a dropped
			// stream would otherwise have swallowed.
			return nil

		default:
			return err
		}
	}
}

// apply takes what an event says the configuration is now.
//
// A delete leaves the last configuration in place rather than reverting
// to defaults. Somebody removing the record is not asking the kernel to
// re-bind to a different address; the record will be recreated on the
// next start, and until then what is running is what was asked for.
func (a *App) apply(event store.Event[resource.Resource]) {
	if event.Kind != store.EventPut {
		return
	}

	config := ConfigFrom(event.Value.Value.Spec())
	if config.Eq(a.Config()) {
		return
	}

	a.hold(config)
}
