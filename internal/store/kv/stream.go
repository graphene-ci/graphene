package kv

import "context"

// Stream is a watch, pulled one event at a time.
//
// Pulled and not pushed on purpose. A channel has to be filled by
// somebody, which means a goroutine per watcher started in the middle of
// a call; this has none, and the consumer's own loop — already running,
// already started from where the process was assembled — is what moves
// it.
//
// It also makes the store testable without concurrency at all: write,
// then call Next, and the answer is there. Nothing sleeps waiting for
// another goroutine to wake up.
type Stream interface {
	// Next blocks until the next event, until ctx is done, or until the
	// stream dies.
	//
	// The context is how a caller does anything else at the same time. A
	// consumer that also wants to re-sync every so often gives Next a
	// deadline and treats the expiry as its tick — the context IS the
	// select, so no channel and no goroutine are needed to hold a timer.
	//
	// It returns ErrLagged when this watcher fell far enough behind that
	// the store stopped keeping its events; the answer to that is a fresh
	// snapshot, never a retry.
	Next(ctx context.Context) (Event, error)

	// Close releases the watcher. Calling it twice is not an error.
	Close() error
}
