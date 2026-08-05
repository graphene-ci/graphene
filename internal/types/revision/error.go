package revision

import "errors"

// The two ways an operation that names a revision fails, plus the one way
// a revision fails to be read at all.
//
// The first two are refusals BY the store, so an argument could be made
// for keeping them there. They live here instead because everyone matches
// on them and almost nobody imports the store: the client retries a
// conflict, the controller restarts a watch that was compacted, the CLI
// tells a person which of the two happened. Putting them beside the type
// they are about is what keeps those callers from each inventing their
// own way of asking.
var (
	// ErrConflict — the record is not at the revision the caller expected.
	// Somebody else wrote in between, so the caller is holding a stale
	// read and whatever it decided from that read has to be decided again.
	//
	// This is not a failure of the store; it is the store doing the one
	// job compare-and-swap exists for. The right answer to it is almost
	// always to re-read and retry, never to force the write.
	ErrConflict = errors.New("revision conflict")

	// ErrCompacted — the revision asked for is older than the oldest the
	// store still keeps. History is not infinite, and a watcher that was
	// away for longer than the store's memory cannot be told what it
	// missed.
	//
	// A caller cannot retry its way out of this. It has to start over from
	// Beginning and take a fresh snapshot, having accepted that it will
	// never learn the individual changes it slept through.
	ErrCompacted = errors.New("revision compacted")

	// ErrMalformed — text handed to Parse that is not a revision.
	ErrMalformed = errors.New("malformed revision")
)
