package kv

import "errors"

// What the byte layer refuses on its own account. The two failures that
// are ABOUT a revision — a CAS that did not hold, history that no longer
// reaches back — are revision.ErrConflict and revision.ErrCompacted, and
// they live beside the revision rather than being restated here: a caller
// matching on them should not have to know which store raised them.
var (
	// ErrNotFound — nothing under that key.
	ErrNotFound = errors.New("kv: no entry under key")

	// ErrLagged — this watcher fell far enough behind that the store
	// stopped keeping its events, and dropped it rather than making a
	// writer wait for a reader.
	//
	// Retrying is not the answer and never becomes one: the events are
	// gone. The answer is a fresh cursor and a fresh snapshot.
	ErrLagged = errors.New("kv: watcher fell behind and was dropped")

	// ErrClosed — the store, or this stream, is shut. Nothing about it
	// improves by asking again.
	ErrClosed = errors.New("kv: store is closed")
)
