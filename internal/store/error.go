package store

import (
	"errors"

	"github.com/graphene-ci/graphene/internal/store/kv"
)

// What the byte layer refuses, restated here so that nothing above has to
// name the byte layer to handle it.
//
// These are aliases and not new sentinels: they ARE the same values, so
// errors.Is matches whichever name the caller reached for. The point is
// not to hide kv — it is that a file deciding what to do about a missing
// resource should not have to import the package that knows about pages
// and prefixes.
//
// The old code is the argument. Its controller imported the store package
// for one constant and nothing else — a byte layer type sitting in the
// middle of a domain event loop, dragging the whole storage vocabulary
// into a file that never touched a byte.
var (
	// ErrNotFound — nothing stored under that id.
	ErrNotFound = kv.ErrNotFound
	// ErrLagged — a watcher fell behind and was dropped. The answer is a
	// fresh snapshot, never a retry.
	ErrLagged = kv.ErrLagged
	// ErrClosed — the store, or this stream, is shut.
	ErrClosed = kv.ErrClosed
)

// ErrNotStorable — a value whose codec could not name it, or named a
// subtree instead of one record.
//
// This is where a codec's Id is held to account. Id returns no error —
// it cannot, since a Store has to be able to build a key for whatever it
// is handed — so a codec that fails to name something returns the zero
// id, and the zero id encodes to a bare kind prefix. Writing there would
// put a value under a key that addresses a whole subtree, and the next
// scan of that subtree would decode a record that is not one.
//
// One check here covers every codec, which is why it is here and not
// repeated in each of them.
var ErrNotStorable = errors.New("value does not name one record")
