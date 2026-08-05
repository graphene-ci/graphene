package codec

import "errors"

// Why a stored value would not read back. All four mean the same thing to
// a caller — this record is not usable — and different things to whoever
// has to find out why, which is the reason they are four and not one.
var (
	// ErrTruncated — fewer bytes than the tag itself. Nothing written by
	// this package is that short, so the value was cut.
	ErrTruncated = errors.New("stored value is too short to be tagged")

	// ErrForeign — the bytes do not begin the way ours do. Somebody
	// else's value, or a read from the wrong place: either way not a
	// record of ours that went wrong, which is a different search.
	ErrForeign = errors.New("stored value was not written by graphene")

	// ErrFormat — ours, but laid out by a version this build does not
	// read. A migration, not a corruption.
	ErrFormat = errors.New("stored value is in an unknown format")

	// ErrDecode — the tag was right and the message underneath was not.
	// Real corruption, or a bug in whatever wrote it.
	ErrDecode = errors.New("stored value does not decode")

	// ErrEncode — a value that would not marshal. Unreachable through the
	// domain types, which cannot hold anything protobuf refuses; kept
	// because "cannot happen" is not the same as "need not be handled".
	ErrEncode = errors.New("value does not encode")
)
