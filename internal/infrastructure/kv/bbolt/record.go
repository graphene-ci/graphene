package bbolt

import (
	"encoding/binary"
	"fmt"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// How a record is laid out inside a page.
//
// This encoding is hand-written, which everything above the byte layer is
// not, and the difference is the point: it never leaves this file. Domain
// values are encoded by a codec because they travel — to another store,
// over a wire, into a version of this program that has not been written
// yet. These bytes travel from one bbolt page to the same bbolt page.
//
// Fixed-width and ordered so that the parts a reader needs first are
// first, and so that decoding is arithmetic rather than parsing.
const (
	revisionBytes = 8
	// entryHeader is the two revisions that precede a value.
	entryHeader = revisionBytes * 2
	// eventHeader is the kind and the length of the key that follow it.
	eventHeader = 1 + 4
)

// encodeEntry lays out a stored record: what it is at, when it was born,
// and then the value.
func encodeEntry(entry kv.Entry) []byte {
	raw := make([]byte, entryHeader+len(entry.Value))

	binary.BigEndian.PutUint64(raw[:revisionBytes], entry.Revision.Uint64())
	binary.BigEndian.PutUint64(raw[revisionBytes:entryHeader], entry.CreatedRevision.Uint64())
	copy(raw[entryHeader:], entry.Value)

	return raw
}

// decodeEntry reads one back. The key is not in the bytes because it is
// the bucket key: storing it twice would be two places to disagree.
func decodeEntry(key kv.Key, raw []byte) (kv.Entry, error) {
	if len(raw) < entryHeader {
		return kv.Entry{}, fmt.Errorf("%w: %s is %d bytes", ErrCorrupt, key, len(raw))
	}

	return kv.Entry{
		Key:             key.Clone(),
		Value:           append([]byte(nil), raw[entryHeader:]...),
		Revision:        revision.Revision(binary.BigEndian.Uint64(raw[:revisionBytes])),
		CreatedRevision: revision.Revision(binary.BigEndian.Uint64(raw[revisionBytes:entryHeader])),
	}, nil
}

// encodeEvent lays out one change in the log: what happened, to which
// key, and what the record was afterwards — or, for a delete, what it
// last was.
func encodeEvent(event kv.Event) []byte {
	entry := encodeEntry(event.Entry)
	raw := make([]byte, eventHeader+len(event.Entry.Key)+len(entry))

	raw[0] = byte(event.Kind)
	binary.BigEndian.PutUint32(raw[1:eventHeader], uint32(len(event.Entry.Key)))
	copy(raw[eventHeader:], event.Entry.Key)
	copy(raw[eventHeader+len(event.Entry.Key):], entry)

	return raw
}

// decodeEvent reads one back.
func decodeEvent(raw []byte) (kv.Event, error) {
	if len(raw) < eventHeader {
		return kv.Event{}, fmt.Errorf("%w: an event of %d bytes", ErrCorrupt, len(raw))
	}

	length := int(binary.BigEndian.Uint32(raw[1:eventHeader]))
	if len(raw) < eventHeader+length {
		return kv.Event{}, fmt.Errorf("%w: an event claiming a %d byte key", ErrCorrupt, length)
	}

	key := kv.Key(raw[eventHeader : eventHeader+length])

	entry, err := decodeEntry(key, raw[eventHeader+length:])
	if err != nil {
		return kv.Event{}, err
	}

	return kv.Event{Kind: kv.EventKind(raw[0]), Entry: entry}, nil
}

// revisionKey is how a revision is written as a bucket key, so that the
// log walks in the order it happened.
func revisionKey(at revision.Revision) []byte {
	raw := make([]byte, revisionBytes)
	binary.BigEndian.PutUint64(raw, at.Uint64())

	return raw
}
