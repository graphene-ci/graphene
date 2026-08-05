package store

import (
	"bytes"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// KeyOf encodes an id as the key its value is stored under.
//
//	kind 0x1E value 0x1F value 0x1F …
//
// One encoder for every kind of value, and that is the point. Everything
// above the byte layer — a scan, a watch, a grant confined to a subtree —
// rests on a shorter key being a byte prefix of everything beneath it. If
// each type built its own key that would be a convention; built here it
// is a property.
//
// The trailing separator after EVERY value is what makes the property
// true, and it is not decoration. Without it "/acme" encodes to a byte
// prefix of "/acme2", and a scan of one tenant would return another
// tenant's resources. With it, the prefix ends in 0x1F and "acme2" does
// not have it.
//
// Nothing needs escaping because nothing needs it: a path segment may not
// contain either separator, which the segment rules refuse rather than
// hide. A value carrying one would not be ugly, it would be ambiguous.
//
// An id whose path is short encodes to the prefix of everything under it,
// so this one function serves reads, writes, scans and watches alike.
func KeyOf(id resource.Id) kv.Key {
	var key bytes.Buffer

	key.WriteString(id.Kind().String())
	key.WriteByte(path.KindSeparator)

	for _, value := range id.Path().Values() {
		key.WriteString(value)
		key.WriteByte(path.SegmentSeparator)
	}

	return key.Bytes()
}
