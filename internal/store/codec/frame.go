// Package codec turns a stored value into bytes and back.
//
// It is the framing and nothing else: the field-for-field work of turning
// a domain value into a message lives in internal/convert, which the API
// needs too. What is here is what only storage needs — a tag saying what
// the bytes are, and the marshalling underneath it.
package codec

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// Every stored value begins with these two bytes.
//
// A storage format is forever: bytes written today are read by whatever
// runs next year, and changing the format is a migration rather than a
// refactor. The tag is what makes that migration possible instead of
// archaeological — without it, the first question of the first migration
// is "what was this even written with", and there is no way to answer it
// from the bytes.
//
// Two bytes on a record that already carries a spec.
const (
	// magic says the bytes are ours. It catches a value from another
	// system, a truncated file and a key read from the wrong bucket — all
	// of which would otherwise arrive at proto.Unmarshal, which is
	// cheerful about garbage and often decodes it into an empty message.
	magic byte = 0x67 // 'g'

	// format is which layout follows. It moves when the encoding changes
	// in a way a reader cannot work out for itself; a field added to a
	// message is not that, because protobuf already handles it.
	format byte = 1

	// headerBytes is how much of the value is the tag.
	headerBytes = 2
)

// frame puts the tag in front of an encoded message.
func frame(message proto.Message) ([]byte, error) {
	body, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrEncode, err)
	}

	framed := make([]byte, 0, headerBytes+len(body))
	framed = append(framed, magic, format)

	return append(framed, body...), nil
}

// unframe checks the tag and decodes what follows into message.
//
// It refuses rather than guesses. A value whose tag is wrong is not a
// value to try harder on: it is somebody else's bytes, or ours from a
// version that laid them out differently, and decoding it anyway would
// produce a record that looks plausible and is not.
func unframe(raw []byte, message proto.Message) error {
	if len(raw) < headerBytes {
		return fmt.Errorf("%w: %d bytes", ErrTruncated, len(raw))
	}

	if raw[0] != magic {
		return fmt.Errorf("%w: begins %#x", ErrForeign, raw[0])
	}

	if raw[1] != format {
		return fmt.Errorf("%w: format %d, this build writes %d", ErrFormat, raw[1], format)
	}

	if err := proto.Unmarshal(raw[headerBytes:], message); err != nil {
		return fmt.Errorf("%w: %w", ErrDecode, err)
	}

	return nil
}
