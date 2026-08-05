package str

import (
	"errors"
	"fmt"
)

// Why a value was refused. Callers match on these rather than on text: a
// message is for a person, and only these say what to do about it.
var (
	// ErrEmpty — nothing left after normalization. A value that was only
	// whitespace is a different mistake from one that was omitted, and
	// this is the first.
	ErrEmpty = errors.New("value is empty")
	// ErrTooShort and ErrTooLong — outside the declared size.
	ErrTooShort = errors.New("value is too short")
	ErrTooLong  = errors.New("value is too long")
	// ErrForbidden — a character that must never appear did.
	ErrForbidden = errors.New("value contains a forbidden character")
	// ErrNotAllowed — a character outside the permitted set.
	ErrNotAllowed = errors.New("value contains a character outside the allowed set")
	// ErrPattern — the shape does not match.
	ErrPattern = errors.New("value does not match the required pattern")
	// ErrNotOneOf — not one of the values the caller enumerated.
	ErrNotOneOf = errors.New("value is not one of the permitted values")
	// ErrNotUTF8 — the bytes are not text at all.
	ErrNotUTF8 = errors.New("value is not valid utf-8")
)

// Error says which rule refused, and what the value looked like BY THEN —
// which is not what the caller passed in, because earlier rules rewrote
// it. Without that, a refusal on a folded and trimmed value reads as a
// lie about the input.
type Error struct {
	// Rule names the step, e.g. "max_bytes".
	Rule string
	// Value is the string as that rule saw it.
	Value string
	// Detail explains the specific limit, when there is one.
	Detail string
	// Err is one of the sentinels above.
	Err error
}

func (e *Error) Error() string {
	if e.Detail == "" {
		return fmt.Sprintf("%s: %v (%q)", e.Rule, e.Err, e.Value)
	}

	return fmt.Sprintf("%s: %v: %s (%q)", e.Rule, e.Err, e.Detail, e.Value)
}

func (e *Error) Unwrap() error { return e.Err }

func refuse(rule, value, detail string, reason error) error {
	return &Error{Rule: rule, Value: value, Detail: detail, Err: reason}
}

// runeAt says which character and where, for a message a person can act
// on. %q so an invisible one shows as an escape rather than as nothing.
func runeAt(offending rune, index int) string {
	return fmt.Sprintf("%q at byte %d", offending, index)
}
