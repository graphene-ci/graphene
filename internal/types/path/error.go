package path

import "errors"

// Why a shape or a path was refused. Callers match on these; the text is
// for a person. Everything about a SEGMENT — empty, too long, a forbidden
// character — comes back as one of the str sentinels instead, because
// that is where the rule that refused it lives.
var (
	// ErrDuplicateName — one shape names two positions the same, which
	// makes every message about "the tenant segment" ambiguous.
	ErrDuplicateName = errors.New("path shape names a position twice")
	// ErrEmptyFieldPath — a field path that names nothing addresses
	// nothing; there is no "the whole resource" to point at.
	ErrEmptyFieldPath = errors.New("field path is empty")
	// ErrArity — a path does not carry the number of segments its shape
	// declares. This is the difference between naming one resource and
	// naming a subtree, so it is never guessed at.
	ErrArity = errors.New("path does not match the arity of its shape")
)
