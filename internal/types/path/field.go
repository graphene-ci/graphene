package path

import (
	"fmt"
	"slices"
	"strings"

	"github.com/graphene-ci/graphene/internal/common/str"
)

// A field path addresses a place inside a resource, read the way its yaml
// reads: spec.blob, status.phase, key.kind, revision.
//
// It says WHERE to look and nothing about what is there. Whether the
// field exists is a question for whoever holds the schema — the same
// division as Path, which is a shape and not a promise that the resource
// exists.
//
// Three things use it and they are the reason it is a type rather than a
// string in three places: selectors on List and Watch, the Where terms of
// a grant, and the references a definition declares.
const maxFieldNameBytes = 128

// fieldNameStrRules is what a field name may be.
//
// NOT folded, unlike everything else here: the name has to match a schema
// byte for byte, and folding would turn "bundleVersion" into
// "bundleversion", which is in no schema anywhere.
var fieldNameStrRules = []str.Rule{
	str.UTF8(),
	str.NFKC(),
	str.TrimSpace(),
	str.NotEmpty(),
	str.MaxBytes(maxFieldNameBytes),
	str.NoInvisible(),
	// Underscore is in the alphabet because our schemas are written in
	// snake_case: ttl_seconds, exit_code, principal_kind.
	str.Alphabet(fieldWordSeparator),
	str.BeginsWith("field_begins_with_letter", str.IsASCIILetter),
}

const (
	fieldSeparator     = '.'
	fieldWordSeparator = '_'
)

// FieldName is one step of a field path.
type FieldName str.String

// NewFieldName normalizes and checks one name.
func NewFieldName(raw string) (FieldName, error) {
	return str.New[FieldName](raw, fieldNameStrRules...)
}

func (f FieldName) String() string { return string(f) }

func (f FieldName) Eq(other FieldName) bool { return f == other }

// IsZero reports the unset name. NewFieldName never returns it.
func (f FieldName) IsZero() bool { return f == "" }

// FieldPath is a path from the root of a resource to one field.
type FieldPath struct {
	names []FieldName
}

// NewFieldPath builds a path from its steps.
func NewFieldPath(names ...string) (FieldPath, error) {
	if len(names) == 0 {
		return FieldPath{}, fmt.Errorf("%w: a field path names nothing", ErrEmptyFieldPath)
	}

	built := FieldPath{names: make([]FieldName, 0, len(names))}

	for index, raw := range names {
		name, err := NewFieldName(raw)
		if err != nil {
			return FieldPath{}, fmt.Errorf("step %d: %w", index, err)
		}

		built.names = append(built.names, name)
	}

	return built, nil
}

// ParseFieldPath splits a written path — "spec.blob".
//
// Splitting is unambiguous because '.' is not in a name's alphabet, so
// this is the inverse of String and not a guess.
func ParseFieldPath(raw string) (FieldPath, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return FieldPath{}, fmt.Errorf("%w: %q", ErrEmptyFieldPath, raw)
	}

	return NewFieldPath(strings.Split(trimmed, string(fieldSeparator))...)
}

// Len is the number of steps.
func (f FieldPath) Len() int { return len(f.names) }

// IsZero reports a path that was never built.
func (f FieldPath) IsZero() bool { return len(f.names) == 0 }

// Names copies the steps out.
func (f FieldPath) Names() []FieldName { return slices.Clone(f.names) }

// Head is the first step and Rest is what follows it — how a resolver
// walks a path: take the head, descend, repeat on the rest.
func (f FieldPath) Head() FieldName {
	if len(f.names) == 0 {
		return ""
	}

	return f.names[0]
}

// Rest is the path after the first step; the zero path when there is
// nothing after it.
func (f FieldPath) Rest() FieldPath {
	if len(f.names) < 2 {
		return FieldPath{}
	}

	return FieldPath{names: f.names[1:]}
}

func (f FieldPath) Eq(other FieldPath) bool {
	return slices.Equal(f.names, other.names)
}

func (f FieldPath) String() string {
	steps := make([]string, len(f.names))
	for i, name := range f.names {
		steps[i] = name.String()
	}

	return strings.Join(steps, string(fieldSeparator))
}
