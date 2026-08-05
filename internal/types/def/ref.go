package def

import (
	"fmt"

	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
)

// Ref declares that the value at a field is the PATH of another resource.
//
// Without it a reference is a string like any other, and three things
// that ought to work do not: writing a resource pointing at something
// absent is accepted, deleting the thing it points at is accepted, and
// nothing can say what is still reachable — which is what a collector
// needs to know before it removes anything.
//
// It says where the reference is and what it points to. What it does NOT
// say is what should happen when the target goes: refusing the delete and
// cascading are both defensible, and that is a decision for the kind, not
// for the shape of the declaration.
type Ref struct {
	field    path.FieldPath
	kind     kind.Kind
	strength Strength
}

// NewRef declares one reference.
//
// The strength is required and has no default. Every value of it is a
// different answer to "what happens when the target is deleted", and
// there is no safe guess among refusing, cascading and doing nothing.
func NewRef(field path.FieldPath, kind kind.Kind, strength Strength) (Ref, error) {
	if field.IsZero() {
		return Ref{}, fmt.Errorf("%w: reference to %s names no field", ErrRefField, kind)
	}

	if kind.IsZero() {
		return Ref{}, fmt.Errorf("%w: reference at %s names no kind", ErrRefKind, field)
	}

	if strength.IsZero() {
		return Ref{}, fmt.Errorf("%w: %s → %s", ErrRefStrength, field, kind)
	}

	return Ref{field: field, kind: kind, strength: strength}, nil
}

// ParseRef is NewRef from what a person writes.
//
// The kind is named "named" and not "kind" because the package it is
// parsed by is called kind, and a parameter that shadowed it would make
// the line below stop compiling for a reason nobody reads twice.
func ParseRef(field, named string, strength Strength) (Ref, error) {
	parsed, err := path.ParseFieldPath(field)
	if err != nil {
		return Ref{}, fmt.Errorf("reference field: %w", err)
	}

	target, err := kind.New(named)
	if err != nil {
		return Ref{}, fmt.Errorf("reference kind: %w", err)
	}

	return NewRef(parsed, target, strength)
}

// Field is where the reference lives.
func (r Ref) Field() path.FieldPath { return r.field }

// Kind is what it points at.
func (r Ref) Kind() kind.Kind { return r.kind }

// Strength is what happens to the two of them when the target is deleted.
func (r Ref) Strength() Strength { return r.strength }

// IsZero reports a reference that was never declared.
func (r Ref) IsZero() bool { return r.field.IsZero() && r.kind.IsZero() }

func (r Ref) Eq(other Ref) bool {
	return r.field.Eq(other.field) && r.kind.Eq(other.kind) && r.strength == other.strength
}

func (r Ref) String() string {
	return r.field.String() + " →" + r.strength.String() + "→ " + r.kind.String()
}
