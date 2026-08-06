package resource

import (
	"fmt"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
)

// Reference is one place a resource points at another, found.
//
// It carries the target as the STRING it was written as, not as an Id,
// and that is not laziness. Turning it into an Id needs the target kind's
// path shape, which lives in the target's definition, which this package
// cannot reach without a lookup — and a lookup is exactly what a pure
// function does not do. So extraction stops here, and whoever has the
// definitions finishes the job.
type Reference struct {
	// Field is where in the resource it was found, for saying which one
	// went wrong when one does.
	Field path.FieldPath
	// Kind is what the definition says it points at.
	Kind kind.Kind
	// Strength is what happens to the two of them when the target goes.
	Strength def.Strength
	// Raw is the path, spelled the way it was written.
	Raw string
}

func (r Reference) String() string {
	return r.Field.String() + " → " + r.Kind.String() + "/" + r.Raw
}

// References reads out everywhere a resource points, following what the
// definition declared.
//
// A reference is DECLARED and then found, never discovered: a value that
// happens to look like a path is a string, and only the definition says
// which strings are references. That is what makes integrity, install
// order and collection possible before a single instance is written.
//
// A field the definition declares and the value has not filled is not a
// reference and not an error — an optional reference is a real thing, and
// a required one is the schema's business, not this function's.
func References(definition def.Definition, value Resource) ([]Reference, error) {
	if definition.IsZero() || value.IsZero() {
		return nil, nil
	}

	var found []Reference

	for _, ref := range definition.Refs() {
		raw, err := lookup(value, ref.Field())
		if err != nil {
			return nil, fmt.Errorf("%s: %w", value.Id(), err)
		}

		for _, one := range raw {
			found = append(found, Reference{
				Field:    ref.Field(),
				Kind:     ref.Kind(),
				Strength: ref.Strength(),
				Raw:      one,
			})
		}
	}

	return found, nil
}

// lookup reads what a declared field path points at.
//
// The head of the path says which half to read — the definition refuses a
// reference that names neither — and schemapb resolves the rest. Walking
// it by hand was thirty lines that did the same thing worse: this dialect
// also addresses inside lists, which a hand-rolled walk would have to
// learn separately and would learn differently.
//
// A path that does not resolve is not an error. The definition already
// checked the field exists in the SCHEMA; failing to find it in a VALUE
// means nobody filled it in, and an optional reference is a real thing.
func lookup(value Resource, field path.FieldPath) ([]string, error) {
	var half *schemapb.StructValue

	switch name := field.Head().String(); name {
	case def.SpecRoot:
		half = value.Spec()
	case def.StatusRoot:
		half = value.Status()
	default:
		return nil, fmt.Errorf("%w: %s", def.ErrRefRoot, field)
	}

	rest := field.Rest()
	if half == nil || rest.IsZero() {
		return nil, nil
	}

	at, err := half.Lookup(rest.String())
	if err != nil {
		return nil, nil
	}

	return pathsIn(at), nil
}

// pathsIn reads the one or more paths a reference field holds.
//
// One string or a list of them, and nothing else: the definition refused
// any other shape when the kind was declared. As reports PRESENCE rather
// than handing back a silent zero, which is the difference between "this
// field holds the empty string" and "this field is not a string at all" —
// and the two used to look the same.
//
// An empty string is skipped rather than refused. It is a field somebody
// cleared, not a path to nowhere, and turning it into an error would make
// clearing a reference impossible.
func pathsIn(at *schemapb.Value) []string {
	if one, ok := schemapb.As[string](at); ok {
		if one == "" {
			return nil
		}

		return []string{one}
	}

	items, ok := at.AsList()
	if !ok {
		return nil
	}

	found := make([]string, 0, len(items))

	for _, item := range items {
		if one, ok := schemapb.As[string](item); ok && one != "" {
			found = append(found, one)
		}
	}

	return found
}
