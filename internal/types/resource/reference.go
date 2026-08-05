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

// lookup walks one declared field path into a resource's values.
//
// The head of the path says which half to walk — the definition refuses a
// reference that names neither — and the rest names fields inside it. The
// value found is either one string or a list of them, which is what the
// definition already checked the SCHEMA allows; here the same shapes are
// read out of the value.
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

	return walk(half, field.Rest())
}

// walk descends the remaining field names and reads what it lands on.
func walk(values *schemapb.StructValue, field path.FieldPath) ([]string, error) {
	if values == nil || field.IsZero() {
		return nil, nil
	}

	at, found := values.GetFields()[field.Head().String()]
	if !found {
		return nil, nil
	}

	if rest := field.Rest(); !rest.IsZero() {
		return walk(at.GetStructValue(), rest)
	}

	return pathsIn(at), nil
}

// pathsIn reads the one or more paths a reference field holds.
//
// One string or a list of them, and nothing else: the definition refused
// any other shape when the kind was declared, so anything else here is a
// value that was written under a different schema than the one being
// read — which is a question for whoever pinned the version, not a shape
// to guess at.
func pathsIn(at *schemapb.Value) []string {
	if at == nil {
		return nil
	}

	if single := at.GetStringValue(); single != "" {
		return []string{single}
	}

	list := at.GetListValue().GetItems()
	found := make([]string, 0, len(list))

	for _, item := range list {
		if one := item.GetStringValue(); one != "" {
			found = append(found, one)
		}
	}

	return found
}
