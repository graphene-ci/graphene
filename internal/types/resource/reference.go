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

// ReferencesIn is the same reading, done on one half that is not part of
// a resource yet.
//
// It exists for the checks that must run BEFORE the write: what a write
// hands out has to be known while it can still be refused, and by then
// there is no stored resource to read. Which half is named rather than
// assumed, because BOTH can carry a reference — an author writes one in a
// spec, a controller reports one in a status — and a check that only ever
// looked at the spec would let the second one hand out what the first
// cannot.
func ReferencesIn(definition def.Definition, half *schemapb.StructValue, root string) ([]Reference, error) {
	if definition.IsZero() || half == nil {
		return nil, nil
	}

	var found []Reference

	for _, ref := range definition.Refs() {
		if ref.Field().Head().String() != root {
			continue
		}

		raw, err := inSpec(half, ref.Field())
		if err != nil {
			return nil, err
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

// inSpec is lookup with the half already decided.
func inSpec(spec *schemapb.StructValue, field path.FieldPath) ([]string, error) {
	rest := field.Rest()
	if rest.IsZero() {
		return nil, nil
	}

	at, err := spec.Lookup(rest.String())
	if err != nil {
		return nil, nil //nolint:nilerr // a field nobody filled in is not a reference
	}

	return pathsIn(at, field)
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
		// Not an error, and the comment above says why: the definition
		// already checked the field exists in the SCHEMA, so failing to
		// find it in a VALUE means nobody filled it in.
		return nil, nil //nolint:nilerr // an unfilled optional reference is not a failure
	}

	return pathsIn(at, field)
}

// pathsIn reads the one or more paths a reference field holds.
//
// It mirrors what def.referenceable checked when the KIND was declared,
// and now says it in the same words: schemapb describes a value's kind
// with the same vocabulary it describes a field's, so "this is a number
// and a reference is a path" reads identically whether it was caught at
// declaration or at read.
//
// A shape that is neither is REFUSED rather than skipped. It cannot
// normally happen — the definition refused the field at declaration, and
// the resource pins the version that refused it — so a value like this is
// a record written under a schema this one is not reading it with. A
// dropped reference is exactly what strong integrity exists to prevent,
// so it is not the thing to be quiet about.
//
// An empty string and a null are skipped, not refused: both are a field
// somebody cleared, and refusing them would make clearing a reference
// impossible.
func pathsIn(at *schemapb.Value, field path.FieldPath) ([]string, error) {
	switch valueKind := schemapb.ValueKindName(at); valueKind {
	case schemapb.KindNull:
		return nil, nil

	case schemapb.KindString:
		one, _ := schemapb.As[string](at)
		if one == "" {
			return nil, nil
		}

		return []string{one}, nil

	case schemapb.KindList:
		items, _ := at.AsList()
		found := make([]string, 0, len(items))

		for _, item := range items {
			one, ok := schemapb.As[string](item)
			if !ok {
				return nil, fmt.Errorf("%w: %s is a list of %s, and a reference is a path",
					def.ErrRefKindMismatch, field, schemapb.ValueKindName(item))
			}

			if one != "" {
				found = append(found, one)
			}
		}

		return found, nil

	default:
		return nil, fmt.Errorf("%w: %s is %s, and a reference is a path",
			def.ErrRefKindMismatch, field, valueKind)
	}
}
