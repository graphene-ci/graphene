package def

import (
	"fmt"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/types/path"
)

// A field path is read from the root of the resource — spec.blob — so its
// first step names the half to look in. Only these two halves carry a
// schema, and only they can hold a declared reference: the rest of the
// envelope is the store's, and what it means is not a kind author's to
// decide.
// Exported because a reference is DECLARED here and READ somewhere else,
// and the two have to name the same halves. Two spellings of "spec" agree
// only until one of them is changed.
const (
	SpecRoot   = "spec"
	StatusRoot = "status"
)

// checkRefField resolves a reference's field against the schemas and says
// what is wrong when it does not.
//
// This is why the check happens at declaration: a typo in "spec.bundle"
// is otherwise a kind that looks fine until the first resource is
// written, and then fails somewhere far from where the mistake was made.
func (d Definition) checkRefField(ref Ref) error {
	schema, rest, err := d.half(ref.Field())
	if err != nil {
		return err
	}

	field, err := schema.LookupPath(rest.String())
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrRefNoField, ref.Field(), err)
	}

	return referenceable(ref, field)
}

// half picks the schema a field path points into, and what is left of the
// path once the half has been named.
func (d Definition) half(field path.FieldPath) (*schemapb.Schema, path.FieldPath, error) {
	rest := field.Rest()
	if rest.IsZero() {
		return nil, path.FieldPath{}, fmt.Errorf("%w: %s names a half and no field",
			ErrRefRoot, field)
	}

	switch field.Head().String() {
	case SpecRoot:
		return d.spec.Schema, rest, nil
	case StatusRoot:
		return d.status.Schema, rest, nil
	default:
		return nil, path.FieldPath{}, fmt.Errorf("%w: %s (a reference lives in %s or %s)",
			ErrRefRoot, field, SpecRoot, StatusRoot)
	}
}

// referenceable reports whether a field can hold a reference at all.
//
// A reference is a PATH of another resource, so the value has to be a
// string. A list of strings is the plural of the same thing — every
// element is a reference — which is how one identity names several roles.
//
// Everything else is refused, computed loudest of all: a reference is an
// edge in the resource graph, and an edge recomputed by an expression
// changes meaning when the expression does, leaving nothing to reason
// about when something asks what is still reachable.
func referenceable(ref Ref, field *schemapb.Schema_Field) error {
	switch kind := schemapb.KindName(field); kind {
	case schemapb.KindString:
		return nil

	case schemapb.KindList:
		items := field.Items()
		if len(items) != 1 {
			return fmt.Errorf("%w: %s is a list of %d kinds; a reference needs one",
				ErrRefKindMismatch, ref.Field(), len(items))
		}

		if itemKind := schemapb.KindName(items[0]); itemKind != schemapb.KindString {
			return fmt.Errorf("%w: %s is a list of %s, and a reference is a path",
				ErrRefKindMismatch, ref.Field(), itemKind)
		}

		return nil

	default:
		return fmt.Errorf("%w: %s is %s, and a reference is a path",
			ErrRefKindMismatch, ref.Field(), kind)
	}
}
