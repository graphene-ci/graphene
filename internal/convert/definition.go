package convert

import (
	"fmt"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/path"
)

// DefinitionToPb writes a published definition down.
//
// It takes the pair and not the bare definition, because the version is
// what tells one publication of a shape from the next and a record
// without it could not be found again.
func DefinitionToPb(published def.Published) *graphenepbv1.Definition {
	if published.IsZero() {
		return nil
	}

	definition := published.Definition()

	shape := make([]string, 0, definition.Shape().Arity())
	for _, name := range definition.Shape().Names() {
		shape = append(shape, name.String())
	}

	refs := make([]*graphenepbv1.Ref, 0, len(definition.Refs()))
	for _, ref := range definition.Refs() {
		refs = append(refs, refToPb(ref))
	}

	return &graphenepbv1.Definition{
		Kind:         definition.Kind().String(),
		Version:      uint32(published.Version()),
		Shape:        shape,
		SpecSchema:   definition.Spec().Schema,
		StatusSchema: definition.Status().Schema,
		Refs:         refs,
	}
}

// DefinitionFromPb reads one back through def.New, which compiles both
// schemas and resolves every reference against them — so a definition
// that loads is a definition that would have been accepted.
func DefinitionFromPb(message *graphenepbv1.Definition) (def.Published, error) {
	if message == nil {
		return def.Published{}, nil
	}

	named, err := kindFromPb(message.GetKind())
	if err != nil {
		return def.Published{}, err
	}

	shape, err := path.NewTPath(message.GetShape()...)
	if err != nil {
		return def.Published{}, fmt.Errorf("%s: path shape: %w", named, err)
	}

	refs := make([]def.Ref, 0, len(message.GetRefs()))

	for _, raw := range message.GetRefs() {
		ref, err := refFromPb(raw)
		if err != nil {
			return def.Published{}, fmt.Errorf("%s: %w", named, err)
		}

		refs = append(refs, ref)
	}

	definition, err := def.New(
		named,
		shape,
		def.Spec(message.GetSpecSchema()),
		def.Status(message.GetStatusSchema()),
		refs...,
	)
	if err != nil {
		return def.Published{}, err
	}

	return def.Publish(definition, def.Version(message.GetVersion()))
}

// refToPb writes one declared reference down.
func refToPb(ref def.Ref) *graphenepbv1.Ref {
	field := make([]string, 0, ref.Field().Len())
	for _, name := range ref.Field().Names() {
		field = append(field, name.String())
	}

	return &graphenepbv1.Ref{
		Field:    field,
		Kind:     ref.Kind().String(),
		Strength: StrengthToPb(ref.Strength()),
	}
}

// refFromPb reads one back.
func refFromPb(message *graphenepbv1.Ref) (def.Ref, error) {
	field, err := path.NewFieldPath(message.GetField()...)
	if err != nil {
		return def.Ref{}, fmt.Errorf("reference field: %w", err)
	}

	target, err := kindFromPb(message.GetKind())
	if err != nil {
		return def.Ref{}, fmt.Errorf("reference kind: %w", err)
	}

	strength, err := strengthFromPb(message.GetStrength())
	if err != nil {
		return def.Ref{}, err
	}

	return def.NewRef(field, target, strength)
}

// strengthToPb and strengthFromPb map the one thing a reference carries
// beyond where it points.
//
// Written out rather than cast, because the two numberings are allowed to
// drift: proto reserves zero for "unset" and Go does not have to. A cast
// would keep working while meaning something else.
// StrengthToPb is exported because the transport writes holders, and a
// holder is a reference found rather than declared.
func StrengthToPb(strength def.Strength) graphenepbv1.Strength {
	switch strength {
	case def.Strong:
		return graphenepbv1.Strength_STRENGTH_STRONG
	case def.Owner:
		return graphenepbv1.Strength_STRENGTH_OWNER
	case def.Weak:
		return graphenepbv1.Strength_STRENGTH_WEAK
	default:
		return graphenepbv1.Strength_STRENGTH_UNSPECIFIED
	}
}

func strengthFromPb(strength graphenepbv1.Strength) (def.Strength, error) {
	switch strength {
	case graphenepbv1.Strength_STRENGTH_STRONG:
		return def.Strong, nil
	case graphenepbv1.Strength_STRENGTH_OWNER:
		return def.Owner, nil
	case graphenepbv1.Strength_STRENGTH_WEAK:
		return def.Weak, nil
	default:
		return def.NoStrength, fmt.Errorf("%w: %s", def.ErrRefStrength, strength)
	}
}
