package codec

import (
	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/convert"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// Definition stores published definitions.
//
// It takes def.Published and not def.Definition because a bare definition
// cannot name itself: its key is Definition plus /<kind>/<version>, and a
// definition deliberately does not carry a version — that is the store's
// word for a shape, not part of the shape.
type Definition struct{}

// Id is where a published definition belongs.
//
// The error is swallowed and the zero id returned in its place, which is
// safe because it cannot happen: the kind and the version both came out
// of types that refuse to hold anything the path rules would reject. A
// zero id fails loudly at the write rather than quietly at the key.
func (Definition) Id(value def.Published) resource.Id {
	at, err := def.PublishedPath(value.Kind(), value.Version())
	if err != nil {
		return resource.Id{}
	}

	return resource.NewId(def.PublishedKind, at)
}

// Encode writes a published definition down.
func (Definition) Encode(value def.Published) ([]byte, error) {
	return frame(convert.DefinitionToPb(value))
}

// Decode reads one back, through def.New — which compiles both schemas
// and resolves every declared reference against them. A definition that
// loads is one that would have been accepted.
func (Definition) Decode(raw []byte) (def.Published, error) {
	var message graphenepbv1.Definition

	if err := unframe(raw, &message); err != nil {
		return def.Published{}, err
	}

	return convert.DefinitionFromPb(&message)
}
