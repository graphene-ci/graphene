package codec

import (
	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/convert"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// Head stores the definition of a kind that is current.
//
// It writes the same message Definition does and reads it back the same
// way. The whole of the difference is Id: one record per kind name rather
// than one per version of it. That is why def.Head is a type and not a
// convention — the two codecs are otherwise identical, and a mistake
// between them would compile.
type Head struct{}

// Id is where the current definition of a kind belongs.
//
// The error is dropped and the zero id returned in its place. It cannot
// happen — the kind came out of a type that holds nothing the path rules
// would reject — and Store.Put refuses a zero id, so the mistake would
// surface at the write rather than under a key that addresses a subtree.
func (Head) Id(value def.Head) resource.Id {
	at, err := def.HeadPath(value.Kind())
	if err != nil {
		return resource.Id{}
	}

	return resource.NewId(def.HeadKind, at)
}

// Encode writes the current definition down.
func (Head) Encode(value def.Head) ([]byte, error) {
	return frame(convert.DefinitionToPb(value.Published))
}

// Decode reads one back, through def.New — which compiles both schemas
// and resolves every declared reference against them.
func (Head) Decode(raw []byte) (def.Head, error) {
	var message graphenepbv1.Definition

	if err := unframe(raw, &message); err != nil {
		return def.Head{}, err
	}

	published, err := convert.DefinitionFromPb(&message)
	if err != nil {
		return def.Head{}, err
	}

	return def.NewHead(published)
}
