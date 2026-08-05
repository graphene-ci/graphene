package codec

import (
	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/convert"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// Resource stores resources.
//
// An empty struct rather than a function returning one: it satisfies
// store.Codec[resource.Resource] structurally, so nothing here has to
// import the store to be its codec, and the store does not have to know
// this package exists. They meet where the kernel wires them together.
type Resource struct{}

// Id is where a resource belongs. It comes off the value, so there is no
// way to write one resource under another one's key.
func (Resource) Id(value resource.Resource) resource.Id { return value.Id() }

// Encode writes a resource down.
func (Resource) Encode(value resource.Resource) ([]byte, error) {
	return frame(convert.ResourceToPb(value))
}

// Decode reads one back, through resource.Restore — so bytes that would
// make an impossible resource produce an error rather than one.
func (Resource) Decode(raw []byte) (resource.Resource, error) {
	var message graphenepbv1.Resource

	if err := unframe(raw, &message); err != nil {
		return resource.Resource{}, err
	}

	return convert.ResourceFromPb(&message)
}
