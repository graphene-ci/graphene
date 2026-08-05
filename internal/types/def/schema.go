package def

import "github.com/gopherex/schemapb/go/schemapb"

// A definition carries two schemas of the same Go type, side by side. If
// they stayed *schemapb.Schema, swapping them at a call site would
// compile — and produce a kind whose spec is validated by the status
// schema and whose status is validated by the spec's. Nothing would say
// so until the first resource was written.
//
// The pointer is EMBEDDED rather than converted: the schema's methods
// come along, so holding a SpecSchema is not worse than holding a schema,
// only more specific. Nothing is copied but the pointer, which matters —
// a protobuf message must not be copied by value.
type SpecSchema struct {
	*schemapb.Schema
}

// StatusSchema is the shape of the status half.
//
// Every kind has one. A kind whose status is empty says so with a schema
// that admits no fields, which is a thing to declare and not a thing to
// leave out: "no status" and "status nobody described" would otherwise be
// the same value.
type StatusSchema struct {
	*schemapb.Schema
}

// Spec and Status wrap a schema. They exist so a call site reads as what
// it means rather than as two pointers in an order someone has to
// remember.
func Spec(schema *schemapb.Schema) SpecSchema { return SpecSchema{Schema: schema} }

// Status wraps the status half's schema.
func Status(schema *schemapb.Schema) StatusSchema { return StatusSchema{Schema: schema} }
