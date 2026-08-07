// Package def is what a kind IS: its name, the shape of its path, the
// schemas of its two halves, and the references its values carry.
//
// It is a shape and nothing else. The version the store assigned, the
// revision it was written at, whether anything controls it — none of that
// is here, because none of it changes what the kind is. Two definitions
// are the same definition when they describe the same shape, which is the
// only question anyone asks of two of them.
package def

import (
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
)

// Definition is the shape of one kind.
type Definition struct {
	kind   kind.Kind
	shape  path.TPath
	spec   SpecSchema
	status StatusSchema
	refs   []Ref
}

// New builds a definition.
//
// Everything is required. A kind with no name cannot be asked for, a kind
// with no shape has no way to address an instance, and a kind missing a
// schema would validate half of itself — all three are mistakes worth
// hearing about at once rather than at the first write.
func New(
	named kind.Kind,
	shape path.TPath,
	spec SpecSchema,
	status StatusSchema,
	options ...Option,
) (Definition, error) {
	switch {
	case named.IsZero():
		return Definition{}, ErrNoKind
	case shape.IsZero():
		return Definition{}, fmt.Errorf("%w: %s", ErrNoShape, named)
	case spec.Schema == nil:
		return Definition{}, fmt.Errorf("%w: %s has no spec schema", ErrNoSchema, named)
	case status.Schema == nil:
		return Definition{}, fmt.Errorf("%w: %s has no status schema", ErrNoSchema, named)
	}

	if err := compiles(named, spec.Schema, status.Schema); err != nil {
		return Definition{}, err
	}

	built := Definition{
		kind:   named,
		shape:  shape,
		spec:   spec,
		status: status,
	}

	for _, option := range options {
		option(&built)
	}

	declared, err := checkRefs(named, built.refs)
	if err != nil {
		return Definition{}, err
	}

	built.refs = declared

	// Resolving happens last: it needs the schemas, and they are only in
	// place once the definition is assembled.
	for _, ref := range declared {
		if err := built.checkRefField(ref); err != nil {
			return Definition{}, fmt.Errorf("%s: %w", named, err)
		}
	}

	return built, nil
}

// MustNew is New for a definition written into the binary.
//
// It panics for the same reason kind.MustNew does: a builtin kind whose
// schemas do not compile is a binary that cannot start, and there is
// nobody to hand an error to. Never reach for it with anything that came
// from outside.
func MustNew(
	named kind.Kind,
	shape path.TPath,
	spec SpecSchema,
	status StatusSchema,
	options ...Option,
) Definition {
	built, err := New(named, shape, spec, status, options...)
	if err != nil {
		panic("builtin definition: " + err.Error())
	}

	return built
}

// compiles refuses a schema that cannot be compiled.
//
// A schema that does not compile is a kind that will never validate
// anything: every write of every instance would fail, and the reason
// would surface at the first Put rather than here, where the mistake is.
//
// The engine is thrown away on purpose. schemapb caches it against the
// schema and compiles on first use anyway, so keeping it would buy
// nothing and would put derived state inside a value whose whole
// character is that it has none.
func compiles(named kind.Kind, schemas ...*schemapb.Schema) error {
	for _, schema := range schemas {
		if _, err := schemapb.Compile(schema); err != nil {
			return fmt.Errorf("%s: %w: %w", named, ErrSchemaBroken, err)
		}
	}

	return nil
}

// checkRefs refuses a reference that is half-declared, and two on one
// field: where a value points would then have two answers and nothing to
// choose between them. Whether the field exists is settled afterwards,
// against the schemas.
func checkRefs(named kind.Kind, refs []Ref) ([]Ref, error) {
	declared := make([]Ref, 0, len(refs))

	for _, ref := range refs {
		if ref.IsZero() {
			return nil, fmt.Errorf("%s: %w", named, ErrRefField)
		}

		if slices.ContainsFunc(declared, func(seen Ref) bool {
			return seen.Field().Eq(ref.Field())
		}) {
			return nil, fmt.Errorf("%s: %w: %s", named, ErrDuplicateRef, ref.Field())
		}

		declared = append(declared, ref)
	}

	return declared, nil
}

// Kind is what this defines.
func (d Definition) Kind() kind.Kind { return d.kind }

// Shape is how an instance of it is addressed.
func (d Definition) Shape() path.TPath { return d.shape }

// Spec and Status are the two halves.
func (d Definition) Spec() SpecSchema { return d.spec }

// Status is the shape of the status half.
func (d Definition) Status() StatusSchema { return d.status }

// Refs are the references instances of this kind carry.
func (d Definition) Refs() []Ref { return slices.Clone(d.refs) }

// IsZero reports a definition that was never built.
func (d Definition) IsZero() bool { return d.kind.IsZero() }

// Eq asks the only question anyone asks of two definitions: do they
// describe the same shape.
//
// It is what decides whether a kind needs a new version. Everything that
// changes how an instance is validated counts — including the references,
// which decide what a Put must resolve before it is accepted.
func (d Definition) Eq(other Definition) bool {
	return d.kind.Eq(other.kind) &&
		d.shape.Eq(other.shape) &&
		proto.Equal(d.spec.Schema, other.spec.Schema) &&
		proto.Equal(d.status.Schema, other.status.Schema) &&
		slices.EqualFunc(d.refs, other.refs, Ref.Eq)
}

func (d Definition) String() string { return d.kind.String() + " " + d.shape.String() }
