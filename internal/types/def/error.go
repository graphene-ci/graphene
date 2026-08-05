package def

import "errors"

// Why a definition was refused. Everything about a kind name, a path
// shape or a field path comes back as the sentinel of the package that
// refused it — this is only what a DEFINITION can be wrong about.
var (
	// ErrNoKind, ErrNoShape — a definition without them describes
	// nothing: there would be no name to ask for it by and no way to
	// address an instance.
	ErrNoKind  = errors.New("definition names no kind")
	ErrNoShape = errors.New("definition has no path shape")
	// ErrNoSchema — every kind has both halves. A kind whose status is
	// empty declares a schema that admits no fields; leaving it out would
	// make "no status" and "status nobody described" the same value.
	ErrNoSchema = errors.New("definition is missing a schema")
	// ErrVersion — text that is not a version. Reading one wrong pins a
	// resource to the wrong schema, so the format is exact.
	ErrVersion = errors.New("malformed definition version")
	// ErrNoVersion — a definition published without one. The zero version
	// is what a definition has BEFORE the store has seen it, so publishing
	// it would claim an acceptance that never happened.
	ErrNoVersion = errors.New("definition has no version")
	// ErrRefField, ErrRefKind — half a reference is not a reference.
	ErrRefField = errors.New("reference names no field")
	ErrRefKind  = errors.New("reference names no kind")
	// ErrRefStrength — a reference that did not say what happens when its
	// target is deleted. There is no safe default among refusing,
	// cascading and doing nothing.
	ErrRefStrength = errors.New("reference names no strength")
	// ErrSchemaBroken — a schema that does not compile is a kind that will
	// never validate anything; better heard here than at the first Put.
	ErrSchemaBroken = errors.New("schema does not compile")
	// ErrRefRoot — a reference points somewhere that carries no schema.
	// Only spec and status do; the rest of the envelope is the store's.
	ErrRefRoot = errors.New("reference points outside spec and status")
	// ErrRefNoField — the field a reference names is not in the schema.
	// Caught when the kind is declared, because otherwise it is caught at
	// the first write, far from where the mistake was made.
	ErrRefNoField = errors.New("reference names a field the schema does not have")
	// ErrRefKindMismatch — the field cannot hold a path: a reference is a
	// string, or a list of them.
	ErrRefKindMismatch = errors.New("reference field cannot hold a path")
	// ErrDuplicateRef — two references on one field. Which kind the value
	// points at would have two answers, and nothing to choose between
	// them.
	ErrDuplicateRef = errors.New("two references declared on one field")
)
