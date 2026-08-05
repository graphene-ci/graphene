package resource

import (
	"errors"
	"strings"
)

// Why an intent or an admission was refused. Callers match on these; the
// text is for a person.
var (
	// ErrNoId — an intent that names nothing. There is no "the resource
	// I mean" without saying which.
	ErrNoId = errors.New("intent names no resource")
	// ErrNotExact — an id that names a subtree rather than one resource.
	// Writing to a subtree is not a bulk write, it is a mistake about what
	// was being addressed.
	ErrNotExact = errors.New("id does not name one resource")
	// ErrNoSpec — no spec at all. An empty spec is an empty struct; only
	// a missing one is this.
	ErrNoSpec = errors.New("intent has no spec")
	// ErrNoStatus — a status report with nothing in it. Nil status means
	// "nobody has looked yet", which is not something a controller reports.
	ErrNoStatus = errors.New("report has no status")
	// ErrNoFinalizer — a claim with no name; nobody could ever match it to
	// release it.
	ErrNoFinalizer = errors.New("finalizer has no name")
	// ErrDuplicateFinalizer — the same claim listed twice. Removing it
	// once would leave it there, and the resource could never finish being
	// deleted.
	ErrDuplicateFinalizer = errors.New("finalizer listed twice")
	// ErrNoFinalizers — a deletion was marked on a resource nothing is
	// waiting for. There is nothing to wait for: delete it outright rather
	// than leaving a tombstone nobody will ever clear.
	ErrNoFinalizers = errors.New("nothing to wait for before deletion")

	// ErrNoDefinition — an admission with no definition to admit against.
	// Nothing can be checked, so nothing is.
	ErrNoDefinition = errors.New("no definition to admit against")
	// ErrNoVersion — a definition whose version was not supplied. The
	// version is what a resource pins, and pinning nothing means nothing
	// afterwards knows which shape it was written under.
	ErrNoVersion = errors.New("definition version not given")
	// ErrNoResource — an operation on a resource that was never admitted.
	ErrNoResource = errors.New("resource was never admitted")
	// ErrNoIntent — an admission with nothing to admit.
	ErrNoIntent = errors.New("no intent to admit")

	// ErrKindMismatch — an intent admitted against the definition of some
	// other kind.
	ErrKindMismatch = errors.New("intent is not of this kind")
	// ErrShapeMismatch — a path that is not shaped the way the kind
	// declares. The same values under another shape name something else.
	ErrShapeMismatch = errors.New("path is not shaped as the kind declares")
	// ErrIdChanged — an admission over a previous resource with a
	// different id, which is not an update of anything.
	ErrIdChanged = errors.New("admission would change the resource's id")
	// ErrDeleting — a spec change on a resource that is already going
	// away. Its finalizers are running against the spec it had; changing
	// it underneath them makes their cleanup wrong.
	ErrDeleting = errors.New("spec cannot change while deleting")

	// ErrInvalid — the values do not satisfy the kind's schema. Always
	// carried by an InvalidError, which says which fields and why.
	ErrInvalid = errors.New("values do not satisfy the schema")
	// ErrSchemaBroken — the schema itself would not compile. Unreachable
	// through a Definition, which compiles its schemas when it is built;
	// kept because "cannot happen" is not the same as "need not be
	// handled".
	ErrSchemaBroken = errors.New("schema does not compile")
)

// Half is which of a resource's two sides is being talked about. A string
// would let "spec" and "status" be swapped at a call site and read the
// same afterwards.
type Half string

// The two halves.
const (
	SpecHalf   Half = "spec"
	StatusHalf Half = "status"
)

func (h Half) String() string { return string(h) }

// Fault is one thing the schema found wrong.
//
// Field is the schema's own path expression — "env[0].name" — and not a
// path.FieldPath, because a fault can point inside a list element and a
// FieldPath names fields only. It is here to be shown to a person and
// matched by nobody.
type Fault struct {
	Field string
	Code  string
}

func (f Fault) String() string { return f.Field + ": " + f.Code }

// InvalidError is every fault at once.
//
// All of them and not the first: a person fixing a manifest wants the
// whole list, and returning one at a time turns one round trip into as
// many as there are mistakes.
type InvalidError struct {
	Half   Half
	Faults []Fault
}

func (e InvalidError) Error() string {
	parts := make([]string, len(e.Faults))
	for index, fault := range e.Faults {
		parts[index] = fault.String()
	}

	return string(e.Half) + " invalid: " + strings.Join(parts, "; ")
}

// Unwrap lets a caller that only wants to know THAT it was invalid match
// on ErrInvalid without reaching for the type.
func (e InvalidError) Unwrap() error { return ErrInvalid }
