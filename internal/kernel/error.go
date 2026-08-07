package kernel

import "errors"

// Why the kernel refused. Everything about a VALUE — a spec that does not
// satisfy its schema, a path of the wrong shape, an owner that changed —
// comes back as one of the resource or def sentinels instead, because
// that is where the rule that refused it lives.
var (
	// ErrNoSuchKind — nothing has been defined under that name.
	//
	// Told apart from a missing record on purpose: a caller that asked for
	// a resource of an undefined kind made a different mistake from one
	// that asked for a resource that is not there, and the second is
	// usually fine while the first never is.
	ErrNoSuchKind = errors.New("kind is not defined")

	// ErrNoSuchVersion — the kind exists but was never published at that
	// version, or the version has been swept.
	ErrNoSuchVersion = errors.New("kind was not published at that version")

	// ErrRefMissing — a resource points at something that is not there.
	// Strong and owning references must resolve; a weak one is never
	// looked up, which is what makes it weak.
	ErrRefMissing = errors.New("reference points at nothing")

	// ErrRefNotExact — a reference that names a subtree rather than one
	// resource. Pointing at "everything under acme" is not a reference,
	// it is a query, and nothing here knows what to do with one.
	ErrRefNotExact = errors.New("reference does not name one resource")

	// ErrReferenced — a resource cannot be deleted while something holds
	// a strong reference to it. That is what a strong reference IS: the
	// promise that the target outlives the pointer.
	ErrReferenced = errors.New("resource is still referenced")

	// ErrOwnerChanged — an attempt to re-point an owning reference. It
	// would quietly change who dies with whom, and the change would be
	// invisible until something died that should not have.
	ErrOwnerChanged = errors.New("an owning reference cannot be re-pointed")

	// ErrShapeChanged — a new version of a kind addresses its instances
	// differently.
	//
	// A shape is part of what a kind IS, so this is not a version of the
	// same kind; it is a different kind under a taken name. References to
	// it are written paths resolved through the current shape, and they
	// would all come to mean something else at once.
	ErrShapeChanged = errors.New("a kind cannot change how its instances are addressed")

	// ErrReservedKind — a kind named the way the kernel names its own
	// records. Its instances would land in the key space the heads or the
	// published shapes live in, and one would overwrite the other without
	// anything noticing.
	ErrReservedKind = errors.New("kind name is reserved")

	// ErrKindInUse — a kind cannot be removed while instances of it are
	// left. Removing it would leave records nothing can validate, read
	// back or address.
	ErrKindInUse = errors.New("kind still has instances")
)
