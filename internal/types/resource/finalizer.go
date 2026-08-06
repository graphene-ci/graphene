package resource

import "github.com/graphene-ci/graphene/internal/common/str"

// A finalizer is a claim on a resource's deletion: while one is present
// the resource is marked for deletion but not removed, so whoever put it
// there gets to clean up first and take it off when done.
//
// The name is therefore a promise between two parties who never meet —
// the one that sets it and the one that reads it later, possibly after a
// restart, possibly in another binary. It is namespaced for the same
// reason a kind is: two blocks that both call theirs "cleanup" would each
// remove the other's claim.
//
//	graphene.io/kernel-lease   aws.io/detach-volume   gc
const (
	finalizerNamespaceSeparator = '.'
	finalizerNameSeparator      = '/'
	finalizerWordSeparator      = '-'

	maxFinalizerBytes = 256
)

// finalizerStrRules is the whole of what a finalizer may be named, in the
// order it is decided.
//
//nolint:gochecknoglobals // a validated value cannot be a const; treated as one
var finalizerStrRules = []str.Rule{
	str.UTF8(),
	str.NFKC(),
	str.TrimSpace(),
	// Folded, unlike a kind. A kind is read by people and keeps the case
	// its author chose; a finalizer is only ever compared, and two
	// spellings of one claim would leave a resource undeletable.
	str.Fold(),
	str.NotEmpty(),
	str.MaxBytes(maxFinalizerBytes),
	str.NoInvisible(),
	str.Alphabet(finalizerNamespaceSeparator, finalizerNameSeparator, finalizerWordSeparator),
	str.BeginsWith("finalizer_begins_with_letter", str.IsASCIILetter),
	str.EndsWith("finalizer_ends_with_letter_or_digit", str.IsASCIIAlphanumeric),
	str.NoAdjacent(finalizerNamespaceSeparator, finalizerNameSeparator, finalizerWordSeparator),
}

// Finalizer names one claim on a resource's deletion.
type Finalizer str.String

// NewFinalizer normalizes and checks a finalizer name.
func NewFinalizer(raw string) (Finalizer, error) {
	return str.New[Finalizer](raw, finalizerStrRules...)
}

func (f Finalizer) String() string { return string(f) }

func (f Finalizer) Eq(other Finalizer) bool { return f == other }

// IsZero reports the unset name. NewFinalizer never returns it: the rules
// refuse an empty value.
func (f Finalizer) IsZero() bool { return f == "" }
