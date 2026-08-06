package kind

import "github.com/graphene-ci/graphene/internal/common/str"

// A kind name is written once by whoever declares the kind and then typed
// by everyone else forever: on a command line, in a manifest, in a grant.
//
// Unlike a path segment it is NOT folded. "KernelLease" is how its author
// spelled it and how it reads everywhere afterwards; folding would leave
// "kernellease", which nobody would recognize. The price is that Kind is
// case-sensitive — "kernel" and "Kernel" are two kinds — and a client
// that wants to be forgiving about case has to say so itself.
const maxKindBytes = 128

// The two separators a kind may use: '-' between the words of a name,
// '.' between a namespace and a name.
//
//	Kernel   KernelLease   kernel-lease   aws.vm   aws.Vm-Small
//
// The dot is what keeps one block's kinds out of another's way. A block
// naming its kinds "vm" and "build" will collide with the next one, and
// by then there is nowhere left to put a namespace.
const (
	kindNamespaceSeparator = '.'
	kindWordSeparator      = '-'
)

// kindStrRules is the whole of what a kind may be named, in the order it
// is decided.
//
//nolint:gochecknoglobals // a validated value cannot be a const; treated as one
var kindStrRules = []str.Rule{
	str.UTF8(),
	// Fullwidth and compatibility spellings fold away, so "Ｋernel"
	// cannot stand beside "Kernel" as a second kind.
	str.NFKC(),
	str.TrimSpace(),
	str.NotEmpty(),
	str.MaxBytes(maxKindBytes),
	str.NoInvisible(),
	str.Alphabet(kindNamespaceSeparator, kindWordSeparator),
	str.BeginsWith("kind_begins_with_letter", str.IsASCIILetter),
	str.EndsWith("kind_ends_with_letter_or_digit", str.IsASCIIAlphanumeric),
	str.NoAdjacent(kindNamespaceSeparator, kindWordSeparator),
}

// Kind is what a resource is: the name of its definition.
//
// It travels in the key beside the path, and the alphabet above admits
// none of the bytes that encoding reserves — a kind carrying one would
// not be ugly, it would be ambiguous.
type Kind str.String

// New normalizes and checks a kind name.
func New(raw string) (Kind, error) {
	return str.New[Kind](raw, kindStrRules...)
}

// MustNew is New for a name written into the binary.
//
// It panics, and that is right exactly here: a builtin kind whose name
// does not pass the rules is a binary that cannot start, not a request
// that failed, and there is nobody to hand an error to. Never reach for
// it with a name that came from outside.
func MustNew(raw string) Kind {
	named, err := New(raw)
	if err != nil {
		panic("builtin kind: " + err.Error())
	}

	return named
}

func (k Kind) String() string { return string(k) }

func (k Kind) Eq(other Kind) bool { return k == other }

// IsZero reports the unset kind. New never returns it: the rules
// refuse an empty name.
func (k Kind) IsZero() bool { return k == "" }
