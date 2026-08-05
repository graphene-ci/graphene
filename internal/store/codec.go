package store

import "github.com/graphene-ci/graphene/internal/types/resource"

// Codec is how one kind of value crosses between types and bytes.
//
// It is an interface ABOUT T rather than a set of methods ON T, and that
// is deliberate. Decoding produces a value, so as a method it would have
// to take a pointer receiver and write into its own receiver — which
// would make every domain value mutable and, worse, would be a public
// constructor taking arbitrary bytes. Admit and Restore would stop being
// the only doors into a Resource the moment such a method existed.
//
// Beside the type instead, a decoder can go through those doors and check
// what they check, and the value it produces stays something nobody can
// forge.
type Codec[T any] interface {
	// Id is what the value is: where it belongs, which is what its key is
	// built from. Writes take a value and need no separate id.
	Id(value T) resource.Id
	// Encode writes the value down.
	Encode(value T) ([]byte, error)
	// Decode reads one back, refusing bytes that do not make a value.
	Decode(raw []byte) (T, error)
}
