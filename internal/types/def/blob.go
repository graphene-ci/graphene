package def

import (
	"errors"
	"fmt"
	"slices"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/types/path"
)

// What a declared blob field can be wrong about.
var (
	// ErrBlobRoot — bytes are asked for, and asking is the spec's half.
	ErrBlobRoot = errors.New("a blob field lives in the spec")
	// ErrBlobNoField — the schema has no such field.
	ErrBlobNoField = errors.New("no such field for a blob")
	// ErrBlobKindMismatch — the field cannot hold an id.
	ErrBlobKindMismatch = errors.New("a blob field holds an id, which is a string")
	// ErrDuplicateBlob — the same field declared twice.
	ErrDuplicateBlob = errors.New("that field is already declared as a blob")
)

// Blobs are the spec fields of this kind whose value is a blob id.
//
// Declared and not guessed, the same as a reference and for the same
// reason: a string that happens to look like an id is a string, and only
// the definition says which strings are handles to bytes.
//
// What it buys is that a client can ATTACH a file — write `blob: {file:
// ./thing}` in a manifest and have it uploaded and replaced by its id on
// the way — without every kind inventing a spelling for "the bytes go
// here". A kind that declares none cannot be given a file, which is right:
// most kinds are not about bytes.
func (d Definition) Blobs() []path.FieldPath { return slices.Clone(d.blobs) }

// Bytes declares fields that hold blob ids.
func Bytes(fields ...path.FieldPath) Option {
	return func(d *Definition) { d.blobs = append(d.blobs, fields...) }
}

// Reference declares references instances of this kind carry.
//
// An option rather than a trailing variadic of its own, because a
// definition now has two optional facets and will have more: what a kind
// points AT and where its bytes are answered separately, and a
// constructor that took each as its own list would grow a position for
// every one of them.
func Reference(refs ...Ref) Option {
	return func(d *Definition) { d.refs = append(d.refs, refs...) }
}

// Option is something optional a definition carries.
type Option func(*Definition)

// checkBlobs resolves each declared field against the spec schema.
//
// At declaration, for the reason references are checked there: a typo is
// otherwise a kind that looks fine until somebody hands it a file, and
// then fails somewhere far from the mistake.
func (d Definition) checkBlobs() error {
	seen := make([]path.FieldPath, 0, len(d.blobs))

	for _, field := range d.blobs {
		if field.Head().String() != SpecRoot {
			return fmt.Errorf("%w: %s", ErrBlobRoot, field)
		}

		if slices.ContainsFunc(seen, field.Eq) {
			return fmt.Errorf("%w: %s", ErrDuplicateBlob, field)
		}

		found, err := d.spec.LookupPath(field.Rest().String())
		if err != nil {
			return fmt.Errorf("%w: %s: %w", ErrBlobNoField, field, err)
		}

		if named := schemapb.KindName(found); named != schemapb.KindString {
			return fmt.Errorf("%w: %s is %s", ErrBlobKindMismatch, field, named)
		}

		seen = append(seen, field)
	}

	return nil
}
