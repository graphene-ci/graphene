package path

import (
	"iter"
	"slices"
	"strings"

	"github.com/graphene-ci/graphene/internal/common/str"
)

// Path is a shape with its positions filled: what names one resource —
// or, with fewer values than the shape has positions, the subtree
// beneath it.
//
// It carries its own shape, so nothing that reads a path has to be told
// what kind it belongs to in order to ask whether it is whole.
//
// There is no exported way to build one but TPath.New. A path assembled
// by hand would be a path whose values never met the rules, and it would
// be indistinguishable from one that did.
type Path struct {
	shape  TPath
	values []str.String
}

// Shape is what this path is a path of.
func (p Path) Shape() TPath { return p.shape }

// Len is how many positions are filled.
func (p Path) Len() int { return len(p.values) }

// IsZero reports a path that was never built.
func (p Path) IsZero() bool { return p.shape.IsZero() && len(p.values) == 0 }

// IsExact reports whether this names ONE resource rather than the subtree
// under it. Everything that reads a path asks this before deciding
// whether it was handed a key or a scan.
func (p Path) IsExact() bool { return len(p.values) == p.shape.Arity() }

// Values are the filled-in strings, in order — what a key is built from.
func (p Path) Values() []string {
	result := make([]string, len(p.values))
	for i, v := range p.values {
		result[i] = string(v)
	}

	return result
}

// All walks the filled positions as name and value together. An iterator
// rather than a slice of pairs: the pair has no life of its own, and a
// type that exists only to be ranged over is a type to explain forever.
func (p Path) All() iter.Seq2[TPathSegment, str.String] {
	return func(yield func(TPathSegment, str.String) bool) {
		for index, value := range p.values {
			if !yield(p.shape.Name(index), value) {
				return
			}
		}
	}
}

// Eq compares shape and values both. The same values under different
// shapes are different paths: one is a tenant and a name, the other is
// whatever else happened to be spelled that way.
func (p Path) Eq(other Path) bool {
	return p.shape.Eq(other.shape) && slices.Equal(p.values, other.values)
}

// HasPrefix reports whether this path lies under the given one.
//
// The rest of the system is built out of this — a grant confines to a
// prefix, a scan walks one, a watch follows one — so it lives here rather
// than being open-coded at each of them. Whole values are compared, so
// "acme" does not cover "acme2".
func (p Path) HasPrefix(prefix Path) bool {
	if !p.shape.Eq(prefix.shape) || len(prefix.values) > len(p.values) {
		return false
	}

	return slices.Equal(prefix.values, p.values[:len(prefix.values)])
}

func (p Path) String() string {
	return "/" + strings.Join(p.Values(), "/")
}
