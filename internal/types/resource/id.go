package resource

import (
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
)

// Id names one resource: which kind it is, and where under that
// kind it lives.
//
// Both halves are needed and neither is enough. Two kinds may shape their
// paths the same way and both have an "/acme/k1", so a path alone is
// ambiguous; a kind alone names a whole family. Together they are what a
// key is built from, what a grant confines, and what a reference points
// at.
type Id struct {
	kind kind.Kind
	path path.Path
}

// NewId names a resource.
//
// Nothing is checked here because there is nothing left to check: a Kind
// and a Path are both types that cannot be built wrong. What this does
// not settle is whether the path has the shape the kind declares — that
// needs the definition, and it is asked where the definition is.
func NewId(named kind.Kind, at path.Path) Id {
	return Id{kind: named, path: at}
}

// Kind is what the resource is.
func (i Id) Kind() kind.Kind { return i.kind }

// Path is where under that kind it lives.
func (i Id) Path() path.Path { return i.path }

// IsZero reports an id that was never built.
func (i Id) IsZero() bool { return i.kind.IsZero() && i.path.IsZero() }

// IsExact reports whether this names ONE resource rather than the subtree
// beneath it. A write asks this and refuses the subtree; a scan asks it
// and is happy either way.
func (i Id) IsExact() bool { return i.path.IsExact() }

// Eq is identity: same kind, same path.
func (i Id) Eq(other Id) bool {
	return i.kind.Eq(other.kind) && i.path.Eq(other.path)
}

// HasPrefix reports whether this id lies under the given one — the same
// kind, and a path beneath its path. This is what a grant, a scan and a
// watch are each confined by.
func (i Id) HasPrefix(prefix Id) bool {
	return i.kind.Eq(prefix.kind) && i.path.HasPrefix(prefix.path)
}

// String reads as the kind followed by the path: "Kernel/acme/k1".
func (i Id) String() string { return i.kind.String() + i.path.String() }
