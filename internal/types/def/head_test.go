package def_test

import (
	"errors"
	"testing"

	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
)

// A head is the current definition, not a pointer at one. It is its own
// type so that the compiler can tell Store[Head] from Store[Published] —
// they differ only in where a value belongs, which never shows up in a
// signature.
func TestAHeadIsTheCurrentDefinition(t *testing.T) {
	t.Parallel()

	built, err := def.New(processKind(t), processShape(t), specSchema(t), statusSchema(t))
	if err != nil {
		t.Fatalf("definition: %v", err)
	}

	published, err := def.Publish(built, 2)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	head, err := def.NewHead(published)
	if err != nil {
		t.Fatalf("head: %v", err)
	}

	// Embedded, so a head is not worse than a definition to hold.
	if !head.Kind().Eq(published.Kind()) || !head.Version().Eq(2) {
		t.Fatalf("head reads as %s", head)
	}

	if !head.Definition().Eq(published.Definition()) {
		t.Fatal("the head carries a different shape than it was made from")
	}

	if _, err := def.NewHead(def.Published{}); !errors.Is(err, def.ErrNoKind) {
		t.Fatalf("a head of nothing: want ErrNoKind, got %v", err)
	}

	var none def.Head
	if !none.IsZero() {
		t.Fatal("the zero head claimed to be built")
	}
}

// A kind keeps the case it was written in; its path does not. Two kinds
// differing only in case therefore cannot both exist — the second one
// collides with the first at the store.
//
// That is the uniqueness rule, enforced by the key rather than by a check
// somebody has to remember to write, so it is asserted where it is
// decided.
func TestTwoKindsDifferingOnlyInCaseShareAPath(t *testing.T) {
	t.Parallel()

	written, err := def.PublishedPath(kind.MustNew("KernelLease"), 2)
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	lower, err := def.PublishedPath(kind.MustNew("kernellease"), 2)
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	if !written.Eq(lower) {
		t.Fatalf("%s and %s address different records", written, lower)
	}

	if written.String() != "/kernellease/v2" {
		t.Fatalf("addressed as %s", written)
	}

	// Different versions of one kind are different records.
	other, err := def.PublishedPath(kind.MustNew("KernelLease"), 3)
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	if written.Eq(other) {
		t.Fatal("two versions of one kind share a record")
	}
}

// A head and the shapes it points at live under different kinds, so
// neither is ever a prefix of the other by accident.
func TestAHeadAndItsShapesAreAddressedApart(t *testing.T) {
	t.Parallel()

	named := kind.MustNew("Process")

	head, err := def.HeadPath(named)
	if err != nil {
		t.Fatalf("head path: %v", err)
	}

	shape, err := def.PublishedPath(named, 1)
	if err != nil {
		t.Fatalf("published path: %v", err)
	}

	if def.HeadKind.Eq(def.PublishedKind) {
		t.Fatal("a head and a published shape share a kind")
	}

	if head.String() != "/process" || shape.String() != "/process/v1" {
		t.Fatalf("addressed as %s and %s", head, shape)
	}
}
