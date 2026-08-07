package store_test

import (
	"bytes"
	"testing"

	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// id builds an id of the given kind at the given path.
func id(t *testing.T, kindT string, values ...string) resource.Id {
	t.Helper()

	named, err := kind.New(kindT)
	if err != nil {
		t.Fatalf("kind %q: %v", kindT, err)
	}

	shape, err := path.NewTPath("tenant", "name")
	if err != nil {
		t.Fatalf("shape: %v", err)
	}

	at, err := shape.New(values...)
	if err != nil {
		t.Fatalf("path %v: %v", values, err)
	}

	return resource.NewId(named, at)
}

// The one property everything above the byte layer rests on: a shorter
// key is a byte prefix of everything beneath it, and of nothing else.
//
// This is what a scan, a watch and a grant confined to a subtree are each
// built out of, so it is asserted directly rather than inferred from
// their behavior.
func TestAShorterKeyIsAPrefixOfWhatIsBeneathIt(t *testing.T) {
	t.Parallel()

	tenant := store.KeyOf(id(t, "Process", "acme"))
	under := store.KeyOf(id(t, "Process", "acme", "web"))

	if !bytes.HasPrefix(under, tenant) {
		t.Fatalf("%q is not under %q", under, tenant)
	}

	// A whole kind is the prefix of every resource of it.
	whole := store.KeyOf(id(t, "Process"))
	if !bytes.HasPrefix(under, whole) || !bytes.HasPrefix(tenant, whole) {
		t.Fatalf("%q does not cover its own kind", whole)
	}
}

// The reason every value ends in a separator. Without it "acme" encodes
// to a byte prefix of "acme2", and a scan of one tenant would hand back
// another tenant's resources — which is a leak, not a bug in ordering.
func TestOneNameIsNotAPrefixOfALongerName(t *testing.T) {
	t.Parallel()

	acme := store.KeyOf(id(t, "Process", "acme"))
	acme2 := store.KeyOf(id(t, "Process", "acme2"))

	if bytes.HasPrefix(acme2, acme) {
		t.Fatalf("%q counted as under %q", acme2, acme)
	}

	// The same trap one level down, and the same reason it does not
	// spring: the kind's separator terminates the kind.
	process := store.KeyOf(id(t, "Process"))
	process2 := store.KeyOf(id(t, "Process2"))

	if bytes.HasPrefix(process2, process) {
		t.Fatalf("%q counted as under %q", process2, process)
	}
}

// Two resources of different kinds at the same path are different
// records, and their keys must not collide.
func TestTheKindIsPartOfTheKey(t *testing.T) {
	t.Parallel()

	process := store.KeyOf(id(t, "Process", "acme", "web"))
	volume := store.KeyOf(id(t, "Volume", "acme", "web"))

	if bytes.Equal(process, volume) {
		t.Fatalf("two kinds encoded to the same key %q", process)
	}
}

// A key is bytes and the separators are not text, so a value that could
// carry one would make the encoding ambiguous. The segment rules refuse
// them outright, which is why nothing here escapes anything.
func TestSeparatorsCannotReachAValue(t *testing.T) {
	t.Parallel()

	shape, err := path.NewTPath("tenant", "name")
	if err != nil {
		t.Fatalf("shape: %v", err)
	}

	for _, forbidden := range []string{
		"ac" + string(rune(path.KindSeparator)) + "me",
		"ac" + string(rune(path.SegmentSeparator)) + "me",
	} {
		if _, err := shape.New(forbidden); err == nil {
			t.Fatalf("%q was accepted as a path value", forbidden)
		}
	}
}
