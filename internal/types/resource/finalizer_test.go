package resource_test

import (
	"errors"
	"testing"

	"github.com/graphene-ci/graphene/internal/common/str"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

func finalizer(t *testing.T, raw string) resource.Finalizer {
	t.Helper()

	built, err := resource.NewFinalizer(raw)
	if err != nil {
		t.Fatalf("finalizer %q: %v", raw, err)
	}

	return built
}

// A finalizer name is a promise between two parties who never meet, so
// two spellings of one claim would leave a resource undeletable. These
// are the answers the rules give.
func TestFinalizerIsNormalizedAndChecked(t *testing.T) {
	t.Parallel()

	shaped := map[string]string{
		"  gc  ":                   "gc",
		"Graphene.io/Kernel-Lease": "graphene.io/kernel-lease",
		"ｇｃ":                       "gc",
	}

	for raw, want := range shaped {
		got := finalizer(t, raw)
		if got.String() != want {
			t.Fatalf("%q became %q, want %q", raw, got, want)
		}
	}

	refused := map[string]error{
		"":              str.ErrEmpty,
		"   ":           str.ErrEmpty,
		"/gc":           str.ErrPattern,
		"gc/":           str.ErrPattern,
		"1gc":           str.ErrPattern,
		"graphene..io":  str.ErrPattern,
		"graphene.io//": str.ErrPattern,
		// A space is simply not in the alphabet; a zero-width character is
		// refused earlier and more loudly, because it produces a name that
		// looks identical to the one beside it and compares unequal to it.
		"gc claim": str.ErrNotAllowed,
		"gc​claim": str.ErrForbidden,
	}

	for raw, want := range refused {
		if _, err := resource.NewFinalizer(raw); !errors.Is(err, want) {
			t.Fatalf("%q: want %v, got %v", raw, want, err)
		}
	}
}

// The same claim twice would be removed once and still be there, and the
// resource could never finish being deleted.
func TestTheSameClaimCannotBeListedTwice(t *testing.T) {
	t.Parallel()

	definition := definition(t)

	_, err := resource.NewIntent(id(t, definition, "local", "web"), spec("b1"),
		resource.WithFinalizers(finalizer(t, "gc"), finalizer(t, "GC")))
	if !errors.Is(err, resource.ErrDuplicateFinalizer) {
		t.Fatalf("want ErrDuplicateFinalizer, got %v", err)
	}

	var unnamed resource.Finalizer

	_, err = resource.NewIntent(id(t, definition, "local", "web"), spec("b1"),
		resource.WithFinalizers(unnamed))
	if !errors.Is(err, resource.ErrNoFinalizer) {
		t.Fatalf("want ErrNoFinalizer, got %v", err)
	}
}
