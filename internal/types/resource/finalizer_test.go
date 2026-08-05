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

// Claiming twice leaves one claim, because a claim listed twice would be
// released once and still be there — and the resource could never finish
// being deleted.
//
// It is idempotent rather than refused: a controller unwinding and
// starting again should not have to remember whether it already claimed.
func TestClaimingTwiceLeavesOneClaim(t *testing.T) {
	t.Parallel()

	definition := definition(t)
	admitted := admit(t, definition, intent(t, definition, "b1"), resource.Resource{})

	once, err := resource.Claim(admitted, finalizer(t, "gc"))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	// The same claim, spelled differently: finalizers are folded, so
	// these are one claim and not two.
	twice, err := resource.Claim(once, finalizer(t, "GC"))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	if len(twice.Finalizers()) != 1 {
		t.Fatalf("claiming twice left %v", twice.Finalizers())
	}

	// Releasing what was never placed is the same forgiveness.
	let, err := resource.Release(twice, finalizer(t, "other"))
	if err != nil {
		t.Fatalf("release: %v", err)
	}

	if len(let.Finalizers()) != 1 {
		t.Fatalf("releasing a claim nobody placed left %v", let.Finalizers())
	}

	gone, err := resource.Release(let, finalizer(t, "gc"))
	if err != nil {
		t.Fatalf("release: %v", err)
	}

	if len(gone.Finalizers()) != 0 {
		t.Fatalf("releasing the claim left %v", gone.Finalizers())
	}

	var unnamed resource.Finalizer
	if _, err := resource.Claim(admitted, unnamed); !errors.Is(err, resource.ErrNoFinalizer) {
		t.Fatalf("want ErrNoFinalizer, got %v", err)
	}
}
