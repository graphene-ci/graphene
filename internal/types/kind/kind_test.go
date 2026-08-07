package kind_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/graphene-ci/graphene/internal/common/str"
	"github.com/graphene-ci/graphene/internal/types/kind"
)

// A kind keeps the spelling its author chose. Folding would leave
// "kernellease", which nobody would recognize.
func TestKindKeepsItsSpelling(t *testing.T) {
	t.Parallel()

	kept := []string{
		"Kernel",
		"KernelLease",
		"kernel-lease",
		"aws.vm",
		"aws.Vm-Small",
		"k8s",
	}

	for _, raw := range kept {
		got, err := kind.New(raw)
		if err != nil {
			t.Fatalf("%q refused: %v", raw, err)
		}

		if got.String() != raw {
			t.Fatalf("%q became %q", raw, got.String())
		}
	}

	// Surrounding space is not part of a name, and a fullwidth spelling
	// is the same name written to look different.
	shaped := map[string]string{
		"  Kernel  ": "Kernel",
		"Ｋｅｒｎｅｌ":     "Kernel",
	}

	for raw, want := range shaped {
		got, err := kind.New(raw)
		if err != nil {
			t.Fatalf("%q refused: %v", raw, err)
		}

		if got.String() != want {
			t.Fatalf("%q became %q, want %q", raw, got.String(), want)
		}
	}
}

// Not folding has a price, and it is worth naming: two spellings are two
// kinds. A client that wants to be forgiving about case says so itself.
func TestKindIsCaseSensitive(t *testing.T) {
	t.Parallel()

	upper, err := kind.New("Kernel")
	if err != nil {
		t.Fatalf("upper: %v", err)
	}

	lower, err := kind.New("kernel")
	if err != nil {
		t.Fatalf("lower: %v", err)
	}

	if upper.Eq(lower) {
		t.Fatal("case-insensitive after all")
	}
}

// The alphabet is closed and ASCII: a Cyrillic "К" renders exactly like a
// Latin "K", and two kinds nobody can tell apart is the one thing a name
// must never allow.
func TestKindAlphabetIsClosed(t *testing.T) {
	t.Parallel()

	refused := map[string]error{
		"":                       str.ErrEmpty,
		"   ":                    str.ErrEmpty,
		"Кernel":                 str.ErrNotAllowed, // Cyrillic К
		"kernel lease":           str.ErrNotAllowed,
		"kernel_lease":           str.ErrNotAllowed,
		"aws/vm":                 str.ErrNotAllowed,
		"a\x1eb":                 str.ErrForbidden,
		"na\u200bme":             str.ErrForbidden,
		strings.Repeat("x", 129): str.ErrTooLong,
	}

	for raw, want := range refused {
		if _, err := kind.New(raw); !errors.Is(err, want) {
			t.Fatalf("%q: want %v, got %v", raw, want, err)
		}
	}
}

// A name begins with a letter, ends on a letter or a digit, and never
// doubles a separator — so one name has exactly one spelling.
func TestKindShapeHasOneSpellingPerName(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"1kernel", "-kernel", ".kernel", "kernel-", "kernel.", "aws..vm", "aws.-vm"} {
		if _, err := kind.New(raw); !errors.Is(err, str.ErrPattern) {
			t.Fatalf("%q: want ErrPattern, got %v", raw, err)
		}
	}
}

func TestZeroKind(t *testing.T) {
	t.Parallel()

	var zero kind.Kind
	if !zero.IsZero() {
		t.Fatal("the zero kind is not zero")
	}

	built, err := kind.New("Kernel")
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	if built.IsZero() {
		t.Fatal("a built kind reported itself unset")
	}
}
