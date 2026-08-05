package path_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/graphene-ci/graphene/internal/common/str"
	"github.com/graphene-ci/graphene/internal/types/path"
)

// A field path reads the way the resource's yaml reads.
func TestFieldPathReadsLikeTheDocument(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"spec.blob", "status.phase", "key.kind", "revision", "spec.ttl_seconds"} {
		parsed, err := path.ParseFieldPath(raw)
		if err != nil {
			t.Fatalf("%q refused: %v", raw, err)
		}

		if parsed.String() != raw {
			t.Fatalf("%q did not survive a round trip: %q", raw, parsed.String())
		}
	}
}

// Names are NOT folded: they have to match a schema byte for byte, and
// "bundleVersion" folded is in no schema anywhere.
func TestFieldNamesKeepTheirCase(t *testing.T) {
	t.Parallel()

	parsed, err := path.ParseFieldPath("spec.bundleVersion")
	if err != nil {
		t.Fatalf("refused: %v", err)
	}

	if parsed.String() != "spec.bundleVersion" {
		t.Fatalf("case was changed: %q", parsed.String())
	}
}

// What a name may be. Underscore is in because our schemas are snake_case.
func TestFieldNameAlphabet(t *testing.T) {
	t.Parallel()

	refused := map[string]error{
		"":                       str.ErrEmpty,
		"1blob":                  str.ErrPattern,
		"_blob":                  str.ErrPattern,
		"spec blob":              str.ErrNotAllowed,
		"spec-blob":              str.ErrNotAllowed,
		"blоb":                   str.ErrNotAllowed, // Cyrillic о
		strings.Repeat("x", 129): str.ErrTooLong,
	}

	for raw, want := range refused {
		if _, err := path.NewFieldName(raw); !errors.Is(err, want) {
			t.Fatalf("%q: want %v, got %v", raw, want, err)
		}
	}

	if _, err := path.NewFieldName("ttl_seconds"); err != nil {
		t.Fatalf("snake_case refused: %v", err)
	}
}

// A path naming nothing addresses nothing: there is no "the whole
// resource" to point at.
func TestEmptyFieldPathIsRefused(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"", "   "} {
		if _, err := path.ParseFieldPath(raw); !errors.Is(err, path.ErrEmptyFieldPath) {
			t.Fatalf("%q: want ErrEmptyFieldPath, got %v", raw, err)
		}
	}

	if _, err := path.NewFieldPath(); !errors.Is(err, path.ErrEmptyFieldPath) {
		t.Fatalf("no steps: want ErrEmptyFieldPath, got %v", err)
	}

	// A trailing separator leaves an empty step, which is a typo and not
	// a path to the parent.
	if _, err := path.ParseFieldPath("spec."); !errors.Is(err, str.ErrEmpty) {
		t.Fatalf("trailing separator: want ErrEmpty, got %v", err)
	}
}

// Walking is head and rest, which is how a resolver descends a schema.
func TestWalkingAFieldPath(t *testing.T) {
	t.Parallel()

	parsed, err := path.ParseFieldPath("spec.limits.cpu")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if parsed.Head().String() != "spec" || parsed.Len() != 3 {
		t.Fatalf("head %q, len %d", parsed.Head(), parsed.Len())
	}

	rest := parsed.Rest()
	if rest.String() != "limits.cpu" {
		t.Fatalf("rest: %q", rest.String())
	}

	last := rest.Rest()
	if last.String() != "cpu" || !last.Rest().IsZero() {
		t.Fatalf("last: %q, after it zero=%v", last.String(), last.Rest().IsZero())
	}
}
