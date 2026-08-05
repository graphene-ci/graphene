package path_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/graphene-ci/graphene/internal/common/str"
	"github.com/graphene-ci/graphene/internal/types/path"
)

// shape is what the tests fill in: a kind that puts a tenant above a
// name, the commonest arrangement there is.
func shape(t *testing.T) path.TPath {
	t.Helper()

	built, err := path.NewTPath("tenant", "name")
	if err != nil {
		t.Fatalf("build shape: %v", err)
	}

	return built
}

// A segment name is decided in one place. These are the answers that
// place gives; the reasons are in the rule list, not here.
func TestSegmentIsNormalizedAndChecked(t *testing.T) {
	t.Parallel()

	shaped := map[string]string{
		"  kernel  ": "kernel",
		"Kernel":     "kernel",
		"STRASSE":    "strasse",
		"ｋｅｒｎｅｌ":     "kernel",
	}

	for raw, want := range shaped {
		got, err := path.NewTPathSegment(raw)
		if err != nil {
			t.Fatalf("%q refused: %v", raw, err)
		}

		if got.String() != want {
			t.Fatalf("%q became %q, want %q", raw, got.String(), want)
		}
	}

	refused := map[string]error{
		"":                       str.ErrEmpty,
		"   ":                    str.ErrEmpty,
		"a/b":                    str.ErrForbidden,
		"a\x1eb":                 str.ErrForbidden,
		"a\x1fb":                 str.ErrForbidden,
		"na​me":                  str.ErrForbidden,
		strings.Repeat("x", 257): str.ErrTooLong,
	}

	for raw, want := range refused {
		if _, err := path.NewTPathSegment(raw); !errors.Is(err, want) {
			t.Fatalf("%q: want %v, got %v", raw, want, err)
		}
	}
}

// Two spellings a person reads as one segment must BE one segment, or the
// same path names two different things.
func TestSegmentsThatLookAlikeAreEqual(t *testing.T) {
	t.Parallel()

	first, err := path.NewTPathSegment("Kernel")
	if err != nil {
		t.Fatalf("first: %v", err)
	}

	second, err := path.NewTPathSegment(" kernel ")
	if err != nil {
		t.Fatalf("second: %v", err)
	}

	if !first.Eq(second) {
		t.Fatalf("%q and %q stayed different", first, second)
	}
}

// A shape names each position once. Twice makes every message about "the
// tenant segment" ambiguous, and a shape is written by hand exactly once,
// so there is nothing to gain by allowing it.
func TestShapeRefusesADuplicateName(t *testing.T) {
	t.Parallel()

	if _, err := path.NewTPath("tenant", "Tenant"); !errors.Is(err, path.ErrDuplicateName) {
		t.Fatalf("want ErrDuplicateName, got %v", err)
	}

	built := shape(t)
	if built.Arity() != 2 {
		t.Fatalf("arity %d", built.Arity())
	}

	if built.String() != "/tenant/name" {
		t.Fatalf("shape reads as %q", built.String())
	}
}

// The value gets the same rules as the name, and needs them more: a name
// is written once by whoever declared the kind, a value comes from
// whoever is calling.
func TestValuesArePutThroughTheRules(t *testing.T) {
	t.Parallel()

	built, err := shape(t).New("  ACME ", "K1")
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	if built.String() != "/acme/k1" {
		t.Fatalf("path reads as %q", built.String())
	}

	if _, err := shape(t).New("acme", "a/b"); !errors.Is(err, str.ErrForbidden) {
		t.Fatalf("a separator inside a value: want ErrForbidden, got %v", err)
	}

	if _, err := shape(t).New("acme", ""); !errors.Is(err, str.ErrEmpty) {
		t.Fatalf("an empty value: want ErrEmpty, got %v", err)
	}
}

// Fewer values than the shape is a PREFIX and is the same type as a whole
// path — asking for "everything under acme" is a real thing to do. More
// values is not a longer path, it is a path of some other kind.
func TestShorterIsAPrefixAndLongerIsRefused(t *testing.T) {
	t.Parallel()

	built := shape(t)

	prefix, err := built.New("acme")
	if err != nil {
		t.Fatalf("prefix: %v", err)
	}

	if prefix.IsExact() {
		t.Fatal("a short path claimed to name one resource")
	}

	whole, err := built.New("acme", "k1")
	if err != nil {
		t.Fatalf("whole: %v", err)
	}

	if !whole.IsExact() {
		t.Fatal("a full path did not name one resource")
	}

	if _, err := built.New("acme", "k1", "extra"); !errors.Is(err, path.ErrArity) {
		t.Fatalf("want ErrArity, got %v", err)
	}
}

// Prefix matching is what grants, scans and watches are all built out of,
// so it has to match whole segments — "acme" must not cover "acme2".
func TestPrefixMatchesWholeSegments(t *testing.T) {
	t.Parallel()

	built := shape(t)

	under := func(values ...string) path.Path {
		t.Helper()

		made, err := built.New(values...)
		if err != nil {
			t.Fatalf("build %v: %v", values, err)
		}

		return made
	}

	whole := under("acme", "k1")

	if !whole.HasPrefix(under("acme")) {
		t.Fatal("acme/k1 is not under acme")
	}

	if !whole.HasPrefix(under("acme", "k1")) {
		t.Fatal("a path is not under itself")
	}

	if whole.HasPrefix(under("acme2")) {
		t.Fatal("acme/k1 counted as under acme2")
	}

	if under("acme").HasPrefix(whole) {
		t.Fatal("a prefix counted as being under something longer than it")
	}
}

// A path knows its own shape, so the same values under different shapes
// are different paths: one is a tenant and a name, the other is whatever
// else happened to be spelled that way.
func TestShapeIsPartOfThePath(t *testing.T) {
	t.Parallel()

	tenants, err := path.NewTPath("tenant", "name")
	if err != nil {
		t.Fatalf("tenants: %v", err)
	}

	environments, err := path.NewTPath("env", "name")
	if err != nil {
		t.Fatalf("environments: %v", err)
	}

	first, err := tenants.New("acme", "k1")
	if err != nil {
		t.Fatalf("first: %v", err)
	}

	second, err := environments.New("acme", "k1")
	if err != nil {
		t.Fatalf("second: %v", err)
	}

	if first.Eq(second) {
		t.Fatal("paths of different shapes compared equal")
	}

	if first.HasPrefix(second) {
		t.Fatal("a path counted as being under a path of another shape")
	}

	// They still read the same, because a path reads as its values.
	if first.String() != second.String() {
		t.Fatalf("%q and %q", first, second)
	}
}

// A written path is split back into the same one, which is only safe
// because '/' is a character a value may never contain.
func TestParseIsTheInverseOfString(t *testing.T) {
	t.Parallel()

	built := shape(t)

	whole, err := built.Parse("/ACME/k1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if whole.String() != "/acme/k1" || !whole.IsExact() {
		t.Fatalf("parsed to %q, exact=%v", whole.String(), whole.IsExact())
	}

	again, err := built.Parse(whole.String())
	if err != nil {
		t.Fatalf("reparse: %v", err)
	}

	if !again.Eq(whole) {
		t.Fatalf("%q did not survive a round trip", whole)
	}

	// Nothing at all is the whole kind, not an error.
	empty, err := built.Parse("")
	if err != nil || empty.Len() != 0 || empty.IsExact() {
		t.Fatalf("empty: len=%d exact=%v err=%v", empty.Len(), empty.IsExact(), err)
	}
}

// Walking a path gives the position and the value together; there is no
// pair type to explain because the pair has no life of its own.
func TestWalkingGivesPositionAndValue(t *testing.T) {
	t.Parallel()

	whole, err := shape(t).New("acme", "k1")
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	seen := map[string]string{}
	for name, value := range whole.All() {
		seen[name.String()] = value.String()
	}

	if seen["tenant"] != "acme" || seen["name"] != "k1" {
		t.Fatalf("walked to %v", seen)
	}
}

// A shape nobody built fills nothing: the zero value is not a back door.
func TestZeroShapeFillsNothing(t *testing.T) {
	t.Parallel()

	var zero path.TPath

	if !zero.IsZero() || zero.Arity() != 0 {
		t.Fatal("a zero shape claimed to have positions")
	}

	if _, err := zero.New("acme"); !errors.Is(err, path.ErrArity) {
		t.Fatalf("want ErrArity, got %v", err)
	}

	empty, err := zero.New()
	if err != nil {
		t.Fatalf("zero shape, no values: %v", err)
	}

	if !empty.IsZero() {
		t.Fatal("a path from a zero shape was not zero")
	}
}
