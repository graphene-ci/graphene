package str_test

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"unicode"

	"github.com/graphene-ci/graphene/internal/common/str"
)

// segment is the shape a path segment has to hold, written the way a
// caller writes it: a package-level list, built once, passed to New.
var segment = []str.Rule{
	str.UTF8(),
	str.NFKC(),
	str.TrimSpace(),
	str.Fold(),
	str.NotEmpty(),
	str.MaxBytes(256),
	str.NoInvisible(),
	str.Forbid('/', 0x1E, 0x1F),
	str.Match(regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)),
}

// The type is the point: what comes out is not an ordinary string, and
// what goes in cannot become one without passing.
func TestOnlyNewProducesAValue(t *testing.T) {
	t.Parallel()

	value, err := str.New[str.String]("  Kernel-1 ", segment...)
	if err != nil {
		t.Fatalf("refused: %v", err)
	}

	if value.String() != "kernel-1" {
		t.Fatalf("normalized to %q", value.String())
	}

	// A zero value is the empty string and nothing New ever returns from
	// a list carrying NotEmpty.
	var zero str.String
	if !zero.IsZero() || zero.String() != "" {
		t.Fatalf("zero value: %q", zero.String())
	}
}

// A rule judges what the rules before it produced. Reverse the order and
// the same input gets a different answer — which is why the order is the
// caller's to state and not ours to guess.
func TestOrderDecidesTheAnswer(t *testing.T) {
	t.Parallel()

	const padded = "  ok  "

	if got, err := str.New[str.String](padded, str.TrimSpace(), str.MaxBytes(2)); err != nil || got.String() != "ok" {
		t.Fatalf("trim then measure: %q, %v", got.String(), err)
	}

	if _, err := str.New[str.String](padded, str.MaxBytes(2), str.TrimSpace()); !errors.Is(err, str.ErrTooLong) {
		t.Fatalf("measure then trim: want ErrTooLong, got %v", err)
	}
}

// Case folding before an enumeration is why shaping and judging share one
// list: split them and every caller folds by hand before asking.
func TestFoldingFeedsTheEnumeration(t *testing.T) {
	t.Parallel()

	platform := []str.Rule{str.TrimSpace(), str.Fold(), str.OneOf("linux", "darwin", "windows")}

	got, err := str.New[str.String]("  Linux ", platform...)
	if err != nil || got.String() != "linux" {
		t.Fatalf("got %q, %v", got.String(), err)
	}

	if _, err := str.New[str.String]("plan9", platform...); !errors.Is(err, str.ErrNotOneOf) {
		t.Fatalf("want ErrNotOneOf, got %v", err)
	}
}

// Two spellings a person reads as one must become one value, or they
// become two resources nobody can tell apart.
func TestCanonicalSpelling(t *testing.T) {
	t.Parallel()

	const (
		composed   = "é"   // é
		decomposed = "é"  // e + combining acute
		ligature   = "ﬁle" // ﬁle
		fullwidth  = "ａｂ"  // ａｂ
	)

	if composed == decomposed {
		t.Fatal("the inputs are already equal; the test proves nothing")
	}

	first, err := str.New[str.String](composed, str.NFC())
	if err != nil {
		t.Fatalf("composed: %v", err)
	}

	second, err := str.New[str.String](decomposed, str.NFC())
	if err != nil {
		t.Fatalf("decomposed: %v", err)
	}

	if !first.Eq(second) {
		t.Fatalf("the same text stayed two values: %q != %q", first, second)
	}

	// NFKC goes further, and identifiers need it: these are the
	// characters someone uses to write a name that LOOKS like yours.
	folded, err := str.New[str.String](ligature, str.NFKC())
	if err != nil || folded.String() != "file" {
		t.Fatalf("ligature: %q, %v", folded.String(), err)
	}

	if wide, err := str.New[str.String](fullwidth, str.NFKC()); err != nil || wide.String() != "ab" {
		t.Fatalf("fullwidth: %q, %v", wide.String(), err)
	}
}

// Folding is not lowercasing: one asks how a word is written, the other
// whether two words are the same. An identifier asks the second.
func TestFoldingIsNotLowercasing(t *testing.T) {
	t.Parallel()

	const sharpS = "STRASSE"

	lowered, err := str.New[str.String]("straße", str.Upper())
	if err != nil {
		t.Fatalf("upper: %v", err)
	}

	folded, err := str.New[str.String]("straße", str.Fold())
	if err != nil {
		t.Fatalf("fold: %v", err)
	}

	if folded.String() != "strasse" {
		t.Fatalf("fold produced %q, want %q", folded.String(), "strasse")
	}

	// And Upper agrees with it, because both go through the same casing
	// engine: the stdlib would have left "ß" alone here.
	if lowered.String() != sharpS {
		t.Fatalf("upper produced %q, want %q", lowered.String(), sharpS)
	}
}

// The rule that stops a name from being a trap: something that looks
// exactly like the value beside it and compares unequal to it.
func TestInvisibleCharactersAreRefused(t *testing.T) {
	t.Parallel()

	traps := map[string]string{
		"zero width space": "na​me",
		"non-breaking":     "na me",
		"soft hyphen":      "na­me",
		"tab":              "na\tme",
	}

	for what, value := range traps {
		if _, err := str.New[str.String](value, str.NoInvisible()); !errors.Is(err, str.ErrForbidden) {
			t.Fatalf("%s: want ErrForbidden, got %v", what, err)
		}
	}

	if _, err := str.New[str.String]("plain name", str.NoInvisible()); err != nil {
		t.Fatalf("an ordinary space was refused: %v", err)
	}
}

// Stacking marks pass every other rule and make a name unreadable.
func TestStackedMarksAreRefused(t *testing.T) {
	t.Parallel()

	const stacked = "á́́́"

	if _, err := str.New[str.String](stacked, str.NFC(), str.NoMarks()); !errors.Is(err, str.ErrForbidden) {
		t.Fatalf("want ErrForbidden, got %v", err)
	}

	// One accent that NFC composed away is not a mark any more.
	if _, err := str.New[str.String]("é", str.NFC(), str.NoMarks()); err != nil {
		t.Fatalf("a composed accent was refused: %v", err)
	}
}

// Categories say what they mean in every script; a rune range says "the
// alphabet I happened to think of".
func TestCategoriesSpanScripts(t *testing.T) {
	t.Parallel()

	letters := str.InCategories("letters_digits", unicode.L, unicode.N)

	for _, good := range []string{"kernel", "ядро", "カーネル", "kernel1"} {
		if _, err := str.New[str.String](good, letters); err != nil {
			t.Fatalf("%q refused: %v", good, err)
		}
	}

	if _, err := str.New[str.String]("kernel-1", letters); !errors.Is(err, str.ErrNotAllowed) {
		t.Fatalf("want ErrNotAllowed, got %v", err)
	}
}

// Characters and bytes are different limits, and a Cyrillic character is
// two of the latter.
func TestRunesAreNotBytes(t *testing.T) {
	t.Parallel()

	const cyrillic = "ядро" // 4 characters, 8 bytes

	if _, err := str.New[str.String](cyrillic, str.MaxRunes(4)); err != nil {
		t.Fatalf("4 characters refused by a 4-character limit: %v", err)
	}

	if _, err := str.New[str.String](cyrillic, str.MaxBytes(4)); !errors.Is(err, str.ErrTooLong) {
		t.Fatalf("8 bytes accepted by a 4-byte limit: %v", err)
	}
}

// Dropping and forbidding are different intentions: noise gets swept up,
// ambiguity gets handed back.
func TestDropRemovesWhatForbidRefuses(t *testing.T) {
	t.Parallel()

	const pasted = "na​me"

	dropped, err := str.New[str.String](pasted, str.Drop('​'))
	if err != nil || dropped.String() != "name" {
		t.Fatalf("drop: %q, %v", dropped.String(), err)
	}

	if _, err := str.New[str.String](pasted, str.Forbid('​')); !errors.Is(err, str.ErrForbidden) {
		t.Fatalf("forbid: want ErrForbidden, got %v", err)
	}
}

// A refusal names the rule and shows the value AS THAT RULE SAW IT — not
// what the caller passed, because earlier rules rewrote it.
func TestRefusalNamesTheRuleAndWhatItSaw(t *testing.T) {
	t.Parallel()

	_, err := str.New[str.String]("  A/B  ", str.TrimSpace(), str.Fold(), str.Forbid('/'))

	var refusal *str.Error
	if !errors.As(err, &refusal) {
		t.Fatalf("not a *str.Error: %v", err)
	}

	if refusal.Rule != "forbid" {
		t.Fatalf("rule: %q", refusal.Rule)
	}

	if refusal.Value != "a/b" {
		t.Fatalf("value as seen: %q, want %q", refusal.Value, "a/b")
	}

	if !errors.Is(err, str.ErrForbidden) {
		t.Fatalf("sentinel lost: %v", err)
	}

	if !strings.Contains(err.Error(), "forbid") {
		t.Fatalf("message does not name the rule: %s", err)
	}
}

// The whole list, on the values it will actually meet.
func TestSegmentShape(t *testing.T) {
	t.Parallel()

	for _, good := range []string{"k1", "  Kernel-1  ", "a.b_c"} {
		if _, err := str.New[str.String](good, segment...); err != nil {
			t.Fatalf("%q refused: %v", good, err)
		}
	}

	refusals := map[string]error{
		"":                       str.ErrEmpty,
		"   ":                    str.ErrEmpty,
		"a/b":                    str.ErrForbidden,
		"a\x1eb":                 str.ErrForbidden,
		"a\x00b":                 str.ErrForbidden,
		"na​me":                  str.ErrForbidden,
		"-leading":               str.ErrPattern,
		"ядро":                   str.ErrPattern,
		strings.Repeat("x", 257): str.ErrTooLong,
	}

	for value, want := range refusals {
		if _, err := str.New[str.String](value, segment...); !errors.Is(err, want) {
			t.Fatalf("%q: want %v, got %v", value, want, err)
		}
	}
}

// No rules is no opinions, not a failure.
func TestNoRulesPassesThrough(t *testing.T) {
	t.Parallel()

	got, err := str.New[str.String](" as is ")
	if err != nil || got.String() != " as is " {
		t.Fatalf("got %q, %v", got.String(), err)
	}
}

// The shape rules answer a question an alphabet cannot: not which
// characters may appear, but where. Each names its own refusal, which a
// single pattern could not.
func TestShapeRulesConstrainPositions(t *testing.T) {
	t.Parallel()

	shape := []str.Rule{
		str.Alphabet('.', '-'),
		str.BeginsWith("begins_with_letter", str.IsASCIILetter),
		str.EndsWith("ends_with_letter_or_digit", str.IsASCIIAlphanumeric),
		str.NoAdjacent('.', '-'),
	}

	for _, good := range []string{"Kernel", "KernelLease", "kernel-lease", "aws.vm", "aws.Vm-Small", "k8s"} {
		if _, err := str.New[str.String](good, shape...); err != nil {
			t.Fatalf("%q refused: %v", good, err)
		}
	}

	// Each refusal names the rule that made it, so the message says which
	// question was answered no.
	refusals := map[string]string{
		"1kernel": "begins_with_letter",
		"-kernel": "begins_with_letter",
		"kernel-": "ends_with_letter_or_digit",
		"kernel.": "ends_with_letter_or_digit",
		"aws..vm": "no_adjacent",
		"aws.-vm": "no_adjacent",
		"aws/vm":  "alphabet",
		"Кernel":  "alphabet", // Cyrillic К
		"a_b":     "alphabet",
	}

	for value, rule := range refusals {
		_, err := str.New[str.String](value, shape...)

		var refusal *str.Error
		if !errors.As(err, &refusal) {
			t.Fatalf("%q: not a refusal: %v", value, err)
		}

		if refusal.Rule != rule {
			t.Fatalf("%q refused by %q, want %q", value, refusal.Rule, rule)
		}
	}
}

// An alphabet says what it allows, because a refusal that only says "not
// allowed" leaves the reader to guess what was.
func TestAlphabetSaysWhatItPermits(t *testing.T) {
	t.Parallel()

	_, err := str.New[str.String]("a/b", str.Alphabet('.', '-'))

	var refusal *str.Error
	if !errors.As(err, &refusal) {
		t.Fatalf("not a refusal: %v", err)
	}

	if !strings.Contains(refusal.Detail, "letters, digits") || !strings.Contains(refusal.Detail, "'.'") {
		t.Fatalf("detail does not say what is allowed: %q", refusal.Detail)
	}
}

// An edge rule has nothing to look at in an empty value, and says so
// rather than reporting a shape problem it cannot have seen.
func TestEdgeRulesOnAnEmptyValue(t *testing.T) {
	t.Parallel()

	if _, err := str.New[str.String]("", str.BeginsWith("first", str.IsASCIILetter)); !errors.Is(err, str.ErrEmpty) {
		t.Fatalf("want ErrEmpty, got %v", err)
	}
}
