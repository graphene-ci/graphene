// Package str is the whole life of one string: bring it to the form the
// system stores, refuse it if it is still not usable, and hand back a
// value that cannot be anything else.
//
// The type is the point — up to a point. A function returning
// (string, error) leaves the caller holding an ordinary string that looks
// exactly like an unchecked one, and the next person cannot tell them
// apart. A named type can.
//
// What it does NOT do is make forgery impossible: TPathSegment("a/b") is
// a legal conversion, and no constraint over ~string can prevent it. The
// type states where a value is meant to come from; the rules are what
// make it true. A struct with a private field would have made it
// airtight, at the cost of a type that cannot be a map key, compared with
// ==, or printed without ceremony — and that cost is paid on every use,
// while forgery costs a deliberate conversion someone has to write.
//
// Normalizing and judging are ONE ordered list, not two. A rule judges
// what the rules before it produced — a length checked before trimming,
// or a character set checked before folding, answers a question nobody
// asked. The order lives in the list, so it cannot drift.
//
//	var segment = []str.Rule{
//		str.NFC(),
//		str.TrimSpace(),
//		str.Fold(),
//		str.NotEmpty(),
//		str.MaxBytes(256),
//		str.NoInvisible(),
//		str.Forbid('/', 0x1E, 0x1F),
//	}
//
//	value, err := str.New(raw, segment...)
//
// The package is named str, not string: importing a package named string
// would take the builtin type out of scope in every file that imports it,
// and "var s string" would stop compiling there.
package str

import "unicode/utf8"

// String is text that has been through the rules. Its zero value is the
// empty string and means "nothing here": a rule list with NotEmpty can
// never produce it, so a zero String is always something the caller
// declared and never something New returned.
type String string

// New runs raw through the rules, in order, and returns it as whatever
// named string type the caller asked for:
//
//	segment, err := str.New[TPathSegment](raw, segmentRules...)
//
// The type argument cannot be inferred — T appears only in the result —
// so it is always written out. That is the price of handing back the
// caller's own type instead of one they have to convert.
//
// New stops at the first refusal rather than collecting every complaint:
// each rule reads what the ones before it produced, so continuing past a
// refusal would be judging a value already called wrong.
func New[T ~string](raw string, rules ...Rule) (T, error) {
	value := raw

	var err error

	for _, rule := range rules {
		if value, err = rule.apply(value); err != nil {
			var zero T

			return zero, err
		}
	}

	return T(value), nil
}

// String returns the text.
func (s String) String() string { return string(s) }

// Eq compares two normalized values.
//
// Comparison is exact and can afford to be: whatever made two spellings
// of the same text differ — case, accent composition, stray spaces — was
// settled by the rules that produced them. Comparing values made under
// DIFFERENT rules compares two different questions.
func (s String) Eq(other String) bool { return string(s) == string(other) }

// IsZero reports the empty value.
func (s String) IsZero() bool { return string(s) == "" }

// Len is the size in bytes; RuneLen is the count of characters. Both are
// here because both get asked for, and confusing them is how a limit
// comes to mean something else in another alphabet.
func (s String) Len() int { return len(s) }

// RuneLen is the count of characters.
func (s String) RuneLen() int { return utf8.RuneCountInString(string(s)) }

// Rule is one step. It either rewrites the value or refuses it; nothing
// separates a "normalizer" from a "validator" except which of the two it
// happens to do.
type Rule struct {
	name  string
	apply func(string) (string, error)
}

// Name reports the rule's name — the same one a refusal carries.
func (r Rule) Name() string { return r.name }
