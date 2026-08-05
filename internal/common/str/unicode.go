package str

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/unicode/norm"
)

// Unicode is where a string type earns its keep. Everything here exists
// because two strings that a person reads as identical can be different
// bytes — and a system that stores both ends up with two resources nobody
// can tell apart, or a name that cannot be retyped from what is on screen.

// NFC composes: "é" written as one character and as "e" plus a combining
// accent become the same bytes.
//
// This belongs first in almost every list. Without it the two spellings
// are two different keys, they sort apart, they compare unequal, and no
// amount of staring at the screen explains why.
func NFC() Rule { return normalize("nfc", norm.NFC) }

// NFD decomposes — the opposite of NFC. Rarely what a key wants; useful
// when something downstream needs to strip accents.
func NFD() Rule { return normalize("nfd", norm.NFD) }

// NFKC composes AND folds compatibility characters: the ligature "ﬁ"
// becomes "fi", fullwidth "ａ" becomes "a", "①" becomes "1".
//
// Use it for identifiers. Those characters exist to preserve the
// appearance of old encodings, and in a name they are a way to write
// something that looks like an existing name and is not it. NFC alone
// leaves them alone.
func NFKC() Rule { return normalize("nfkc", norm.NFKC) }

// NFKD is NFKC without the composing step.
func NFKD() Rule { return normalize("nfkd", norm.NFKD) }

func normalize(name string, form norm.Form) Rule {
	return Rule{name: name, apply: func(value string) (string, error) {
		return form.String(value), nil
	}}
}

// Fold case-folds for comparison, which is NOT the same as lowercasing.
//
// Lowercasing answers "how is this written in lower case"; folding
// answers "are these the same word". German "ß" folds to "ss" and lowers
// to "ß"; they are different questions, and identifiers ask the second.
// Use Fold when the value is a key, Lower when it is shown to a person.
func Fold() Rule {
	return Rule{name: "fold", apply: func(value string) (string, error) {
		// A Caser carries state and must not be shared between
		// goroutines, so it is built per call rather than kept in the
		// rule. The rule is a value that anyone may copy anywhere.
		return cases.Fold().String(value), nil
	}}
}

// Lower and Upper change case for display. For anything compared or used
// as a key, see Fold.
//
// They go through x/text rather than strings.ToLower, and the difference
// is not pedantry: the stdlib maps rune by rune, so German "ß" upcases to
// itself and Turkish "i" loses its dot. One casing engine per package, or
// two rules in the same list disagree about what the same word is.
func Lower() Rule {
	return recase("lower", cases.Lower)
}

// Upper changes case upward.
func Upper() Rule {
	return recase("upper", cases.Upper)
}

func recase(name string, build func(language.Tag, ...cases.Option) cases.Caser) Rule {
	return Rule{name: name, apply: func(value string) (string, error) {
		// A Caser carries state and must not be shared between
		// goroutines, so it is built per call. The Rule itself is a value
		// anyone may copy anywhere, which is worth more than the saving.
		return build(language.Und).String(value), nil
	}}
}

// UTF8 refuses bytes that are not text.
//
// Worth stating first because everything after assumes text: a character
// count over invalid bytes is a number with no meaning, and a regexp over
// them matches whatever the replacement character happens to do.
func UTF8() Rule {
	return Rule{name: "utf8", apply: func(value string) (string, error) {
		if !utf8.ValidString(value) {
			return "", refuse("utf8", value, "", ErrNotUTF8)
		}

		return value, nil
	}}
}

// NoInvisible refuses characters that take up no space on screen:
// controls, format characters, and every space that is not a plain one.
//
// This is the rule that stops a name from being a trap. A zero-width
// space or a non-breaking space inside a value produces something that
// looks exactly like the value beside it, compares unequal to it, and
// cannot be reproduced by typing what is displayed.
func NoInvisible() Rule {
	return Rule{name: "no_invisible", apply: func(value string) (string, error) {
		index := strings.IndexFunc(value, func(candidate rune) bool {
			switch {
			case candidate == ' ':
				return false
			case unicode.IsControl(candidate), unicode.IsSpace(candidate):
				return true
			default:
				return unicode.In(candidate, unicode.Cf)
			}
		})
		if index >= 0 {
			offending, _ := utf8.DecodeRuneInString(value[index:])

			return "", refuse("no_invisible", value,
				runeAt(offending, index), ErrForbidden)
		}

		return value, nil
	}}
}

// NoMarks refuses combining marks left standing on their own.
//
// After NFC most marks have joined the character before them; the ones
// that remain are stacking accents — the way a name is made unreadable
// while still passing every other rule.
func NoMarks() Rule {
	return NotInCategories("no_marks", unicode.M)
}

// InCategories permits only characters in the given Unicode categories,
// e.g. unicode.L and unicode.N for "letters and digits in any script".
//
// Prefer it to a hand-written rune range: a range says "the alphabet I
// happened to think of", a category says what it means and keeps meaning
// it when someone writes their name in Devanagari.
func InCategories(name string, tables ...*unicode.RangeTable) Rule {
	return Allow(name, func(candidate rune) bool {
		return unicode.In(candidate, tables...)
	})
}

// NotInCategories refuses characters in the given categories — the same
// question as InCategories asked the other way round, because some rules
// read honestly as "only these" and others as "never these".
func NotInCategories(name string, tables ...*unicode.RangeTable) Rule {
	return Rule{name: name, apply: func(value string) (string, error) {
		index := strings.IndexFunc(value, func(candidate rune) bool {
			return unicode.In(candidate, tables...)
		})
		if index >= 0 {
			offending, _ := utf8.DecodeRuneInString(value[index:])

			return "", refuse(name, value, runeAt(offending, index), ErrForbidden)
		}

		return value, nil
	}}
}
