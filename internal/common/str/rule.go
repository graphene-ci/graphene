package str

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

// --- shaping ------------------------------------------------------------

// TrimSpace removes leading and trailing whitespace.
func TrimSpace() Rule {
	return Rule{name: "trim_space", apply: func(value string) (string, error) {
		return strings.TrimSpace(value), nil
	}}
}

// Trim removes leading and trailing characters from the cutset.
func Trim(cutset string) Rule {
	return Rule{name: "trim", apply: func(value string) (string, error) {
		return strings.Trim(value, cutset), nil
	}}
}

// CollapseSpaces turns every run of whitespace into a single space.
// It leaves the edges alone; pair it with TrimSpace.
func CollapseSpaces() Rule {
	return Rule{name: "collapse_spaces", apply: func(value string) (string, error) {
		return strings.Join(strings.Fields(value), " "), nil
	}}
}

// Replace rewrites every occurrence of old.
func Replace(old, replacement string) Rule {
	return Rule{name: "replace", apply: func(value string) (string, error) {
		return strings.ReplaceAll(value, old, replacement), nil
	}}
}

// Map rewrites characters; whatever the mapping returns negative is
// dropped, which is how a rule removes rather than refuses.
func Map(name string, mapping func(rune) rune) Rule {
	return Rule{name: name, apply: func(value string) (string, error) {
		return strings.Map(mapping, value), nil
	}}
}

// Drop removes the given characters instead of refusing them.
//
// The difference from Forbid is intent, and it matters: a zero-width
// space someone pasted is noise to be swept up, while a path separator
// means the value was meant for something else and should be handed back.
func Drop(runes ...rune) Rule {
	dropped := slices.Clone(runes)

	return Map("drop", func(candidate rune) rune {
		if slices.Contains(dropped, candidate) {
			return -1
		}

		return candidate
	})
}

// --- judging ------------------------------------------------------------

// NotEmpty refuses a value with nothing in it.
func NotEmpty() Rule {
	return Rule{name: "not_empty", apply: func(value string) (string, error) {
		if value == "" {
			return "", refuse("not_empty", value, "", ErrEmpty)
		}

		return value, nil
	}}
}

// MinRunes and MaxRunes count CHARACTERS; MinBytes and MaxBytes count
// STORAGE. Both exist because they answer different questions — one
// protects a person from an unreadable name, the other protects a key
// encoding — and one Cyrillic character is two of the latter.
func MinRunes(minimum int) Rule {
	return Rule{name: "min_runes", apply: func(value string) (string, error) {
		if count := utf8.RuneCountInString(value); count < minimum {
			return "", refuse("min_runes", value,
				fmt.Sprintf("%d < %d characters", count, minimum), ErrTooShort)
		}

		return value, nil
	}}
}

// MaxRunes refuses more characters than the limit.
func MaxRunes(maximum int) Rule {
	return Rule{name: "max_runes", apply: func(value string) (string, error) {
		if count := utf8.RuneCountInString(value); count > maximum {
			return "", refuse("max_runes", value,
				fmt.Sprintf("%d > %d characters", count, maximum), ErrTooLong)
		}

		return value, nil
	}}
}

// MinBytes refuses fewer bytes than the limit.
func MinBytes(minimum int) Rule {
	return Rule{name: "min_bytes", apply: func(value string) (string, error) {
		if len(value) < minimum {
			return "", refuse("min_bytes", value,
				fmt.Sprintf("%d < %d bytes", len(value), minimum), ErrTooShort)
		}

		return value, nil
	}}
}

// MaxBytes refuses more bytes than the limit.
func MaxBytes(maximum int) Rule {
	return Rule{name: "max_bytes", apply: func(value string) (string, error) {
		if len(value) > maximum {
			return "", refuse("max_bytes", value,
				fmt.Sprintf("%d > %d bytes", len(value), maximum), ErrTooLong)
		}

		return value, nil
	}}
}

// Forbid refuses a value containing any of the given characters.
//
// This is the rule for characters that carry meaning somewhere else — a
// separator in a key encoding, a path delimiter. Their presence does not
// make a value ugly, it makes it ambiguous.
func Forbid(runes ...rune) Rule {
	forbidden := slices.Clone(runes)

	return Rule{name: "forbid", apply: func(value string) (string, error) {
		index := strings.IndexFunc(value, func(candidate rune) bool {
			return slices.Contains(forbidden, candidate)
		})
		if index >= 0 {
			offending, _ := utf8.DecodeRuneInString(value[index:])

			return "", refuse("forbid", value, runeAt(offending, index), ErrForbidden)
		}

		return value, nil
	}}
}

// Allow refuses anything the predicate rejects — Forbid stated as a
// closed vocabulary. Prefer it when the good characters are known:
// a list of what is allowed cannot be outgrown by a character nobody
// thought of. For Unicode categories, see InCategories.
func Allow(name string, permitted func(rune) bool) Rule {
	return Rule{name: name, apply: func(value string) (string, error) {
		index := strings.IndexFunc(value, func(candidate rune) bool {
			return !permitted(candidate)
		})
		if index >= 0 {
			offending, _ := utf8.DecodeRuneInString(value[index:])

			return "", refuse(name, value, runeAt(offending, index), ErrNotAllowed)
		}

		return value, nil
	}}
}

// ASCII permits only printable ASCII.
func ASCII() Rule {
	return Allow("ascii", func(candidate rune) bool {
		return candidate >= ' ' && candidate <= '~'
	})
}

// Match refuses a value that does not match the expression.
//
// It takes a COMPILED expression so this package never panics on a typo
// and never fails at build time: compiling belongs where the pattern is
// written, next to a package-level regexp.MustCompile the author can see.
func Match(pattern *regexp.Regexp) Rule {
	return Rule{name: "match", apply: func(value string) (string, error) {
		if !pattern.MatchString(value) {
			return "", refuse("match", value, pattern.String(), ErrPattern)
		}

		return value, nil
	}}
}

// OneOf refuses anything outside the enumerated values.
//
// It compares the NORMALIZED value, so listing "linux" and folding case
// earlier accepts "Linux" — which is the whole reason shaping and judging
// share one list.
func OneOf(permitted ...string) Rule {
	allowed := slices.Clone(permitted)

	return Rule{name: "one_of", apply: func(value string) (string, error) {
		if !slices.Contains(allowed, value) {
			return "", refuse("one_of", value, strings.Join(allowed, ", "), ErrNotOneOf)
		}

		return value, nil
	}}
}

// Check is the escape hatch: any predicate, with the sentinel it reports.
// Reach for it when a rule is genuinely specific to one caller — and put
// a named rule in this package instead when it turns out not to be.
func Check(name string, ok func(string) bool, reason error) Rule {
	return Rule{name: name, apply: func(value string) (string, error) {
		if !ok(value) {
			return "", refuse(name, value, "", reason)
		}

		return value, nil
	}}
}

// --- shape --------------------------------------------------------------

// The shape rules answer questions an alphabet cannot: not "which
// characters may appear" but "where they may appear". A name that begins
// with a digit, ends on a separator, or doubles one is a name with more
// than one plausible spelling — and one spelling per name is the whole
// point of putting a string through rules.

// IsASCIILetter, IsASCIIDigit and IsASCIIAlphanumeric are the predicates
// the shape rules are usually given.
//
// ASCII on purpose, and worth saying once: for an identifier a Cyrillic
// "К" renders exactly like a Latin "K", so an alphabet open to every
// script is an alphabet in which two names can be indistinguishable.
func IsASCIILetter(candidate rune) bool {
	return (candidate >= 'a' && candidate <= 'z') || (candidate >= 'A' && candidate <= 'Z')
}

// IsASCIIDigit reports a decimal digit.
func IsASCIIDigit(candidate rune) bool { return candidate >= '0' && candidate <= '9' }

// IsASCIIAlphanumeric reports a letter or a digit.
func IsASCIIAlphanumeric(candidate rune) bool {
	return IsASCIILetter(candidate) || IsASCIIDigit(candidate)
}

// Alphabet permits ASCII letters and digits, plus whatever separators the
// caller names — the commonest closed vocabulary there is.
func Alphabet(extra ...rune) Rule {
	permitted := slices.Clone(extra)

	return Rule{name: "alphabet", apply: func(value string) (string, error) {
		index := strings.IndexFunc(value, func(candidate rune) bool {
			return !IsASCIIAlphanumeric(candidate) && !slices.Contains(permitted, candidate)
		})
		if index >= 0 {
			offending, _ := utf8.DecodeRuneInString(value[index:])

			return "", refuse("alphabet", value,
				runeAt(offending, index)+", allowed: letters, digits"+alsoAllowed(permitted),
				ErrNotAllowed)
		}

		return value, nil
	}}
}

func alsoAllowed(extra []rune) string {
	if len(extra) == 0 {
		return ""
	}

	return " and " + strconv.QuoteRune(extra[0]) + repeatQuoted(extra[1:])
}

func repeatQuoted(extra []rune) string {
	var out strings.Builder
	for _, candidate := range extra {
		out.WriteString(", " + strconv.QuoteRune(candidate))
	}

	return out.String()
}

// BeginsWith and EndsWith constrain the edges. They take a name because
// the predicate has none: "kind_begins_with_letter" is what a refusal
// will say, and it should read as the rule the author had in mind.
func BeginsWith(name string, permitted func(rune) bool) Rule {
	return edge(name, permitted, func(value string) (rune, int) {
		first, _ := utf8.DecodeRuneInString(value)

		return first, 0
	})
}

// EndsWith constrains the last character.
func EndsWith(name string, permitted func(rune) bool) Rule {
	return edge(name, permitted, func(value string) (rune, int) {
		last, size := utf8.DecodeLastRuneInString(value)

		return last, len(value) - size
	})
}

func edge(name string, permitted func(rune) bool, pick func(string) (rune, int)) Rule {
	return Rule{name: name, apply: func(value string) (string, error) {
		if value == "" {
			return "", refuse(name, value, "nothing to look at", ErrEmpty)
		}

		if candidate, index := pick(value); !permitted(candidate) {
			return "", refuse(name, value, runeAt(candidate, index), ErrPattern)
		}

		return value, nil
	}}
}

// NoAdjacent refuses two of the given characters standing next to each
// other: "aws..vm" and "aws.-vm" are typos, and accepting them would give
// one name two spellings.
func NoAdjacent(runes ...rune) Rule {
	watched := slices.Clone(runes)

	return Rule{name: "no_adjacent", apply: func(value string) (string, error) {
		previous := false

		for offset, candidate := range value {
			current := slices.Contains(watched, candidate)
			if current && previous {
				return "", refuse("no_adjacent", value, runeAt(candidate, offset), ErrPattern)
			}

			previous = current
		}

		return value, nil
	}}
}
