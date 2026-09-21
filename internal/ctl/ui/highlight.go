package ui

import "regexp"

// A tool's output arrives as plain lines: pytest, a compiler, a shell
// script carry no severity, so their failures are recorded at the same
// level as everything else. Highlight marks the WORDS such lines use —
// it never changes the record's level, which stays what the emitter said.
var (
	badWords  = regexp.MustCompile(`\b(ERROR|FATAL|PANIC|FAILED|FAILURE|FAIL|Traceback|Exception|AssertionError|panic:|\d+ (?:failed|errors?))\b`)
	warnWords = regexp.MustCompile(`\b(WARN|WARNING|\w*Warning|\d+ warnings?|deprecated|DEPRECATED)\b`)
	goodWords = regexp.MustCompile(`\b(PASSED|PASS|OK|\d+ passed)\b`)
	// pytestErr is pytest's own marker of an assertion's explanation.
	pytestErr = regexp.MustCompile(`^E\s{2,}\S`)
)

// Highlight colors the telling words of an unstyled log body. A pytest
// "E   …" line is an error as a whole.
func Highlight(body string) string {
	if !enabled || body == "" {
		return body
	}
	if pytestErr.MatchString(body) {
		return Red(body)
	}
	body = badWords.ReplaceAllStringFunc(body, func(s string) string { return Bold(Red(s)) })
	body = warnWords.ReplaceAllStringFunc(body, Yellow)
	return goodWords.ReplaceAllStringFunc(body, Green)
}
