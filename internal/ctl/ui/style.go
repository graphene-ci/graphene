// Package ui is how graphenectl LOOKS: color, tables, trees, charts. It is
// plain text with ANSI styling — no screen takeover, every view pipes,
// greps and scrolls like any other command output. Color is a property of
// the DESTINATION: on a terminal it is on, in a pipe or a file it is off,
// so a script never has to strip escape codes.
package ui

import (
	"os"
	"regexp"
	"strings"
	"unicode/utf8"

	"golang.org/x/term"
)

// Color modes of the --color flag.
const (
	ColorAuto   = "auto"
	ColorAlways = "always"
	ColorNever  = "never"
)

var enabled = detect(ColorAuto)

// Configure applies the --color flag; auto follows the terminal and the
// NO_COLOR convention (https://no-color.org).
func Configure(mode string) { enabled = detect(mode) }

// Enabled reports whether styling is on.
func Enabled() bool { return enabled }

func detect(mode string) bool {
	switch mode {
	case ColorAlways:
		return true
	case ColorNever:
		return false
	}
	if os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
		return false
	}
	return term.IsTerminal(int(os.Stdout.Fd()))
}

// Width is the terminal's width, 0 when stdout is not one — "do not fit
// anything to a width".
func Width() int {
	w, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return 0
	}
	return w
}

// SGR codes used by the palette. The 8 basic colors only: they follow the
// user's terminal theme, where 256-color picks fight it.
const (
	sgrBold   = "1"
	sgrDim    = "2"
	sgrRed    = "31"
	sgrGreen  = "32"
	sgrYellow = "33"
	sgrBlue   = "34"
	sgrPurple = "35"
	sgrCyan   = "36"
	sgrGray   = "90"
)

func paint(code, s string) string {
	if !enabled || s == "" {
		return s
	}
	return "\x1b[" + code + "m" + s + "\x1b[0m"
}

// The palette, by MEANING where there is one.
func Bold(s string) string   { return paint(sgrBold, s) }
func Dim(s string) string    { return paint(sgrDim, s) }
func Gray(s string) string   { return paint(sgrGray, s) }
func Red(s string) string    { return paint(sgrRed, s) }
func Green(s string) string  { return paint(sgrGreen, s) }
func Yellow(s string) string { return paint(sgrYellow, s) }
func Blue(s string) string   { return paint(sgrBlue, s) }
func Purple(s string) string { return paint(sgrPurple, s) }
func Cyan(s string) string   { return paint(sgrCyan, s) }

// Phase colors a record phase or a run status by what it MEANS: fine,
// moving, wrong, over. Unknown words pass through.
func Phase(s string) string {
	switch strings.ToLower(s) {
	case "ready", "completed", "success", "connected":
		return Green(s)
	case "creating", "running", "pending", "updating", "continuedasnew":
		return Yellow(s)
	case "deleting", "canceled", "cancelled", "terminated":
		return Purple(s)
	case "failed", "error", "timedout", "failure":
		return Red(s)
	case "deleted":
		return Gray(s)
	}
	return s
}

// Ref renders "kind/id" with the kind quiet and the id plain: in a column
// of refs the eye wants the ids.
func Ref(ref string) string {
	kind, id, ok := strings.Cut(ref, "/")
	if !ok {
		return ref
	}
	return Gray(kind+"/") + id
}

var ansi = regexp.MustCompile("\x1b\\[[0-9;]*m")

// Strip removes styling.
func Strip(s string) string { return ansi.ReplaceAllString(s, "") }

// Len is the VISIBLE width of a cell: styling takes no columns. Runes
// count one column each — the glyphs used here (box drawing, blocks) and
// the texts of records (ASCII, Cyrillic) all are.
func Len(s string) int { return utf8.RuneCountInString(Strip(s)) }

// Pad right-pads a styled cell to a visible width.
func Pad(s string, width int) string {
	if n := Len(s); n < width {
		return s + strings.Repeat(" ", width-n)
	}
	return s
}

// PadLeft left-pads — numbers align on their last digit.
func PadLeft(s string, width int) string {
	if n := Len(s); n < width {
		return strings.Repeat(" ", width-n) + s
	}
	return s
}

// Cut shortens an UNSTYLED text to a visible width, marking the cut.
func Cut(s string, width int) string {
	if width <= 0 || utf8.RuneCountInString(s) <= width {
		return s
	}
	if width == 1 {
		return "…"
	}
	r := []rune(s)
	return string(r[:width-1]) + "…"
}
