package ui

import (
	"fmt"
	"io"
	"strings"
)

// Table lays styled cells out in aligned columns. text/tabwriter cannot:
// it counts escape codes as width.
type Table struct {
	header []string
	rows   [][]string
	// right marks right-aligned columns (numbers).
	right map[int]bool
	// flex is the one column that gives way when the table is wider than
	// the terminal; -1 — none.
	flex int
	// breaks holds the row indexes a blank separator line goes BEFORE.
	breaks map[int]bool
}

// NewTable starts a table; header cells are plain words.
func NewTable(header ...string) *Table {
	return &Table{header: header, right: map[int]bool{}, flex: -1, breaks: map[int]bool{}}
}

// Right marks columns as right-aligned.
func (t *Table) Right(cols ...int) *Table {
	for _, c := range cols {
		t.right[c] = true
	}
	return t
}

// Flex names the column that is cut when the terminal is too narrow.
func (t *Table) Flex(col int) *Table { t.flex = col; return t }

// Row appends a row of (possibly styled) cells.
func (t *Table) Row(cells ...string) { t.rows = append(t.rows, cells) }

// Break puts a blank line before the next row — a group boundary.
func (t *Table) Break() {
	if len(t.rows) > 0 {
		t.breaks[len(t.rows)] = true
	}
}

// Len is the number of rows.
func (t *Table) Len() int { return len(t.rows) }

// Render writes the table. termWidth 0 means "do not fit".
func (t *Table) Render(w io.Writer, termWidth int) error {
	widths := make([]int, len(t.header))
	for i, h := range t.header {
		widths[i] = Len(h)
	}
	for _, row := range t.rows {
		for i, c := range row {
			if i < len(widths) && Len(c) > widths[i] {
				widths[i] = Len(c)
			}
		}
	}
	const gap = 2
	if t.flex >= 0 && t.flex < len(widths) && termWidth > 0 {
		total := gap * (len(widths) - 1)
		for _, n := range widths {
			total += n
		}
		if over := total - termWidth; over > 0 {
			widths[t.flex] = max(widths[t.flex]-over, Len(t.header[t.flex]), 8)
		}
	}
	line := func(cells []string, style func(string) string) error {
		parts := make([]string, len(widths))
		for i := range widths {
			cell := ""
			if i < len(cells) {
				cell = cells[i]
			}
			if i == t.flex && Len(cell) > widths[i] {
				// Cutting drops the styling with the tail: a cut cell is
				// re-rendered plain rather than left with an open escape.
				cell = Cut(Strip(cell), widths[i])
			}
			if style != nil {
				cell = style(cell)
			}
			if t.right[i] {
				parts[i] = PadLeft(cell, widths[i])
			} else {
				parts[i] = Pad(cell, widths[i])
			}
		}
		_, err := fmt.Fprintln(w, strings.TrimRight(strings.Join(parts, strings.Repeat(" ", gap)), " "))
		return err
	}
	if err := line(t.header, func(s string) string { return Bold(Gray(s)) }); err != nil {
		return err
	}
	for i, row := range t.rows {
		if t.breaks[i] {
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
		}
		if err := line(row, nil); err != nil {
			return err
		}
	}
	return nil
}

// Fields renders "key: value" pairs with the keys aligned and quiet; empty
// values are left out — a blank field is noise, not information.
func Fields(w io.Writer, pairs ...[2]string) error {
	width := 0
	for _, p := range pairs {
		if p[1] != "" && len(p[0]) > width {
			width = len(p[0])
		}
	}
	for _, p := range pairs {
		if p[1] == "" {
			continue
		}
		if _, err := fmt.Fprintf(w, "%s  %s\n", Gray(Pad(p[0]+":", width+1)), p[1]); err != nil {
			return err
		}
	}
	return nil
}
