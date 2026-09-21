package ui

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

// Block limits: a record's spec and state are whatever the kind's author
// made them — a docker container's is docker's whole Config. The default
// view shows the SHAPE and the first levels; the full document is what
// -o yaml is for.
const (
	blockDepth   = 3
	blockItems   = 6
	blockScalars = 96
)

// Block renders a raw JSON document under a title as an indented outline:
// quiet keys, plain values, deep or long parts folded. It reports whether
// anything was folded; an empty document ({} / [] / null) prints nothing.
func Block(w io.Writer, title string, raw []byte) (folded bool, err error) {
	if len(raw) == 0 {
		return false, nil
	}
	var v any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if decErr := dec.Decode(&v); decErr != nil {
		_, err = fmt.Fprintf(w, "%s %s\n", Gray(title+":"), string(raw))
		return false, err
	}
	if isEmpty(v) {
		return false, nil
	}
	b := &blockWriter{w: w}
	if _, err := fmt.Fprintln(w, Bold(Gray(title+":"))); err != nil {
		return false, err
	}
	b.value(v, "  ", 1)
	return b.folded, b.err
}

type blockWriter struct {
	w      io.Writer
	folded bool
	err    error
}

func (b *blockWriter) printf(format string, args ...any) {
	if b.err == nil {
		_, b.err = fmt.Fprintf(b.w, format, args...)
	}
}

func isEmpty(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case map[string]any:
		return len(t) == 0
	case []any:
		return len(t) == 0
	case string:
		return t == ""
	}
	return false
}

func (b *blockWriter) value(v any, indent string, depth int) {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			if !isEmpty(t[k]) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			b.entry(Gray(k+":"), t[k], indent, depth)
		}
	case []any:
		for i, item := range t {
			if i == blockItems {
				b.folded = true
				b.printf("%s%s\n", indent, Gray(fmt.Sprintf("… %d more", len(t)-i)))
				return
			}
			b.entry(Gray("-"), item, indent, depth)
		}
	default:
		b.printf("%s%s\n", indent, scalar(t))
	}
}

// entry prints one "key: value" (or "- value"): scalars on the same line,
// containers below — or folded to a one-line summary past the depth.
func (b *blockWriter) entry(lead string, v any, indent string, depth int) {
	switch t := v.(type) {
	case map[string]any:
		if depth >= blockDepth {
			b.folded = true
			b.printf("%s%s %s\n", indent, lead, Gray(fmt.Sprintf("{… %d fields}", len(t))))
			return
		}
		b.printf("%s%s\n", indent, lead)
		b.value(t, indent+"  ", depth+1)
	case []any:
		if depth >= blockDepth {
			b.folded = true
			b.printf("%s%s %s\n", indent, lead, Gray(fmt.Sprintf("[… %d items]", len(t))))
			return
		}
		b.printf("%s%s\n", indent, lead)
		b.value(t, indent+"  ", depth+1)
	default:
		text := scalar(t)
		if Len(text) > blockScalars {
			b.folded = true
			text = Cut(Strip(text), blockScalars)
		}
		b.printf("%s%s %s\n", indent, lead, text)
	}
}

func scalar(v any) string {
	switch t := v.(type) {
	case bool:
		if t {
			return Green("true")
		}
		return Gray("false")
	case json.Number:
		return Cyan(t.String())
	case string:
		return t
	}
	return fmt.Sprint(v)
}
