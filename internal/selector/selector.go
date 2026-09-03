// Package selector is the ONE query language over everything listable:
// entity resources and runs alike. A selector is a comma-separated
// conjunction of terms — `kind=run, phase in (Running, Failed),
// pipeline=deploy, label.env=prod, started>-2h` — compiled into a
// Temporal visibility query. The grammar is a stable API contract; the
// compiler targets what the CURRENT store can answer and refuses the
// rest loudly, so the language never lies about its backend.
package selector

import (
	"fmt"
	"strings"
	"time"
)

// Reserved field names. Any `label.<key>` field matches the record's
// labels; everything else is refused.
const (
	fieldKind     = "kind"
	fieldId       = "id"
	fieldPhase    = "phase"
	fieldOwner    = "owner"
	fieldPipeline = "pipeline"
	fieldStarted  = "started"
	fieldFinished = "finished"
)

// KindRun is the system kind translated onto pipeline-run workflows
// instead of entity records.
const KindRun = "run"

// Op is a term's comparison operator.
type Op string

// Operators of the grammar.
const (
	OpEq     Op = "="
	OpNeq    Op = "!="
	OpPrefix Op = "=^"
	OpGt     Op = ">"
	OpLt     Op = "<"
	OpIn     Op = "in"
)

// Term is one parsed condition.
type Term struct {
	// Field is the reserved field name, or "label" for label terms.
	Field string
	// LabelKey is set for `label.<key>` terms.
	LabelKey string
	Op       Op
	// Values holds one value, or several for `in`.
	Values []string
}

// Query is a parsed selector.
type Query struct {
	Terms []Term
}

// Parse parses the selector text. It validates the grammar only —
// which fields combine with which kinds is Compile's business.
func Parse(input string) (Query, error) {
	var q Query
	rest := strings.TrimSpace(input)
	if rest == "" {
		return q, fmt.Errorf("empty selector")
	}
	for _, raw := range splitTop(rest) {
		term, err := parseTerm(strings.TrimSpace(raw))
		if err != nil {
			return Query{}, err
		}
		q.Terms = append(q.Terms, term)
	}
	return q, nil
}

// splitTop splits on commas outside parentheses and quotes.
func splitTop(s string) []string {
	var parts []string
	depth, start, inStr := 0, 0, false
	for i, r := range s {
		switch {
		case r == '"':
			inStr = !inStr
		case inStr:
		case r == '(':
			depth++
		case r == ')':
			depth--
		case r == ',' && depth == 0:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func parseTerm(s string) (Term, error) {
	if s == "" {
		return Term{}, fmt.Errorf("empty term")
	}
	// `field in (a, b)` — the word form first.
	if f, rest, ok := cutWord(s, " in "); ok {
		rest = strings.TrimSpace(rest)
		if !strings.HasPrefix(rest, "(") || !strings.HasSuffix(rest, ")") {
			return Term{}, fmt.Errorf("%q: `in` needs a parenthesized list", s)
		}
		var values []string
		for _, v := range strings.Split(rest[1:len(rest)-1], ",") {
			value, err := parseValue(strings.TrimSpace(v))
			if err != nil {
				return Term{}, err
			}
			values = append(values, value)
		}
		if len(values) == 0 {
			return Term{}, fmt.Errorf("%q: empty `in` list", s)
		}
		return newTerm(f, OpIn, values)
	}
	for _, op := range []Op{OpNeq, OpPrefix, OpEq, OpGt, OpLt} {
		if f, v, ok := strings.Cut(s, string(op)); ok {
			value, err := parseValue(strings.TrimSpace(v))
			if err != nil {
				return Term{}, err
			}
			return newTerm(strings.TrimSpace(f), op, []string{value})
		}
	}
	return Term{}, fmt.Errorf("%q: no operator (=, !=, =^, >, <, in)", s)
}

// cutWord cuts around a word operator outside quotes.
func cutWord(s, word string) (string, string, bool) {
	inStr := false
	for i := range len(s) {
		if s[i] == '"' {
			inStr = !inStr
		}
		if !inStr && i+len(word) <= len(s) && s[i:i+len(word)] == word {
			return strings.TrimSpace(s[:i]), s[i+len(word):], true
		}
	}
	return "", "", false
}

func parseValue(v string) (string, error) {
	if strings.HasPrefix(v, `"`) && strings.HasSuffix(v, `"`) && len(v) >= 2 {
		v = v[1 : len(v)-1]
	}
	if v == "" {
		return "", fmt.Errorf("empty value")
	}
	if strings.ContainsAny(v, `'"`) {
		return "", fmt.Errorf("%q: quotes are not allowed in a value", v)
	}
	return v, nil
}

func newTerm(field string, op Op, values []string) (Term, error) {
	if key, ok := strings.CutPrefix(field, "label."); ok {
		if key == "" {
			return Term{}, fmt.Errorf("label term needs a key: label.<key>")
		}
		return Term{Field: "label", LabelKey: key, Op: op, Values: values}, nil
	}
	switch field {
	case fieldKind, fieldId, fieldPhase, fieldOwner, fieldPipeline, fieldStarted, fieldFinished:
		return Term{Field: field, Op: op, Values: values}, nil
	}
	return Term{}, fmt.Errorf("unknown field %q (want kind, id, phase, owner, pipeline, started, finished, label.<key>)", field)
}

// Compile renders the visibility query. now anchors relative times.
func Compile(q Query, now time.Time) (string, error) {
	kinds, err := kindsOf(q)
	if err != nil {
		return "", err
	}
	runMode := len(kinds) == 1 && kinds[0] == KindRun
	var terms []string
	if runMode {
		terms = append(terms, `WorkflowId STARTS_WITH "run/"`)
	} else {
		switch {
		case len(kinds) == 0:
			terms = append(terms, `EntityKind IS NOT NULL`)
		case len(kinds) == 1:
			terms = append(terms, fmt.Sprintf("EntityKind = '%s'", kinds[0]))
		default:
			terms = append(terms, fmt.Sprintf("EntityKind IN (%s)", quoteList(kinds)))
		}
		// Entity listings see LIVE records, same as the structural path.
		terms = append(terms, `ExecutionStatus = 'Running'`)
	}
	for _, t := range q.Terms {
		if t.Field == fieldKind {
			continue
		}
		rendered, err := compileTerm(t, runMode, now)
		if err != nil {
			return "", err
		}
		terms = append(terms, rendered)
	}
	return strings.Join(terms, " AND "), nil
}

func kindsOf(q Query) ([]string, error) {
	var kinds []string
	for _, t := range q.Terms {
		if t.Field != fieldKind {
			continue
		}
		if kinds != nil {
			return nil, fmt.Errorf("kind may appear once")
		}
		if t.Op != OpEq && t.Op != OpIn {
			return nil, fmt.Errorf("kind supports = and in only")
		}
		kinds = t.Values
	}
	for _, k := range kinds {
		if k == KindRun && len(kinds) > 1 {
			return nil, fmt.Errorf("kind=run cannot be mixed with entity kinds")
		}
	}
	return kinds, nil
}

func compileTerm(t Term, runMode bool, now time.Time) (string, error) {
	switch t.Field {
	case "label":
		if t.Op != OpEq && t.Op != OpIn {
			return "", fmt.Errorf("label.%s: labels support = and in only", t.LabelKey)
		}
		pairs := make([]string, len(t.Values))
		for i, v := range t.Values {
			pairs[i] = t.LabelKey + "=" + v
		}
		return fmt.Sprintf("EntityLabels IN (%s)", quoteList(pairs)), nil
	case fieldId:
		field := "WorkflowId"
		prefix := ""
		if runMode {
			prefix = "run/"
		}
		switch t.Op {
		case OpEq:
			return fmt.Sprintf("%s = '%s%s'", field, prefix, t.Values[0]), nil
		case OpPrefix:
			return fmt.Sprintf(`%s STARTS_WITH "%s%s"`, field, prefix, t.Values[0]), nil
		case OpIn:
			ids := make([]string, len(t.Values))
			for i, v := range t.Values {
				ids[i] = prefix + v
			}
			return fmt.Sprintf("%s IN (%s)", field, quoteList(ids)), nil
		case OpNeq, OpGt, OpLt:
			return "", fmt.Errorf("id supports =, =^, in")
		}
	case fieldPhase:
		field := "EntityPhase"
		if runMode {
			field = "ExecutionStatus"
		}
		return equality(field, t)
	case fieldOwner:
		if runMode {
			return "", fmt.Errorf("owner does not apply to kind=run")
		}
		return equality("EntityOwner", t)
	case fieldPipeline:
		if !runMode {
			return "", fmt.Errorf("pipeline applies to kind=run only")
		}
		switch t.Op {
		case OpEq, OpNeq, OpIn:
			return equality("WorkflowType", t)
		case OpPrefix:
			return fmt.Sprintf(`WorkflowType STARTS_WITH "%s"`, t.Values[0]), nil
		case OpGt, OpLt:
			return "", fmt.Errorf("pipeline supports =, !=, =^, in")
		}
	case fieldStarted, fieldFinished:
		field := "StartTime"
		if t.Field == fieldFinished {
			field = "CloseTime"
		}
		if t.Op != OpGt && t.Op != OpLt {
			return "", fmt.Errorf("%s supports > and < only", t.Field)
		}
		at, err := parseTime(t.Values[0], now)
		if err != nil {
			return "", fmt.Errorf("%s: %w", t.Field, err)
		}
		return fmt.Sprintf("%s %s '%s'", field, t.Op, at.Format(time.RFC3339)), nil
	}
	return "", fmt.Errorf("unknown field %q", t.Field)
}

func equality(field string, t Term) (string, error) {
	switch t.Op {
	case OpEq:
		return fmt.Sprintf("%s = '%s'", field, t.Values[0]), nil
	case OpNeq:
		return fmt.Sprintf("%s != '%s'", field, t.Values[0]), nil
	case OpIn:
		return fmt.Sprintf("%s IN (%s)", field, quoteList(t.Values)), nil
	case OpPrefix, OpGt, OpLt:
		return "", fmt.Errorf("%s supports =, !=, in", strings.ToLower(field))
	}
	return "", fmt.Errorf("%s has an unknown operator", strings.ToLower(field))
}

func quoteList(values []string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = "'" + v + "'"
	}
	return strings.Join(quoted, ", ")
}

// parseTime accepts RFC3339 or a relative offset like -2h / -30m / -7d.
func parseTime(v string, now time.Time) (time.Time, error) {
	if strings.HasPrefix(v, "-") {
		if d, ok := parseRelative(v[1:]); ok {
			return now.Add(-d), nil
		}
		return time.Time{}, fmt.Errorf("%q: want -<N>(m|h|d) or RFC3339", v)
	}
	at, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("%q: want -<N>(m|h|d) or RFC3339", v)
	}
	return at, nil
}

func parseRelative(v string) (time.Duration, bool) {
	if v == "" {
		return 0, false
	}
	unit := v[len(v)-1]
	var n int
	for _, r := range v[:len(v)-1] {
		if r < '0' || r > '9' {
			return 0, false
		}
		n = n*10 + int(r-'0')
	}
	if len(v) == 1 {
		return 0, false
	}
	switch unit {
	case 'm':
		return time.Duration(n) * time.Minute, true
	case 'h':
		return time.Duration(n) * time.Hour, true
	case 'd':
		return time.Duration(n) * 24 * time.Hour, true
	}
	return 0, false
}

// IsRunQuery reports whether the parsed selector targets kind=run.
func IsRunQuery(q Query) bool {
	kinds, err := kindsOf(q)
	return err == nil && len(kinds) == 1 && kinds[0] == KindRun
}

// PinnedKind reports the single kind this selector restricts to, if
// it provably does. Only an equality on `kind` pins it: `in` names
// several, `!=` and `=^` name a set, and no term at all names
// everything. Authorization uses this — a listing narrowed to one kind
// is checked against THAT kind, so the answer must never be a guess.
func (q Query) PinnedKind() (string, bool) {
	pinned, found := "", 0
	for _, t := range q.Terms {
		if t.Field != "kind" {
			continue
		}
		found++
		if t.Op != OpEq || len(t.Values) != 1 {
			return "", false
		}
		pinned = t.Values[0]
	}
	if found != 1 || pinned == "" {
		return "", false
	}
	return pinned, true
}
