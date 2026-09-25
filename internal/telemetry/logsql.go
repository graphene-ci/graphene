package telemetry

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// LogsQL reads logs from VictoriaLogs.
type LogsQL struct {
	// Base is the VictoriaLogs base URL (http://victorialogs:9428).
	Base   string
	Client *http.Client
}

// scopeFilter is the part of a LogsQL query that names the record: the
// namespace and the correlation axes. Everything the caller adds is ANDed
// after it, inside its own parentheses.
func scopeFilter(sel Selector) string {
	match := fmt.Sprintf("%q:=%q", sel.Attribute, sel.Value)
	if sel.AltAttribute != "" {
		match = fmt.Sprintf("(%s OR %q:=%q)", match, sel.AltAttribute, sel.AltValue)
	}
	return fmt.Sprintf("%q:=%q AND %s", "graphene.namespace", sel.Namespace, match)
}

// fenceable checks that a caller's filter can be laid inside parentheses
// without ever closing them: parentheses balanced outside string literals
// and never below zero, every literal terminated, no pipe at the top
// level (a pipe cannot live inside a filter). Without this check
// `*) OR (x` would close the fence early and OR itself with the scope.
func fenceable(filter string) error {
	depth := 0
	var quote byte
	for i := 0; i < len(filter); i++ {
		c := filter[i]
		switch {
		case quote != 0:
			switch {
			case c == '\\' && quote != '`':
				i++ // the escaped character is part of the literal
			case c == quote:
				quote = 0
			}
		case c == '"' || c == '\'' || c == '`':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			if depth--; depth < 0 {
				return &ClientError{Msg: "query: a closing parenthesis without an opening one"}
			}
		case c == '|':
			return &ClientError{Msg: "query: a pipe cannot be used inside a record's selection; use the selection's own fields"}
		}
	}
	if quote != 0 {
		return &ClientError{Msg: "query: an unterminated string literal"}
	}
	if depth != 0 {
		return &ClientError{Msg: "query: unbalanced parentheses"}
	}
	return nil
}

// scopeExtra is the namespace as VictoriaLogs' extra_filters argument:
// the backend ANDs it onto the query itself, whatever the query says —
// a second wall around the tenant, independent of the fence.
func scopeExtra(sel Selector) string {
	raw, _ := json.Marshal(map[string]string{"graphene.namespace": sel.Namespace})
	return string(raw)
}

// selection renders the filter part of a query — scope, bounds, the
// caller's filters — without the pipes that order and page it.
func selection(sel Selector, q LogQuery) (string, error) {
	if err := fenceable(q.Filter); err != nil {
		return "", err
	}
	parts := []string{scopeFilter(sel)}
	since := sel.After(q.Since)
	switch {
	case !q.Cursor.IsZero() && q.Desc:
		parts = append(parts, "_time:<="+q.Cursor.Time.UTC().Format(time.RFC3339Nano))
	case !q.Cursor.IsZero():
		parts = append(parts, "_time:>="+q.Cursor.Time.UTC().Format(time.RFC3339Nano))
	}
	if !since.IsZero() {
		parts = append(parts, "_time:>"+since.UTC().Format(time.RFC3339Nano))
	}
	if !q.Until.IsZero() {
		parts = append(parts, "_time:<"+q.Until.UTC().Format(time.RFC3339Nano))
	}
	if f := strings.TrimSpace(q.Filter); f != "" {
		// The caller's language, fenced: an OR inside cannot reach the
		// scope outside.
		parts = append(parts, "("+f+")")
	}
	if len(q.Severities) > 0 {
		quoted := make([]string, len(q.Severities))
		for i, sev := range q.Severities {
			quoted[i] = strconv.Quote(strings.ToUpper(sev))
		}
		list := strings.Join(quoted, ",")
		parts = append(parts, fmt.Sprintf("(severity:in(%s) OR severity_text:in(%s))", list, list))
	}
	for _, attr := range sortedKeys(q.Attributes) {
		parts = append(parts, fmt.Sprintf("%q:=%q", attr, q.Attributes[attr]))
	}
	if q.Text != "" {
		parts = append(parts, "_msg:"+strconv.Quote(q.Text))
	}
	return strings.Join(parts, " AND "), nil
}

// Query returns one page of the selection, ordered by (time, stream,
// body) so a cursor names a position exactly.
func (l *LogsQL) Query(ctx context.Context, sel Selector, q LogQuery) (LogPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = DefaultLogLimit
	}
	if limit > MaxLogLimit {
		limit = MaxLogLimit
	}
	order := ""
	if q.Desc {
		order = " desc"
	}
	// One more than the page says: that record is the answer to "is there
	// more?", never delivered.
	where, err := selection(sel, q)
	if err != nil {
		return LogPage{}, err
	}
	query := fmt.Sprintf("%s | sort by (_time, _stream_id, _msg)%s | offset %d | limit %d",
		where, order, q.Cursor.Skip, limit+1)
	raw, err := l.post(ctx, "/select/logsql/query", url.Values{"query": {query}, "extra_filters": {scopeExtra(sel)}})
	if err != nil {
		return LogPage{}, err
	}
	records, err := parseLogsQLStream(raw)
	if err != nil {
		return LogPage{}, err
	}
	page := LogPage{Records: records}
	if len(records) > limit {
		page.Records, page.Truncated = records[:limit], true
	}
	if n := len(page.Records); n > 0 {
		last := page.Records[n-1].Time
		next := LogCursor{Time: last}
		for i := n - 1; i >= 0 && page.Records[i].Time.Equal(last); i-- {
			next.Skip++
		}
		if last.Equal(q.Cursor.Time) {
			// The page never left the cursor's instant: the skip accumulates.
			next.Skip += q.Cursor.Skip
		}
		page.Next = next
	}
	return page, nil
}

// Facets counts the values of each field within the selection.
func (l *LogsQL) Facets(ctx context.Context, sel Selector, q LogQuery, fields []string, limit int) ([]Facet, error) {
	if limit <= 0 {
		limit = DefaultFacetLimit
	}
	query, err := selection(sel, q)
	if err != nil {
		return nil, err
	}
	out := make([]Facet, 0, len(fields))
	for _, field := range fields {
		raw, err := l.post(ctx, "/select/logsql/field_values", url.Values{
			"query": {query}, "field": {field}, "limit": {strconv.Itoa(limit)}, "extra_filters": {scopeExtra(sel)},
		})
		if err != nil {
			return nil, err
		}
		var reply struct {
			Values []struct {
				Value string `json:"value"`
				Hits  any    `json:"hits"`
			} `json:"values"`
		}
		if err := json.Unmarshal(raw, &reply); err != nil {
			return nil, fmt.Errorf("logs backend: field_values: %w", err)
		}
		facet := Facet{Field: field}
		for _, v := range reply.Values {
			hits, _ := strconv.ParseInt(fmt.Sprint(v.Hits), 10, 64)
			facet.Values = append(facet.Values, FacetValue{Value: v.Value, Hits: hits})
		}
		out = append(out, facet)
	}
	return out, nil
}

// RawLogs runs one LogsQL query as given — the raw view, the whole store.
func (l *LogsQL) RawLogs(ctx context.Context, query string, limit int) ([]LogRecord, error) {
	if limit <= 0 {
		limit = DefaultLogLimit
	}
	raw, err := l.post(ctx, "/select/logsql/query", url.Values{"query": {query}, "limit": {strconv.Itoa(limit)}})
	if err != nil {
		return nil, err
	}
	return parseLogsQLStream(raw)
}

const maxLogsBytes = 64 << 20

func (l *LogsQL) post(ctx context.Context, path string, form url.Values) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimSuffix(l.Base, "/")+path, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := l.Client.Do(req)
	if err != nil {
		return nil, err
	}
	return readBackend("logs", resp, maxLogsBytes)
}

// parseLogsQLStream reads the backend's JSONL response in the order the
// backend chose — the sort pipe is the query's.
func parseLogsQLStream(body []byte) ([]LogRecord, error) {
	var out []LogRecord
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for scanner.Scan() {
		var fields map[string]string
		if json.Unmarshal(scanner.Bytes(), &fields) != nil {
			continue
		}
		rec := LogRecord{Attributes: map[string]string{}}
		for k, v := range fields {
			switch k {
			case "_time":
				rec.Time, _ = time.Parse(time.RFC3339Nano, v)
			case "_msg":
				rec.Body = v
			case "severity", "severity_text":
				rec.Severity = v
			case "_stream", "_stream_id":
				// stream identity is derivable from the attributes
			default:
				rec.Attributes[k] = v
			}
		}
		if rec.Severity == "" {
			rec.Severity = severityFromNumber(fields["severity_number"])
		}
		out = append(out, rec)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// severityFromNumber maps OTel severity numbers to names.
func severityFromNumber(n string) string {
	v, err := strconv.Atoi(n)
	if err != nil {
		return ""
	}
	switch {
	case v == 0:
		return ""
	case v <= 4:
		return "TRACE"
	case v <= 8:
		return "DEBUG"
	case v <= 12:
		return "INFO"
	case v <= 16:
		return "WARN"
	case v <= 20:
		return "ERROR"
	}
	return "FATAL"
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
