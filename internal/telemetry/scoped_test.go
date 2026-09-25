package telemetry

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A record's scope is laid over the caller's own expression in a way the
// expression cannot escape: the filter is fenced, the bounds and the
// scope stay outside the fence.
func TestLogsScopedQueryFencesTheCallersFilter(t *testing.T) {
	var query string
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		query = r.Form.Get("query")
	}))
	defer server.Close()
	l := &LogsQL{Base: server.URL, Client: server.Client()}
	q := LogQuery{
		Filter:     `level:error OR "graphene.namespace":="other"`, // an attempt to widen the scope
		Severities: []string{"warn", "ERROR"},
		Attributes: map[string]string{"stream": "stderr", "graphene.agent": "db-1"},
		Text:       "connection refused",
		Until:      born.Add(time.Hour),
		Limit:      50,
		Desc:       true,
	}
	if _, err := l.Query(context.Background(), sel, q); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"graphene.namespace":="default" AND ("graphene.agent":="db-1" OR "graphene.entity":="agent/db-1")`,
		` AND (level:error OR "graphene.namespace":="other")`,
		`(severity:in("WARN","ERROR") OR severity_text:in("WARN","ERROR"))`,
		`"graphene.agent":="db-1" AND "stream":="stderr"`,
		`_msg:"connection refused"`,
		`_time:>2026-09-18T11:00:00Z`,
		`_time:<2026-09-18T12:00:00Z`,
		`| sort by (_time, _stream_id, _msg) desc | offset 0 | limit 51`,
	} {
		if !strings.Contains(query, want) {
			t.Errorf("query lacks %q:\n%s", want, query)
		}
	}
	if !strings.HasPrefix(query, `"graphene.namespace":="default" AND`) {
		t.Errorf("the scope must come first: %s", query)
	}
}

// Records sharing one timestamp are not lost across pages: the cursor is
// the time of the last record and how many with that time went already.
func TestLogsCursorKeepsEqualTimestamps(t *testing.T) {
	at := born.Add(time.Minute).UTC().Format(time.RFC3339Nano)
	line := func(msg string) string { return `{"_time":"` + at + `","_msg":"` + msg + `"}` + "\n" }
	var query string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		query = r.Form.Get("query")
		// Three records at the same instant, the page asks for two.
		_, _ = w.Write([]byte(line("a") + line("b") + line("c")))
	}))
	defer server.Close()
	l := &LogsQL{Base: server.URL, Client: server.Client()}
	page, err := l.Query(context.Background(), sel, LogQuery{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 2 || !page.Truncated {
		t.Fatalf("page: %d records, truncated %v", len(page.Records), page.Truncated)
	}
	if page.Next.Skip != 2 || !page.Next.Time.Equal(page.Records[1].Time) {
		t.Fatalf("cursor: %+v", page.Next)
	}
	// The next page starts AT the cursor's instant and skips what went.
	if _, err := l.Query(context.Background(), sel, LogQuery{Limit: 2, Cursor: page.Next}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"_time:>=" + at, "| offset 2 | limit 3"} {
		if !strings.Contains(query, want) {
			t.Errorf("next page lacks %q:\n%s", want, query)
		}
	}
	// Still inside the same instant: the skip accumulates.
	page2, err := l.Query(context.Background(), sel, LogQuery{Limit: 2, Cursor: page.Next})
	if err != nil {
		t.Fatal(err)
	}
	if page2.Next.Skip != 4 {
		t.Fatalf("accumulated skip = %d, want 4", page2.Next.Skip)
	}
}

func TestLogsFacetsCountValuesInsideTheScope(t *testing.T) {
	var fields, queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.URL.Path != "/select/logsql/field_values" {
			t.Errorf("path %s", r.URL.Path)
		}
		fields = append(fields, r.Form.Get("field"))
		queries = append(queries, r.Form.Get("query"))
		_, _ = w.Write([]byte(`{"values":[{"value":"INFO","hits":"61"},{"value":"WARN","hits":2}]}`))
	}))
	defer server.Close()
	l := &LogsQL{Base: server.URL, Client: server.Client()}
	facets, err := l.Facets(context.Background(), sel, LogQuery{Text: "job"}, []string{"severity", "stream"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(facets) != 2 || facets[0].Field != "severity" || facets[0].Values[0].Hits != 61 || facets[0].Values[1].Hits != 2 {
		t.Fatalf("facets: %+v", facets)
	}
	if strings.Join(fields, ",") != "severity,stream" {
		t.Fatalf("fields asked: %v", fields)
	}
	for _, q := range queries {
		if !strings.HasPrefix(q, `"graphene.namespace":="default"`) || strings.Contains(q, "| sort") || !strings.Contains(q, `_msg:"job"`) {
			t.Errorf("facet query must be the fenced selection without pipes: %s", q)
		}
	}
}

// The scoped metric form leans on the backend: extra_filters, one per axis
// per label spelling, the namespace inside each; the expression goes as is.
func TestMetricsScopedQueryUsesExtraFilters(t *testing.T) {
	var got url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.Query()
		_, _ = w.Write([]byte(`{"status":"success","data":{"result":[]}}`))
	}))
	defer server.Close()
	p := &PromQL{Base: server.URL, Client: server.Client(), ExtraFilters: true}
	q := MetricsQuery{Start: born, End: born.Add(time.Hour), Step: 30 * time.Second, Expr: `rate(stroppy_ops_total[1m])`}
	if _, err := p.Series(context.Background(), sel, q); err != nil {
		t.Fatal(err)
	}
	if got.Get("query") != `rate(stroppy_ops_total[1m])` || got.Get("step") != "30s" {
		t.Fatalf("query/step: %v", got)
	}
	filters := got["extra_filters[]"]
	if len(filters) != 4 {
		t.Fatalf("extra_filters: %v", filters)
	}
	for _, f := range filters {
		if !strings.Contains(f, `namespace"="default"`) && !strings.Contains(f, `namespace="default"`) {
			t.Errorf("a filter without the namespace: %s", f)
		}
	}
	// A backend without extra_filters cannot scope an expression.
	plain := &PromQL{Base: server.URL, Client: server.Client()}
	if _, err := plain.Series(context.Background(), sel, q); !errors.Is(err, ErrScopedQueryUnsupported) {
		t.Fatalf("plain backend: %v", err)
	}
}

func TestMetricsStepIsBounded(t *testing.T) {
	hour := MetricsQuery{Start: born, End: born.Add(time.Hour)}
	if got := hour.StepOrDefault(); got != 18*time.Second {
		t.Fatalf("default step over an hour = %s, want 18s (3600/200)", got)
	}
	short := MetricsQuery{Start: born, End: born.Add(time.Minute)}
	if got := short.StepOrDefault(); got != 15*time.Second {
		t.Fatalf("default step over a minute = %s, want the 15s floor", got)
	}
	var client *ClientError
	fine := MetricsQuery{Start: born, End: born.Add(time.Hour), Step: time.Second}
	if err := fine.Validate(); err != nil {
		t.Fatalf("3600 points must pass: %v", err)
	}
	tooFine := MetricsQuery{Start: born, End: born.Add(24 * time.Hour), Step: time.Second}
	if err := tooFine.Validate(); !errors.As(err, &client) {
		t.Fatalf("86400 points must be refused as the caller's fault: %v", err)
	}
	if err := (MetricsQuery{Start: born, End: born.Add(time.Hour), Step: time.Millisecond}).Validate(); !errors.As(err, &client) {
		t.Fatalf("a sub-second step must be refused: %v", err)
	}
}

// The door tells the query's fault from the backend's by the status.
func TestBackendErrorsAreClassified(t *testing.T) {
	for _, tc := range []struct {
		status int
		client bool
	}{{400, true}, {422, true}, {500, false}, {503, false}} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte("nope"))
		}))
		l := &LogsQL{Base: server.URL, Client: server.Client()}
		_, err := l.Query(context.Background(), sel, LogQuery{})
		server.Close()
		var be *BackendError
		if !errors.As(err, &be) || be.ClientFault() != tc.client || be.Status != tc.status {
			t.Fatalf("status %d: %v", tc.status, err)
		}
	}
}

// The scope's tags win over the caller's; a service the caller names is
// the only one asked; the birth floors the caller's start.
func TestTraceScopedSearchKeepsTheScopeTags(t *testing.T) {
	var asked []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/api/services") {
			_, _ = w.Write([]byte(`{"data":["graphene-pipeline","other"]}`))
			return
		}
		asked = append(asked, r.URL.RawQuery)
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer server.Close()
	j := &Jaeger{Base: server.URL, Client: server.Client()}
	params := `service=stroppy&operation=query&tags=` + `{"graphene.agent":"other-1","db":"pg"}` + `&start=1`
	if _, err := j.Search(context.Background(), sel, params, 5); err != nil {
		t.Fatal(err)
	}
	if len(asked) != 2 {
		t.Fatalf("one service, two axes — %d queries", len(asked))
	}
	for _, q := range asked {
		if !strings.Contains(q, "service=stroppy") || !strings.Contains(q, "operation=query") {
			t.Errorf("caller's service/operation lost: %s", q)
		}
		if strings.Contains(q, "other-1") {
			t.Errorf("caller overrode the scope tag: %s", q)
		}
		if !strings.Contains(q, "%22db%22%3A%22pg%22") {
			t.Errorf("caller's own tag lost: %s", q)
		}
		if !strings.Contains(q, "start="+strconv.FormatInt(born.UnixMicro(), 10)) {
			t.Errorf("start must be floored to the birth: %s", q)
		}
	}
	if _, err := j.Search(context.Background(), sel, "tags=notjson", 5); err == nil {
		t.Fatal("malformed tags must be refused")
	}
}

// The fence holds only if the caller's filter cannot close it: parentheses
// must balance outside string literals and never dip below zero, literals
// must end, and a pipe has no place inside a filter. What would escape is
// refused as the caller's fault before any backend sees it.
func TestLogsFilterCannotCloseTheFence(t *testing.T) {
	var form url.Values
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		form = r.Form
	}))
	defer server.Close()
	l := &LogsQL{Base: server.URL, Client: server.Client()}
	for _, filter := range []string{
		`*) OR ("graphene.namespace":="beta") OR ("graphene.run":="r2"`,
		`*) OR ("graphene.run":="r2") AND (`,
		`level:error)`,
		`(level:error`,
		`"unterminated`,
		`'unterminated`,
		"`unterminated",
		`* | delete "graphene.namespace"`,
		`"ok")`,
	} {
		form = nil
		_, err := l.Query(context.Background(), sel, LogQuery{Filter: filter})
		var ce *ClientError
		if !errors.As(err, &ce) {
			t.Errorf("filter %q: err=%v, want a client error", filter, err)
		}
		if form != nil {
			t.Errorf("filter %q reached the backend", filter)
		}
		if _, err := l.Facets(context.Background(), sel, LogQuery{Filter: filter}, []string{"stream"}, 5); !errors.As(err, &ce) {
			t.Errorf("facets with %q: err=%v, want a client error", filter, err)
		}
	}
	// Parentheses and pipes INSIDE a literal are text, not structure.
	for _, filter := range []string{
		`_msg:"a ) b | c"`, `_msg:'quote " inside' OR (level:error AND _msg:~"x\)y")`, "_msg:`raw ) text`", `(a OR (b AND (c)))`, ``,
	} {
		if _, err := l.Query(context.Background(), sel, LogQuery{Filter: filter}); err != nil {
			t.Errorf("filter %q refused: %v", filter, err)
		}
	}
	// Whatever the query says, the backend gets the tenant as its own
	// extra filter — the wall behind the fence.
	if got := form.Get("extra_filters"); got != `{"graphene.namespace":"default"}` {
		t.Errorf("extra_filters = %q", got)
	}
}

// A live record is admitted by the same selection the history obeyed.
func TestLogQueryAdmitsLiveRecords(t *testing.T) {
	at := time.Now()
	rec := LogRecord{Time: at, Severity: "ERROR", Body: "connection refused by 10.0.0.1", Attributes: map[string]string{"stream": "stderr", "graphene.agent": "db-1"}}
	cases := map[string]struct {
		q    LogQuery
		want bool
	}{
		"empty selection":       {LogQuery{}, true},
		"severity any case":     {LogQuery{Severities: []string{"warn", "error"}}, true},
		"severity other":        {LogQuery{Severities: []string{"INFO"}}, false},
		"attribute equal":       {LogQuery{Attributes: map[string]string{"stream": "stderr"}}, true},
		"attribute other":       {LogQuery{Attributes: map[string]string{"stream": "stdout"}}, false},
		"attribute missing":     {LogQuery{Attributes: map[string]string{"graphene.entity": "docker/pg"}}, false},
		"text case-insensitive": {LogQuery{Text: "Connection Refused"}, true},
		"text absent":           {LogQuery{Text: "timeout"}, false},
		"until after":           {LogQuery{Until: at.Add(time.Second)}, true},
		"until before":          {LogQuery{Until: at}, false},
	}
	for name, c := range cases {
		if got := c.q.Admits(rec); got != c.want {
			t.Errorf("%s: Admits=%v want %v", name, got, c.want)
		}
	}
}
