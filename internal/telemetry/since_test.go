package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A ref is a name and names are reused: agent/db-1 of this run must not read
// the signals of agent/db-1 of the last one. The record's birth is the lower
// bound of all three backends.

var (
	born = time.Date(2026, 9, 18, 11, 0, 0, 0, time.UTC)
	sel  = Selector{Namespace: "default", Attribute: "graphene.agent", Value: "db-1",
		AltAttribute: "graphene.entity", AltValue: "agent/db-1", Since: born}
)

func TestSelectorAfter(t *testing.T) {
	if got := sel.After(time.Time{}); !got.Equal(born) {
		t.Fatalf("no caller bound: %v, want the birth", got)
	}
	if got := sel.After(born.Add(-time.Hour)); !got.Equal(born) {
		t.Fatalf("caller bound before the birth: %v, want the birth", got)
	}
	cursor := born.Add(time.Minute)
	if got := sel.After(cursor); !got.Equal(cursor) {
		t.Fatalf("caller cursor after the birth: %v, want the cursor", got)
	}
	if got := (Selector{}).After(time.Time{}); !got.IsZero() {
		t.Fatalf("unknown birth must not invent a bound: %v", got)
	}
}

func TestLogsQueryIsBoundedByBirth(t *testing.T) {
	var query string
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		query = r.Form.Get("query")
	}))
	defer server.Close()
	l := &LogsQL{Base: server.URL, Client: server.Client()}
	// run watch asks a node's logs from the beginning: since is zero.
	if _, err := l.Query(context.Background(), sel, time.Time{}, 10); err != nil {
		t.Fatal(err)
	}
	if want := "_time:>2026-09-18T11:00:00Z"; !strings.Contains(query, want) {
		t.Fatalf("query %q lacks %q", query, want)
	}
}

func TestMetricsWindowStartsAtBirth(t *testing.T) {
	var start, end string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end = r.URL.Query().Get("start"), r.URL.Query().Get("end")
		_, _ = w.Write([]byte(`{"status":"success","data":{"result":[]}}`))
	}))
	defer server.Close()
	p := &PromQL{Base: server.URL, Client: server.Client()}
	windowEnd := born.Add(10 * time.Minute)
	if _, err := p.Series(context.Background(), sel, windowEnd.Add(-time.Hour), windowEnd); err != nil {
		t.Fatal(err)
	}
	if want := strconv.FormatInt(born.Unix(), 10); start != want {
		t.Fatalf("start = %s, want the birth %s", start, want)
	}
	// A window that ended before the record was born is empty, not inverted.
	if _, err := p.Series(context.Background(), sel, born.Add(-2*time.Hour), born.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if start != end {
		t.Fatalf("window before the birth: start %s, end %s — want an empty window", start, end)
	}
}

func TestTraceSearchStartsAtBirth(t *testing.T) {
	var starts []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/api/services") {
			_, _ = w.Write([]byte(`{"data":["graphene-pipeline"]}`))
			return
		}
		starts = append(starts, r.URL.Query().Get("start"))
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer server.Close()
	j := &Jaeger{Base: server.URL, Client: server.Client()}
	if _, err := j.Search(context.Background(), sel, 20); err != nil {
		t.Fatal(err)
	}
	want := strconv.FormatInt(born.UnixMicro(), 10)
	if len(starts) == 0 {
		t.Fatal("no trace query was made")
	}
	for _, got := range starts {
		if got != want {
			t.Fatalf("start = %q, want %q (microseconds)", got, want)
		}
	}
}
