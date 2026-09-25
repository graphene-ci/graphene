package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

// The real backends, opt-in: GRAPHENE_TELEMETRY_IT=1 runs VictoriaLogs and
// VictoriaMetrics in throwaway containers (the images the compose stack
// pins; GRAPHENE_TELEMETRY_IT_VL / _VM override) and proves the scoped
// query surface end to end: signals of one record do not leak into
// another's answer across runs or namespaces, the caller's own expression
// cannot widen the scope, a page walk reaches every record once, and
// what the executor's own run reports is separable by role.
const (
	itVictoriaLogs    = "victoriametrics/victoria-logs:v1.52.0"
	itVictoriaMetrics = "victoriametrics/victoria-metrics:v1.151.0"
)

func startContainer(t *testing.T, image, port string, args ...string) string {
	t.Helper()
	run := append([]string{"run", "-d", "--rm", "-p", "127.0.0.1:0:" + port, image}, args...)
	out, err := exec.Command("docker", run...).Output() //nolint:gosec // the test drives the docker CLI with its own arguments
	if err != nil {
		t.Fatalf("docker run %s: %v", image, err)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() }) //nolint:gosec // removes the container this test started
	addr, err := exec.Command("docker", "port", id, port).Output() //nolint:gosec // id and port are the test's own
	if err != nil {
		t.Fatalf("docker port: %v", err)
	}
	base := "http://" + strings.TrimSpace(strings.Split(string(addr), "\n")[0])
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := http.Get(base + "/health") //nolint:gosec,noctx // a readiness poll of the test's own container
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return base
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not become healthy at %s", image, base)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func postProto(t *testing.T, url string, msg proto.Message) {
	t.Helper()
	body, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/x-protobuf", bytes.NewReader(body)) //nolint:gosec,noctx // ingest into the test's own container
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("post %s: %s", url, resp.Status)
	}
}

func kv(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

// resourceOf is what the door and the executor's intake stamp: the
// record's namespace and correlation axes.
func resourceOf(namespace, run, agent, role string) *resourcepb.Resource {
	return &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
		kv("graphene.namespace", namespace), kv("graphene.run", run), kv("graphene.agent", agent), kv("graphene.role", role),
		kv("service.name", "it"),
	}}
}

type line struct {
	at       time.Time
	body     string
	severity string
	stream   string
	role     string
}

func logsRequest(namespace, run string, lines []line) *collogspb.ExportLogsServiceRequest {
	byRole := map[string]*logspb.ResourceLogs{}
	req := &collogspb.ExportLogsServiceRequest{}
	for _, l := range lines {
		rl, ok := byRole[l.role]
		if !ok {
			rl = &logspb.ResourceLogs{Resource: resourceOf(namespace, run, "a1", l.role), ScopeLogs: []*logspb.ScopeLogs{{}}}
			byRole[l.role] = rl
			req.ResourceLogs = append(req.ResourceLogs, rl)
		}
		num := logspb.SeverityNumber_SEVERITY_NUMBER_INFO
		if l.severity == "ERROR" {
			num = logspb.SeverityNumber_SEVERITY_NUMBER_ERROR
		}
		rl.ScopeLogs[0].LogRecords = append(rl.ScopeLogs[0].LogRecords, &logspb.LogRecord{
			TimeUnixNano:   uint64(l.at.UnixNano()),
			SeverityText:   l.severity,
			SeverityNumber: num,
			Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: l.body}},
			Attributes:     []*commonpb.KeyValue{kv("stream", l.stream)},
		})
	}
	return req
}

func gaugeRequest(namespace, run string, value float64, at time.Time) *colmetricspb.ExportMetricsServiceRequest {
	var points []*metricspb.NumberDataPoint
	for i := range 5 {
		points = append(points, &metricspb.NumberDataPoint{
			TimeUnixNano: uint64(at.Add(time.Duration(i) * 15 * time.Second).UnixNano()),
			Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: value},
		})
	}
	return &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource: resourceOf(namespace, run, "a1", "workload"),
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{
			Name: "demo_ops", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: points}},
		}}}},
	}}}
}

func bodies(recs []LogRecord) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = r.Body
	}
	return out
}

// walk pages through the whole selection from the given order and returns
// every body in the order the pages delivered it.
func walk(t *testing.T, l *LogsQL, sel Selector, q LogQuery) []string {
	t.Helper()
	var out []string
	for pages := 0; ; pages++ {
		if pages > 50 {
			t.Fatal("page walk does not end")
		}
		page, err := l.Query(context.Background(), sel, q)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, bodies(page.Records)...)
		if !page.Truncated {
			return out
		}
		q.Cursor = page.Next
	}
}

func TestScopedQueriesAgainstVictoria(t *testing.T) {
	if os.Getenv("GRAPHENE_TELEMETRY_IT") == "" {
		t.Skip("set GRAPHENE_TELEMETRY_IT=1 to run against VictoriaLogs and VictoriaMetrics in docker")
	}
	vlImage, vmImage := itVictoriaLogs, itVictoriaMetrics
	if v := os.Getenv("GRAPHENE_TELEMETRY_IT_VL"); v != "" {
		vlImage = v
	}
	if v := os.Getenv("GRAPHENE_TELEMETRY_IT_VM"); v != "" {
		vmImage = v
	}
	vl := startContainer(t, vlImage, "9428")
	// The metrics backend hides the last 30s by default; the test writes
	// into the past and asks for it now.
	vm := startContainer(t, vmImage, "8428", "-search.latencyOffset=1s")
	ctx := context.Background()

	// Three records: alpha/run r1 (the subject), alpha/run r2 (a sibling
	// in the same tenant), beta/run r1 (the SAME run id in another
	// tenant). Seven lines of r1 share one instant — a cursor must walk
	// through them without loss or repeat.
	t0 := time.Now().Add(-5 * time.Minute).Truncate(time.Second)
	var r1 []line
	for i := range 7 {
		r1 = append(r1, line{t0, fmt.Sprintf("same-%d", i), "INFO", "stdout", "machine"})
	}
	for i := 1; i <= 5; i++ {
		sev, stream, role := "INFO", "stdout", "machine"
		if i == 2 || i == 4 {
			sev, stream, role = "ERROR", "stderr", "workload"
		}
		r1 = append(r1, line{t0.Add(time.Duration(i) * time.Second), fmt.Sprintf("later-%d", i), sev, stream, role})
	}
	postProto(t, vl+"/insert/opentelemetry/v1/logs", logsRequest("alpha", "r1", r1))
	postProto(t, vl+"/insert/opentelemetry/v1/logs", logsRequest("alpha", "r2", []line{
		{t0, "sibling-0", "INFO", "stdout", "machine"}, {t0.Add(time.Second), "sibling-1", "ERROR", "stderr", "machine"},
	}))
	postProto(t, vl+"/insert/opentelemetry/v1/logs", logsRequest("beta", "r1", []line{
		{t0, "tenant-0", "INFO", "stdout", "machine"}, {t0.Add(time.Second), "tenant-1", "ERROR", "stderr", "machine"},
	}))
	postProto(t, vm+"/opentelemetry/v1/metrics", gaugeRequest("alpha", "r1", 10, t0))
	postProto(t, vm+"/opentelemetry/v1/metrics", gaugeRequest("alpha", "r2", 20, t0))
	postProto(t, vm+"/opentelemetry/v1/metrics", gaugeRequest("beta", "r1", 30, t0))

	logs := &LogsQL{Base: vl, Client: http.DefaultClient}
	metrics := &PromQL{Base: vm, Client: http.DefaultClient, ExtraFilters: true}
	subject := SelectorFor("alpha", "run/r1")

	// Ingest is asynchronous on both sides: wait for the whole of r1.
	deadline := time.Now().Add(20 * time.Second)
	for {
		page, err := logs.Query(ctx, subject, LogQuery{Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Records) == 12 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("logs backend holds %d of 12 records of the subject", len(page.Records))
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Run("isolation", func(t *testing.T) {
		got := walk(t, logs, subject, LogQuery{Limit: 100})
		for _, b := range got {
			if strings.HasPrefix(b, "sibling") || strings.HasPrefix(b, "tenant") {
				t.Fatalf("another record's line in the subject's answer: %s", b)
			}
		}
		if len(got) != 12 {
			t.Fatalf("subject has %d lines, want 12: %v", len(got), got)
		}
		// The same run id in another tenant is that tenant's alone.
		other := walk(t, logs, SelectorFor("beta", "run/r1"), LogQuery{Limit: 100})
		if strings.Join(other, ",") != "tenant-0,tenant-1" {
			t.Fatalf("beta/run/r1 = %v", other)
		}
	})

	t.Run("bypass attempts", func(t *testing.T) {
		// An OR that would widen the scope stays inside the fence.
		got := walk(t, logs, subject, LogQuery{Limit: 100, Filter: `* OR "graphene.namespace":="beta" OR "graphene.run":="r2"`})
		if len(got) != 12 {
			t.Fatalf("widened: %d lines: %v", len(got), got)
		}
		// Naming another record inside the scope contradicts it: nothing.
		got = walk(t, logs, subject, LogQuery{Limit: 100, Filter: `"graphene.run":="r2"`})
		if len(got) != 0 {
			t.Fatalf("another run's lines through the filter: %v", got)
		}
		// A pipe inside the fence is not a valid filter: the caller's
		// fault, not the backend's.
		wide := `_time:[2000-01-01, 2100-01-01]`
		for _, filter := range []string{`* | delete "graphene.namespace"`, `later-1)`, wide} {
			_, err := logs.Query(ctx, subject, LogQuery{Limit: 100, Filter: filter})
			if filter == wide {
				// A time filter is legal; it cannot reach before the
				// record either — the bounds sit outside the fence.
				if err != nil {
					t.Fatalf("%s: %v", filter, err)
				}
				continue
			}
			var be *BackendError
			if !errors.As(err, &be) || !be.ClientFault() {
				t.Fatalf("filter %q: err=%v, want a client-fault backend error", filter, err)
			}
		}
	})

	t.Run("pagination", func(t *testing.T) {
		asc := walk(t, logs, subject, LogQuery{Limit: 3})
		want := []string{"same-0", "same-1", "same-2", "same-3", "same-4", "same-5", "same-6", "later-1", "later-2", "later-3", "later-4", "later-5"}
		if strings.Join(asc, ",") != strings.Join(want, ",") {
			t.Fatalf("asc walk by 3:\n got %v\nwant %v", asc, want)
		}
		desc := walk(t, logs, subject, LogQuery{Limit: 5, Desc: true})
		if len(desc) != 12 || desc[0] != "later-5" || desc[11] != "same-0" {
			t.Fatalf("desc walk by 5: %v", desc)
		}
		seen := map[string]bool{}
		for _, b := range desc {
			if seen[b] {
				t.Fatalf("repeated on the desc walk: %s", b)
			}
			seen[b] = true
		}
	})

	t.Run("selection", func(t *testing.T) {
		if got := walk(t, logs, subject, LogQuery{Severities: []string{"error"}}); strings.Join(got, ",") != "later-2,later-4" {
			t.Fatalf("severity error: %v", got)
		}
		if got := walk(t, logs, subject, LogQuery{Attributes: map[string]string{"stream": "stderr"}}); len(got) != 2 {
			t.Fatalf("stream stderr: %v", got)
		}
		if got := walk(t, logs, subject, LogQuery{Text: "later-3"}); strings.Join(got, ",") != "later-3" {
			t.Fatalf("text: %v", got)
		}
		// The executor's own lines and the workload's are told apart by role.
		if got := walk(t, logs, subject, LogQuery{Attributes: map[string]string{"graphene.role": "workload"}}); len(got) != 2 {
			t.Fatalf("role workload: %v", got)
		}
		// Birth bounds the record: a record born at t0+3s owns nothing older.
		born := subject
		born.Since = t0.Add(3 * time.Second)
		if got := walk(t, logs, born, LogQuery{}); strings.Join(got, ",") != "later-4,later-5" {
			t.Fatalf("born later: %v", got)
		}
		// A time window of the caller's is honored inside the bound.
		if got := walk(t, logs, subject, LogQuery{Since: t0.Add(time.Second), Until: t0.Add(4 * time.Second)}); strings.Join(got, ",") != "later-2,later-3" {
			t.Fatalf("since/until: %v", got)
		}
	})

	t.Run("facets", func(t *testing.T) {
		facets, err := logs.Facets(ctx, subject, LogQuery{}, []string{"stream", "graphene.role"}, 10)
		if err != nil {
			t.Fatal(err)
		}
		counts := map[string]int64{}
		for _, f := range facets {
			for _, v := range f.Values {
				counts[f.Field+"="+v.Value] = v.Hits
			}
		}
		want := map[string]int64{"stream=stdout": 10, "stream=stderr": 2, "graphene.role=machine": 10, "graphene.role=workload": 2}
		for k, n := range want {
			if counts[k] != n {
				t.Fatalf("facet %s = %d, want %d (all: %v)", k, counts[k], n, counts)
			}
		}
	})

	t.Run("metrics", func(t *testing.T) {
		window := MetricsQuery{Start: t0.Add(-time.Minute), End: t0.Add(3 * time.Minute), Step: 15 * time.Second}
		values := func(expr string, wait bool) []string {
			t.Helper()
			deadline := time.Now().Add(20 * time.Second)
			for {
				raw, err := metrics.Series(ctx, subject, MetricsQuery{Start: window.Start, End: window.End, Step: window.Step, Expr: expr})
				if err != nil {
					t.Fatal(err)
				}
				var reply struct {
					Data struct {
						Result []struct {
							Values [][2]any `json:"values"`
						} `json:"result"`
					} `json:"data"`
				}
				if err := json.Unmarshal(raw, &reply); err != nil {
					t.Fatal(err)
				}
				var out []string
				for _, r := range reply.Data.Result {
					for _, v := range r.Values {
						out = append(out, fmt.Sprint(v[1]))
					}
				}
				if len(out) > 0 || !wait || time.Now().After(deadline) {
					return out
				}
				time.Sleep(200 * time.Millisecond)
			}
		}
		distinct := func(vals []string) map[string]bool {
			set := map[string]bool{}
			for _, v := range vals {
				set[v] = true
			}
			return set
		}
		// The bare form: the record's own series.
		if got := distinct(values("", true)); len(got) != 1 || !got["10"] {
			t.Fatalf("own series: %v", got)
		}
		// The scoped form: the tenant's expression, fenced to the record —
		// a sum sees 10, never 10+20+30.
		if got := distinct(values("sum(demo_ops)", true)); len(got) != 1 || !got["10"] {
			t.Fatalf("sum(demo_ops) scoped: %v", got)
		}
		// Every selector of the expression is fenced, a wildcard included.
		if got := distinct(values(`sum({__name__=~".+"})`, true)); len(got) != 1 || !got["10"] {
			t.Fatalf("wildcard scoped: %v", got)
		}
		// Naming another record inside contradicts the fence: nothing.
		if got := values(`sum(demo_ops{graphene_run="r2"}) or sum(demo_ops{"graphene.run"="r2"})`, false); len(got) != 0 {
			t.Fatalf("another run through the expression: %v", got)
		}
	})
}
