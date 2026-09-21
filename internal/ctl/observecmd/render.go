package observecmd

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	"github.com/graphene-ci/graphene/internal/ctl/ui"
)

// The metrics and trace answers are backend passthroughs (PromQL and
// Jaeger JSON), not proto messages — so the output flags are honored
// here by hand: -o json prints the raw payload, --jq runs over it,
// and the default is a readable table.

func renderMetrics(f *cmdutil.Factory, series []byte) error {
	if f.JQ != "" {
		return cmdutil.JQBytes(f.JQ, series)
	}
	if f.Output == "json" || f.Output == "yaml" {
		fmt.Fprintln(cmdutil.Out, string(series))
		return nil
	}
	var payload struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(series, &payload); err != nil {
		// Not the PromQL shape — hand over the raw payload.
		fmt.Fprintln(cmdutil.Out, string(series))
		return nil
	}
	if len(payload.Data.Result) == 0 {
		fmt.Fprintln(os.Stderr, "No metrics recorded.")
		return nil
	}
	// The FULL render: histogram series fold into count/avg/max, the
	// SDK's own noise labels disappear, and what remains reads like a
	// dashboard row rather than a Prometheus dump.
	byKey := map[string]*metricSeries{}
	order := []string{}
	for _, sr := range payload.Data.Result {
		name := sr.Metric["__name__"]
		base := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(name, "_bucket"), "_sum"), "_count")
		labels := cleanLabels(sr.Metric)
		key := base + "|" + labelKey(labels)
		a, ok := byKey[key]
		if !ok {
			a = &metricSeries{name: base, labels: labels}
			byKey[key] = a
			order = append(order, key)
		}
		points := make([]float64, 0, len(sr.Values))
		var from, to time.Time
		for i, pair := range sr.Values {
			var v float64
			if _, err := fmt.Sscan(fmt.Sprint(pair[1]), &v); err != nil {
				return fmt.Errorf("metric %q value: %w", name, err)
			}
			points = append(points, v)
			if ts, ok := pair[0].(float64); ok {
				at := time.Unix(0, int64(ts*float64(time.Second)))
				if i == 0 {
					from = at
				}
				to = at
			}
		}
		switch {
		case strings.HasSuffix(name, "_count"):
			a.isHist, a.counts = true, points
			a.from, a.to = from, to
		case strings.HasSuffix(name, "_sum"):
			a.isHist, a.sums = true, points
		case strings.HasSuffix(name, "_bucket"):
			a.isHist = true
		default:
			a.points, a.from, a.to = points, from, to
		}
	}
	sort.Strings(order)
	// -o wide: every series as its own chart panel.
	if f.Output == "wide" {
		width := 72
		if w := ui.Width(); w > 0 {
			width = min(max(w-14, 20), 100)
		}
		for i, key := range order {
			a := byKey[key]
			if i > 0 {
				fmt.Fprintln(cmdutil.Out)
			}
			head := ui.Bold(a.name)
			if a.isHist {
				head += ui.Gray(" (average)")
			}
			if lk := compactLabels(a.labels); lk != "" {
				head += "  " + ui.Gray(lk)
			}
			fmt.Fprintln(cmdutil.Out, head)
			for _, row := range ui.Chart(a.drawn(), a.from, a.to, width, 6, a.format()) {
				fmt.Fprintln(cmdutil.Out, row)
			}
		}
		return nil
	}
	// The table: one row per series, the metric named once per group. N
	// is a histogram's observation count; VALUE its average, or the last
	// point of a plain series.
	// SERIES goes last: it is the one column of unbounded width, and the
	// last column is never padded — a pipe gets it whole, a terminal cut.
	table := ui.NewTable("METRIC", "N", "VALUE", "MIN", "MAX", "TREND", "SERIES").Right(1, 2, 3, 4).Flex(6)
	lastName := ""
	for _, key := range order {
		a := byKey[key]
		drawn, format := a.drawn(), a.format()
		value, lo, hi, n := "", "", "", ""
		if len(drawn) > 0 {
			minV, maxV := drawn[0], drawn[0]
			for _, v := range drawn {
				minV, maxV = min(minV, v), max(maxV, v)
			}
			value, lo, hi = format(drawn[len(drawn)-1]), format(minV), format(maxV)
		}
		if a.isHist && len(a.counts) > 0 {
			n = ui.Number(a.counts[len(a.counts)-1])
		}
		name := ""
		if a.name != lastName {
			name, lastName = a.name, a.name
		}
		table.Row(ui.Bold(name), n, ui.Bold(value), ui.Gray(lo), ui.Gray(hi), ui.Cyan(ui.Spark(drawn, 24)), ui.Gray(compactLabels(a.labels)))
	}
	if err := table.Render(cmdutil.Out, ui.Width()); err != nil {
		return err
	}
	if ui.Enabled() {
		fmt.Fprintln(os.Stderr, ui.Gray("… -o wide draws every series as a chart"))
	}
	return nil
}

// metricSeries is one metric with one label set, as it is read and drawn.
type metricSeries struct {
	name     string
	labels   map[string]string
	isHist   bool
	points   []float64 // a plain series
	counts   []float64 // a histogram's _count over time
	sums     []float64 // a histogram's _sum over time
	from, to time.Time
}

// drawn is what the chart shows: the points themselves, or a histogram's
// running average — its count alone is a ramp that says nothing.
func (s *metricSeries) drawn() []float64 {
	if !s.isHist {
		return s.points
	}
	n := min(len(s.counts), len(s.sums))
	out := make([]float64, 0, n)
	for i := range n {
		if s.counts[i] > 0 {
			out = append(out, s.sums[i]/s.counts[i])
		}
	}
	return out
}

// format picks the unit from the metric's NAME — OTel's own convention
// puts it there (…seconds, …bytes, …percent).
func (s *metricSeries) format() func(float64) string {
	switch {
	case strings.HasSuffix(s.name, "seconds"), strings.HasSuffix(s.name, "_s"):
		return func(v float64) string { return ui.Duration(time.Duration(v * float64(time.Second))) }
	case strings.HasSuffix(s.name, "_ms"):
		return func(v float64) string { return ui.Duration(time.Duration(v * float64(time.Millisecond))) }
	case strings.HasSuffix(s.name, "bytes"):
		return ui.Bytes
	case strings.HasSuffix(s.name, "percent"):
		return func(v float64) string { return fmt.Sprintf("%.2f%%", v) }
	}
	return ui.Number
}

// quietLabels say the same thing on every row of one record's metrics
// (which run, which contour) — or nothing worth a column (a fine outcome).
var quietLabels = map[string]string{
	"graphene.run": "", "graphene_run": "",
	"graphene.role": "", "graphene_role": "",
	"contour": "", "outcome": "ok",
}

// compactLabels renders a series' labels for a table cell: the quiet ones
// dropped, the "graphene." prefix with them — "activity=docker.job
// agent=db-1" reads, the full form does not.
func compactLabels(labels map[string]string) string {
	parts := make([]string, 0, len(labels))
	for k, v := range labels {
		if quiet, ok := quietLabels[k]; ok && (quiet == "" || quiet == v) {
			continue
		}
		k = strings.TrimPrefix(strings.TrimPrefix(k, "graphene."), "graphene_")
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return strings.Join(parts, " ")
}

// noiseLabels are the SDK's own stamps — true for every series, so
// they say nothing when reading one entity.
var noiseLabels = map[string]bool{
	"__name__": true, "le": true,
	"telemetry.sdk.language": true, "telemetry.sdk.name": true, "telemetry.sdk.version": true,
	"scope.name": true, "scope.version": true, "service.name": true,
	"graphene.namespace": true, "graphene.entity": true, "graphene.attempt": true,
	"telemetry_sdk_language": true, "telemetry_sdk_name": true, "telemetry_sdk_version": true,
	"scope_name": true, "scope_version": true, "service_name": true,
	"graphene_namespace": true, "graphene_entity": true, "graphene_attempt": true,
}

func cleanLabels(in map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range in {
		if !noiseLabels[k] {
			out[k] = v
		}
	}
	return out
}

func labelKey(labels map[string]string) string {
	parts := make([]string, 0, len(labels))
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func renderTrace(f *cmdutil.Factory, trace []byte) error {
	if f.JQ != "" {
		return cmdutil.JQBytes(f.JQ, trace)
	}
	if f.Output == "json" || f.Output == "yaml" {
		fmt.Fprintln(cmdutil.Out, string(trace))
		return nil
	}
	var payload struct {
		Data []struct {
			TraceID   string `json:"traceID"`
			Processes map[string]struct {
				ServiceName string `json:"serviceName"`
			} `json:"processes"`
			Spans []jaegerSpan `json:"spans"`
		} `json:"data"`
	}
	if err := json.Unmarshal(trace, &payload); err != nil {
		fmt.Fprintln(cmdutil.Out, string(trace))
		return nil
	}
	shown := 0
	for _, t := range payload.Data {
		if len(t.Spans) == 0 {
			continue
		}
		if shown > 0 {
			fmt.Fprintln(cmdutil.Out)
		}
		shown++
		services := make(map[string]string, len(t.Processes))
		for pid, p := range t.Processes {
			services[pid] = p.ServiceName
		}
		if err := renderWaterfall(t.TraceID, t.Spans, services); err != nil {
			return err
		}
	}
	if shown == 0 {
		fmt.Fprintln(os.Stderr, "No trace recorded.")
	}
	return nil
}

type jaegerSpan struct {
	SpanID        string `json:"spanID"`
	OperationName string `json:"operationName"`
	ProcessID     string `json:"processID"`
	StartTime     int64  `json:"startTime"` // µs
	Duration      int64  `json:"duration"`  // µs
	References    []struct {
		RefType string `json:"refType"`
		SpanID  string `json:"spanID"`
	} `json:"references"`
	Tags []struct {
		Key   string `json:"key"`
		Value any    `json:"value"`
	} `json:"tags"`
}

func (s jaegerSpan) failed() bool {
	for _, tag := range s.Tags {
		if tag.Key == "error" && fmt.Sprint(tag.Value) != "unset" && fmt.Sprint(tag.Value) != "false" {
			return true
		}
	}
	return false
}

func (s jaegerSpan) parent() string {
	for _, r := range s.References {
		if r.RefType == "CHILD_OF" {
			return r.SpanID
		}
	}
	return ""
}

// renderWaterfall draws one trace: spans as a tree by parentage, each with
// its bar on a shared time track — where it sat in the trace and for how
// long. A span whose parent is not in the answer is a root of its own.
func renderWaterfall(traceID string, spans []jaegerSpan, services map[string]string) error {
	known := make(map[string]bool, len(spans))
	for _, sp := range spans {
		known[sp.SpanID] = true
	}
	children := map[string][]jaegerSpan{}
	begin, end := spans[0].StartTime, spans[0].StartTime+spans[0].Duration
	for _, sp := range spans {
		parent := sp.parent()
		if !known[parent] {
			parent = ""
		}
		children[parent] = append(children[parent], sp)
		begin, end = min(begin, sp.StartTime), max(end, sp.StartTime+sp.Duration)
	}
	for _, list := range children {
		sort.Slice(list, func(i, j int) bool { return list[i].StartTime < list[j].StartTime })
	}
	total := time.Duration(end-begin) * time.Microsecond

	type line struct {
		name string
		span jaegerSpan
	}
	var lines []line
	var walk func(parent, prefix string)
	walk = func(parent, prefix string) {
		list := children[parent]
		for i, sp := range list {
			own, inherit := ui.TreeGlyphs(i == len(list)-1)
			if parent == "" {
				own, inherit = "", ""
			}
			name := sp.OperationName
			if svc := services[sp.ProcessID]; svc != "" {
				name += ui.Gray(" · " + svc)
			}
			lines = append(lines, line{name: ui.Gray(prefix+own) + name, span: sp})
			walk(sp.SpanID, prefix+inherit)
		}
	}
	walk("", "")

	nameWidth := 0
	for _, l := range lines {
		nameWidth = max(nameWidth, ui.Len(l.name))
	}
	track := 40
	if w := ui.Width(); w > 0 {
		nameWidth = min(nameWidth, max(w/2, 24))
		track = min(max(w-nameWidth-14, 16), 80)
	}
	fmt.Fprintf(cmdutil.Out, "%s %s  %s  %s\n", ui.Bold("trace"), traceID,
		ui.Gray(cmdutil.Stamp(begin*int64(time.Microsecond))), ui.Bold(ui.Duration(total)))
	for _, l := range lines {
		name := l.name
		if ui.Len(name) > nameWidth {
			name = ui.Cut(ui.Strip(name), nameWidth)
		}
		length := time.Duration(l.span.Duration) * time.Microsecond
		bar := ui.Span(time.Duration(l.span.StartTime-begin)*time.Microsecond, length, total, track)
		took := ui.PadLeft(ui.Duration(length), 9)
		if l.span.failed() {
			bar, took = ui.Red(bar), ui.Red(took)
		} else {
			bar = ui.Cyan(bar)
		}
		if _, err := fmt.Fprintf(cmdutil.Out, "%s  %s  %s%s\n", ui.Pad(name, nameWidth), took, ui.Gray("▏"), bar); err != nil {
			return err
		}
	}
	return nil
}
