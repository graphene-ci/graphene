package observecmd

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
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
	type agg struct {
		count, sum, inf float64
		last            float64
		points          int
		labels          map[string]string
		isHist          bool
	}
	byKey := map[string]*agg{}
	order := []string{}
	for _, sr := range payload.Data.Result {
		name := sr.Metric["__name__"]
		base := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(name, "_bucket"), "_sum"), "_count")
		labels := cleanLabels(sr.Metric)
		key := base + "|" + labelKey(labels)
		a, ok := byKey[key]
		if !ok {
			a = &agg{labels: labels}
			byKey[key] = a
			order = append(order, key)
		}
		var lastVal float64
		if n := len(sr.Values); n > 0 {
			if _, err := fmt.Sscan(fmt.Sprint(sr.Values[n-1][1]), &lastVal); err != nil {
				return fmt.Errorf("metric %q value: %w", name, err)
			}
			if len(sr.Values) > a.points {
				a.points = len(sr.Values)
			}
		}
		switch {
		case strings.HasSuffix(name, "_count"):
			a.isHist, a.count = true, lastVal
		case strings.HasSuffix(name, "_sum"):
			a.isHist, a.sum = true, lastVal
		case strings.HasSuffix(name, "_bucket"):
			a.isHist = true
			if sr.Metric["le"] == "+Inf" {
				a.inf = lastVal
			}
		default:
			a.last = lastVal
		}
	}
	sort.Strings(order)
	rows := make([][]string, 0, len(order))
	for _, key := range order {
		a := byKey[key]
		base := strings.SplitN(key, "|", 2)[0]
		cell := base
		if lk := labelKey(a.labels); lk != "" {
			cell += "{" + lk + "}"
		}
		value := fmt.Sprintf("%g", a.last)
		if a.isHist {
			avg := 0.0
			if a.count > 0 {
				avg = a.sum / a.count
			}
			value = fmt.Sprintf("n=%g avg=%.3gs", a.count, avg)
		}
		rows = append(rows, []string{cell, fmt.Sprint(a.points), value})
	}
	return cmdutil.Table([]string{"METRIC", "POINTS", "VALUE"}, rows)
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
			Processes map[string]struct {
				ServiceName string `json:"serviceName"`
			} `json:"processes"`
			Spans []struct {
				OperationName string `json:"operationName"`
				ProcessID     string `json:"processID"`
				StartTime     int64  `json:"startTime"` // µs
				Duration      int64  `json:"duration"`  // µs
				Tags          []struct {
					Key   string `json:"key"`
					Value any    `json:"value"`
				} `json:"tags"`
			} `json:"spans"`
		} `json:"data"`
	}
	if err := json.Unmarshal(trace, &payload); err != nil {
		fmt.Fprintln(cmdutil.Out, string(trace))
		return nil
	}
	type row struct {
		start int64
		cols  []string
	}
	var rows []row
	for _, t := range payload.Data {
		for _, s := range t.Spans {
			errMark := ""
			for _, tag := range s.Tags {
				if tag.Key == "error" && fmt.Sprint(tag.Value) != "unset" && fmt.Sprint(tag.Value) != "false" {
					errMark = "error"
				}
			}
			rows = append(rows, row{s.StartTime, []string{
				cmdutil.Stamp(s.StartTime * int64(time.Microsecond)),
				fmt.Sprintf("%.1fms", float64(s.Duration)/1000),
				s.OperationName,
				t.Processes[s.ProcessID].ServiceName,
				errMark,
			}})
		}
	}
	if len(rows) == 0 {
		fmt.Fprintln(os.Stderr, "No trace recorded.")
		return nil
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].start < rows[j].start })
	cells := make([][]string, len(rows))
	for i, r := range rows {
		cells[i] = r.cols
	}
	return cmdutil.Table([]string{"START", "DURATION", "OPERATION", "SERVICE", ""}, cells)
}
