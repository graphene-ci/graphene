package observecmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// Options is one dimension read as the flags spell it: the window, the
// selection of logs, the caller's own query inside the record's scope.
type Options struct {
	// Start/End bound metrics and logs (Unix nanoseconds; zero — the
	// server's default).
	Start, End int64
	// Step is the metrics resolution in seconds; zero lets the door pick.
	Step int32
	// Query is the caller's expression in the backend's language: with a
	// record it runs inside the record's scope, alone it is the raw view.
	Query string
	// Logs: the page and the filters.
	Limit      int32
	Desc       bool
	PageToken  string
	Severities []string
	Stream     string
	Agent      string
	Entity     string
	Text       string
	// Facets names the log fields to count instead of listing records.
	Facets []string
}

// BindFlags adds the dimension's flags to a command; dim "" binds every
// dimension's flags (the resource-first root).
func BindFlags(cmd *cobra.Command, dim string) {
	fl := cmd.Flags()
	if dim == "" || dim == "metrics" || dim == "logs" {
		fl.String("start", "", "range start: RFC3339, or a duration ago (-2h)")
		fl.String("end", "", "range end: RFC3339, or a duration ago (-10m)")
	}
	if dim == "" || dim == "metrics" {
		fl.String("step", "", "metrics resolution (30s, 1m); default: range/200, at least 15s")
	}
	if dim == "" || dim != "events" {
		fl.String("query", "", "your own query in the backend's language (LogsQL, PromQL, Jaeger params), evaluated inside the record")
	}
	if dim == "" || dim == "logs" {
		fl.Int32("limit", 0, "records per page (default 1000, at most 10000)")
		fl.Bool("desc", false, "newest first")
		fl.String("page", "", "continue from a page token the previous page printed")
		fl.StringSlice("severity", nil, "only these severities (repeatable): DEBUG, INFO, WARN, ERROR")
		fl.String("stream", "", "only this stream of a job: stdout or stderr")
		fl.String("agent", "", "only records emitted on this agent")
		fl.String("entity", "", "only records about this record (kind/id)")
		fl.String("text", "", "only records whose body contains this text")
		fl.StringSlice("facets", nil, "count the values of these fields instead of listing records (severity, stream, job, ...)")
	}
	if dim == "" || dim == "trace" {
		fl.Int32("traces", 0, "traces in the snapshot (default 20)")
	}
}

// ReadOptions validates the flags before opening a connection.
func ReadOptions(cmd *cobra.Command, dim string) (Options, error) {
	var o Options
	fl := cmd.Flags()
	str := func(name string) string {
		if f := fl.Lookup(name); f != nil && f.Changed {
			return f.Value.String()
		}
		return ""
	}
	for _, field := range []struct {
		name  string
		value *int64
	}{{"start", &o.Start}, {"end", &o.End}} {
		raw := str(field.name)
		if raw == "" {
			continue
		}
		if dim != "metrics" && dim != "logs" {
			return Options{}, fmt.Errorf("--%s applies to metrics and logs", field.name)
		}
		at, err := parseMoment(raw)
		if err != nil {
			return Options{}, fmt.Errorf("--%s: %w", field.name, err)
		}
		*field.value = at.UnixNano()
	}
	if o.Start > 0 && o.End > 0 && o.Start >= o.End {
		return Options{}, fmt.Errorf("--start must be before --end")
	}
	if raw := str("step"); raw != "" {
		if dim != "metrics" {
			return Options{}, fmt.Errorf("--step applies to metrics")
		}
		d, err := time.ParseDuration(raw)
		if err != nil || d < time.Second {
			return Options{}, fmt.Errorf("--step: want a duration of at least 1s (30s, 1m)")
		}
		o.Step = int32(d / time.Second) //nolint:gosec // a step in seconds
	}
	o.Query = str("query")
	if raw := str("limit"); raw != "" {
		if _, err := fmt.Sscan(raw, &o.Limit); err != nil || o.Limit < 0 {
			return Options{}, fmt.Errorf("--limit: want a positive number")
		}
	}
	if f := fl.Lookup("desc"); f != nil && f.Changed {
		o.Desc = true
	}
	o.PageToken, o.Stream, o.Agent, o.Entity, o.Text = str("page"), str("stream"), str("agent"), str("entity"), str("text")
	if f := fl.Lookup("severity"); f != nil && f.Changed {
		o.Severities, _ = fl.GetStringSlice("severity")
	}
	if f := fl.Lookup("facets"); f != nil && f.Changed {
		o.Facets, _ = fl.GetStringSlice("facets")
	}
	if raw := str("traces"); raw != "" {
		if _, err := fmt.Sscan(raw, &o.Limit); err != nil || o.Limit <= 0 {
			return Options{}, fmt.Errorf("--traces: want a positive number")
		}
	}
	return o, nil
}

// parseMoment reads an RFC3339 timestamp or a duration ago ("-2h", "2h").
func parseMoment(raw string) (time.Time, error) {
	if at, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return at, nil
	}
	d, err := time.ParseDuration(strings.TrimPrefix(raw, "-"))
	if err != nil {
		return time.Time{}, fmt.Errorf("want an RFC3339 timestamp or a duration ago (-2h)")
	}
	return time.Now().Add(-d), nil
}
