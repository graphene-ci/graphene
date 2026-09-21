// Package observecmd is dimensions 2-5 as top-level verbs: events,
// logs, metrics, trace — over any record target.
package observecmd

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	"github.com/graphene-ci/graphene/internal/ctl/ui"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// New builds one observe verb.
func New(f *cmdutil.Factory, dim, short string) *cobra.Command {
	var follow bool
	cmd := &cobra.Command{
		Use:   dim + " <kind> <id>",
		Short: short,
		Args:  cobra.RangeArgs(1, 2),
		ValidArgsFunction: func(cmd *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
			switch len(args) {
			case 0:
				return f.LiveKinds(), cobra.ShellCompDirectiveNoFileComp
			case 1:
				return f.LiveIDs(args[0]), cobra.ShellCompDirectiveNoFileComp
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			window, err := ReadWindow(cmd, dim)
			if err != nil {
				return err
			}
			// The RAW view: one argument in the backend's own language
			// ("gctl metrics 'rate(...)'"), over the whole store. A
			// record target never contains these characters.
			if len(args) == 1 && strings.ContainsAny(args[0], "{}()|=* ") {
				return RunQuery(cmd.Context(), f, dim, args[0], window)
			}
			ref, rest, err := cmdutil.TargetRef(args)
			if err != nil || len(rest) != 0 {
				return fmt.Errorf("usage: %s <kind> <id>, or %s '<backend query>'", dim, dim)
			}
			return Run(cmd.Context(), f, dim, ref, follow, window)
		},
	}
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "keep streaming live entries (push from the collector, no polling)")
	if dim == "metrics" {
		BindWindowFlags(cmd)
	}
	return cmd
}

// Run executes one dimension read — the shared engine of the verb
// form ("gctl logs pipeline/x") and the resource-first form
// ("gctl pipeline/x logs").
func Run(ctx context.Context, f *cmdutil.Factory, dim, ref string, follow bool, window Window) error {
	d, err := f.Dial()
	if err != nil {
		return err
	}
	switch dim {
	case "events":
		stream, err := d.Observe.Events(ctx, connect.NewRequest(&managementv1.EventsRequest{
			Ref: ref, Follow: follow,
		}))
		if err != nil {
			return err
		}
		n, hidden := 0, 0
		for stream.Receive() {
			n++
			ev := stream.Msg()
			if done, err := f.Emit(ev); err != nil {
				return err
			} else if done {
				continue
			}
			// Temporal's own bookkeeping (workflow tasks, timers) is most
			// of a history and none of its story; -o wide keeps it.
			if strings.HasPrefix(ev.GetKind(), "internal-") && f.Output != "wide" {
				hidden++
				continue
			}
			fmt.Fprintln(cmdutil.Out, eventLine(ev))
		}
		if err := stream.Err(); err != nil {
			return cmdutil.OrNoRecord(err, ref)
		}
		if n == 0 && !follow {
			if err := d.Exists(ctx, ref); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "%s has no events.\n", ref)
		}
		if hidden > 0 && !follow {
			fmt.Fprintln(os.Stderr, ui.Gray(fmt.Sprintf("… %d internal events hidden; -o wide shows them", hidden)))
		}
		return nil
	case "logs":
		stream, err := d.Observe.Logs(ctx, connect.NewRequest(&managementv1.LogsRequest{
			Ref: ref, Follow: follow,
		}))
		if err != nil {
			return err
		}
		n := 0
		for stream.Receive() {
			chunk := stream.Msg()
			if done, err := f.Emit(chunk); err != nil {
				return err
			} else if done {
				continue
			}
			if d := chunk.GetDropped(); d > 0 {
				fmt.Fprintf(os.Stderr, "... %d lines dropped (slow consumer)\n", d)
				continue
			}
			rec := chunk.GetRecord()
			if rec == nil {
				continue
			}
			n++
			fmt.Fprintln(cmdutil.Out, logLine(rec, f.Output == "wide"))
		}
		if err := stream.Err(); err != nil {
			return err
		}
		if n == 0 && !follow {
			if err := d.Exists(ctx, ref); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "%s has no log records.\n", ref)
		}
		return nil
	case "metrics":
		stream, err := d.Observe.Metrics(ctx, connect.NewRequest(&managementv1.MetricsRequest{Ref: ref, Follow: follow, StartUnixNano: window.Start, EndUnixNano: window.End}))
		if err != nil {
			return err
		}
		for stream.Receive() {
			chunk := stream.Msg()
			if done, err := f.Emit(chunk); err != nil {
				return err
			} else if done {
				continue
			}
			switch {
			case chunk.GetSnapshot() != nil:
				if err := renderMetrics(f, chunk.GetSnapshot()); err != nil {
					return err
				}
			case chunk.GetOtlp() != nil:
				renderLiveMetrics(chunk.GetOtlp())
			case chunk.GetDropped() > 0:
				fmt.Fprintf(os.Stderr, "... %d metric batches dropped\n", chunk.GetDropped())
			}
		}
		return stream.Err()
	case "trace":
		stream, err := d.Observe.Trace(ctx, connect.NewRequest(&managementv1.TraceRequest{Ref: ref, Follow: follow}))
		if err != nil {
			return err
		}
		for stream.Receive() {
			chunk := stream.Msg()
			if done, err := f.Emit(chunk); err != nil {
				return err
			} else if done {
				continue
			}
			switch {
			case chunk.GetSnapshot() != nil:
				if err := renderTrace(f, chunk.GetSnapshot()); err != nil {
					return err
				}
			case chunk.GetOtlp() != nil:
				renderLiveSpans(chunk.GetOtlp())
			case chunk.GetDropped() > 0:
				fmt.Fprintf(os.Stderr, "... %d span batches dropped\n", chunk.GetDropped())
			}
		}
		return stream.Err()
	default:
		return fmt.Errorf("unknown dimension %q", dim)
	}
}

// eventLine renders one history event: when, what (colored by how it
// went), about what, where, and the error if there is one.
func eventLine(ev *managementv1.Event) string {
	kind := ev.GetKind()
	styled := ui.Pad(kind, 20)
	switch {
	case strings.HasSuffix(kind, "-failed"), strings.HasSuffix(kind, "-timed-out"), strings.HasSuffix(kind, "-terminated"):
		styled = ui.Red(styled)
	case strings.HasSuffix(kind, "-completed"):
		styled = ui.Green(styled)
	case strings.HasSuffix(kind, "-started"), strings.HasSuffix(kind, "-scheduled"):
		styled = ui.Yellow(styled)
	case strings.HasSuffix(kind, "-canceled"):
		styled = ui.Purple(styled)
	case strings.HasPrefix(kind, "internal-"):
		styled = ui.Gray(styled)
	default:
		styled = ui.Cyan(styled)
	}
	line := ui.Gray(cmdutil.Stamp(ev.GetTimeUnixNano())) + "  " + styled
	if ev.GetSubject() != "" {
		line += " " + ev.GetSubject()
	}
	if ev.GetAgent() != "" {
		line += "  " + ui.Blue("@"+ev.GetAgent())
	}
	if ev.GetError() != "" {
		// With no subject the error takes the subject's column.
		gap := "  "
		if ev.GetSubject() == "" && ev.GetAgent() == "" {
			gap = " "
		}
		line += gap + ui.Red(ev.GetError())
	}
	// The kind is padded to a column; with nothing after it the padding is
	// trailing whitespace.
	return strings.TrimRight(line, " ")
}

// logLine renders one log record: time, a three-letter level colored by
// severity, the source a library put on it (a job's name), the body. The
// wide form appends every attribute.
func logLine(rec *managementv1.LogRecord, wide bool) string {
	level, paintBody := logLevel(rec.GetSeverity())
	line := ui.Gray(cmdutil.Stamp(rec.GetTimeUnixNano())) + "  " + level + "  "
	if job := rec.GetAttributes()["job"]; job != "" {
		line += ui.Blue(job) + ui.Gray(" │ ")
	}
	line += paintBody(rec.GetBody())
	if wide {
		keys := make([]string, 0, len(rec.GetAttributes()))
		for k := range rec.GetAttributes() {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			line += "  " + ui.Gray(k+"="+rec.GetAttributes()[k])
		}
	}
	return line
}

// logLevel maps an OTLP severity text onto a fixed-width tag and the
// style of the body: a warning or an error must not look like the rest.
func logLevel(severity string) (string, func(string) string) {
	// An ordinary line keeps its level and gets its telling words marked:
	// a tool's output (pytest, a compiler) carries no severity of its own.
	plain := ui.Highlight
	switch sev := strings.ToUpper(severity); {
	case strings.HasPrefix(sev, "ERR"), strings.HasPrefix(sev, "FATAL"):
		return ui.Red("ERR"), ui.Red
	case strings.HasPrefix(sev, "WARN"):
		return ui.Yellow("WRN"), ui.Yellow
	case strings.HasPrefix(sev, "DEBUG"), strings.HasPrefix(sev, "TRACE"):
		return ui.Gray("DBG"), ui.Gray
	}
	return ui.Green("INF"), plain
}

// renderLiveMetrics prints one live OTLP metric batch: standard OTel
// bytes, decoded with the standard types.
func renderLiveMetrics(raw []byte) {
	var req colmetricspb.ExportMetricsServiceRequest
	if proto.Unmarshal(raw, &req) != nil {
		return
	}
	for _, rm := range req.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				for _, p := range numberPoints(m) {
					fmt.Fprintf(cmdutil.Out, "%s  %s = %g\n",
						cmdutil.Stamp(int64(p.GetTimeUnixNano())), m.GetName(), pointValue(p)) //nolint:gosec // otel nanos
				}
			}
		}
	}
}

func numberPoints(m *metricspb.Metric) []*metricspb.NumberDataPoint {
	switch data := m.GetData().(type) {
	case *metricspb.Metric_Sum:
		return data.Sum.GetDataPoints()
	case *metricspb.Metric_Gauge:
		return data.Gauge.GetDataPoints()
	}
	return nil
}

func pointValue(p *metricspb.NumberDataPoint) float64 {
	if _, ok := p.GetValue().(*metricspb.NumberDataPoint_AsInt); ok {
		return float64(p.GetAsInt())
	}
	return p.GetAsDouble()
}

// renderLiveSpans prints one live OTLP span batch.
func renderLiveSpans(raw []byte) {
	var req coltracepb.ExportTraceServiceRequest
	if proto.Unmarshal(raw, &req) != nil {
		return
	}
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, span := range ss.GetSpans() {
				ms := float64(span.GetEndTimeUnixNano()-span.GetStartTimeUnixNano()) / 1e6
				line := fmt.Sprintf("%s  %-32s %8.1fms", cmdutil.Stamp(int64(span.GetStartTimeUnixNano())), span.GetName(), ms) //nolint:gosec // otel nanos
				if span.GetStatus().GetCode() == tracepb.Status_STATUS_CODE_ERROR {
					line += "  error: " + span.GetStatus().GetMessage()
				}
				fmt.Fprintln(cmdutil.Out, line)
			}
		}
	}
}

// RunQuery executes one raw backend query through the door.
func RunQuery(ctx context.Context, f *cmdutil.Factory, dim, query string, window Window) error {
	d, err := f.Dial()
	if err != nil {
		return err
	}
	switch dim {
	case "logs":
		stream, err := d.Observe.Logs(ctx, connect.NewRequest(&managementv1.LogsRequest{Query: query}))
		if err != nil {
			return err
		}
		for stream.Receive() {
			if rec := stream.Msg().GetRecord(); rec != nil {
				fmt.Fprintln(cmdutil.Out, logLine(rec, f.Output == "wide"))
			}
		}
		return stream.Err()
	case "metrics":
		stream, err := d.Observe.Metrics(ctx, connect.NewRequest(&managementv1.MetricsRequest{Query: query, StartUnixNano: window.Start, EndUnixNano: window.End}))
		if err != nil {
			return err
		}
		for stream.Receive() {
			if snap := stream.Msg().GetSnapshot(); snap != nil {
				if err := renderMetrics(f, snap); err != nil {
					return err
				}
			}
		}
		return stream.Err()
	case "trace":
		stream, err := d.Observe.Trace(ctx, connect.NewRequest(&managementv1.TraceRequest{Query: query}))
		if err != nil {
			return err
		}
		for stream.Receive() {
			if snap := stream.Msg().GetSnapshot(); snap != nil {
				if err := renderTrace(f, snap); err != nil {
					return err
				}
			}
		}
		return stream.Err()
	}
	return fmt.Errorf("%s has no raw query form", dim)
}
