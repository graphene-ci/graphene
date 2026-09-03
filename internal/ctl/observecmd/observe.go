// Package observecmd is dimensions 2-5 as top-level verbs: events,
// logs, metrics, trace — over any record target.
package observecmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
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
			// The RAW view: one argument in the backend's own language
			// ("gctl metrics 'rate(...)'"), over the whole store. A
			// record target never contains these characters.
			if len(args) == 1 && strings.ContainsAny(args[0], "{}()|=* ") {
				return RunQuery(cmd.Context(), f, dim, args[0])
			}
			ref, rest, err := cmdutil.TargetRef(args)
			if err != nil || len(rest) != 0 {
				return fmt.Errorf("usage: %s <kind> <id>, or %s '<backend query>'", dim, dim)
			}
			return Run(cmd.Context(), f, dim, ref, follow)
		},
	}
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "keep streaming live entries (push from the collector, no polling)")
	return cmd
}

// Run executes one dimension read — the shared engine of the verb
// form ("gctl logs pipeline/x") and the resource-first form
// ("gctl pipeline/x logs").
func Run(ctx context.Context, f *cmdutil.Factory, dim, ref string, follow bool) error {
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
		n := 0
		for stream.Receive() {
			n++
			ev := stream.Msg()
			if done, err := f.Emit(ev); err != nil {
				return err
			} else if done {
				continue
			}
			line := fmt.Sprintf("%s  %-24s %s", cmdutil.Stamp(ev.GetTimeUnixNano()), ev.GetKind(), ev.GetSubject())
			if ev.GetAgent() != "" {
				line += "  @" + ev.GetAgent()
			}
			if ev.GetError() != "" {
				line += "  error: " + ev.GetError()
			}
			fmt.Fprintln(cmdutil.Out, line)
		}
		if err := stream.Err(); err != nil {
			return err
		}
		if n == 0 && !follow {
			fmt.Fprintln(os.Stderr, "No events.")
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
			fmt.Fprintf(cmdutil.Out, "%s  %s\n", cmdutil.Stamp(rec.GetTimeUnixNano()), rec.GetBody())
		}
		if err := stream.Err(); err != nil {
			return err
		}
		if n == 0 && !follow {
			fmt.Fprintln(os.Stderr, "No log records.")
		}
		return nil
	case "metrics":
		stream, err := d.Observe.Metrics(ctx, connect.NewRequest(&managementv1.MetricsRequest{Ref: ref, Follow: follow}))
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
func RunQuery(ctx context.Context, f *cmdutil.Factory, dim, query string) error {
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
				fmt.Fprintf(cmdutil.Out, "%s  %s\n", cmdutil.Stamp(rec.GetTimeUnixNano()), rec.GetBody())
			}
		}
		return stream.Err()
	case "metrics":
		stream, err := d.Observe.Metrics(ctx, connect.NewRequest(&managementv1.MetricsRequest{Query: query}))
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
