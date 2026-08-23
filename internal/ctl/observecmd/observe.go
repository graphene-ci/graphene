// Package observecmd is dimensions 2-5 as top-level verbs: events,
// logs, metrics, trace — over any record target.
package observecmd

import (
	"context"
	"fmt"
	"os"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
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
				return f.LiveIds(args[0]), cobra.ShellCompDirectiveNoFileComp
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ref, rest, err := cmdutil.TargetRef(args)
			if err != nil || len(rest) != 0 {
				return fmt.Errorf("usage: %s <kind> <id>", dim)
			}
			return run(cmd.Context(), f, dim, ref, follow)
		},
	}
	if dim == "events" || dim == "logs" {
		cmd.Flags().BoolVar(&follow, "follow", false, "keep streaming new entries")
	}
	return cmd
}

func run(ctx context.Context, f *cmdutil.Factory, dim, ref string, follow bool) error {
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
			n++
			rec := stream.Msg()
			if done, err := f.Emit(rec); err != nil {
				return err
			} else if done {
				continue
			}
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
		resp, err := d.Observe.Metrics(ctx, connect.NewRequest(&managementv1.MetricsRequest{Ref: ref}))
		if err != nil {
			return err
		}
		return renderMetrics(f, resp.Msg.GetSeries())
	case "trace":
		resp, err := d.Observe.Trace(ctx, connect.NewRequest(&managementv1.TraceRequest{Ref: ref}))
		if err != nil {
			return err
		}
		return renderTrace(f, resp.Msg.GetTrace())
	default:
		return fmt.Errorf("unknown dimension %q", dim)
	}
}
