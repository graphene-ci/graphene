package ctl

import (
	"context"
	"flag"
	"fmt"

	"connectrpc.com/connect"

	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// observeDimension serves the shared dimensions 2-5 for any ref.
// refPrefix ("run/") lets the run command take bare ids.
func observeDimension(ctx context.Context, dim string, args []string, refPrefix string) error {
	fs := flag.NewFlagSet(dim, flag.ExitOnError)
	co := commonFlags(fs)
	follow := fs.Bool("follow", false, "keep streaming new entries")
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: %s <ref>", dim)
	}
	ref := refPrefix + pos[0]
	d, err := co.dial()
	if err != nil {
		return err
	}
	switch dim {
	case "events":
		stream, err := d.Observe.Events(ctx, connect.NewRequest(&managementv1.EventsRequest{
			Ref: ref, Follow: *follow,
		}))
		if err != nil {
			return err
		}
		for stream.Receive() {
			ev := stream.Msg()
			if *co.output == "json" {
				if err := printJSON(ev); err != nil {
					return err
				}
				continue
			}
			line := fmt.Sprintf("%s  %-24s %s", stamp(ev.GetTimeUnixNano()), ev.GetKind(), ev.GetSubject())
			if ev.GetAgent() != "" {
				line += "  @" + ev.GetAgent()
			}
			if ev.GetError() != "" {
				line += "  error: " + ev.GetError()
			}
			fmt.Fprintln(out, line)
		}
		return stream.Err()
	case "logs":
		stream, err := d.Observe.Logs(ctx, connect.NewRequest(&managementv1.LogsRequest{
			Ref: ref, Follow: *follow,
		}))
		if err != nil {
			return err
		}
		for stream.Receive() {
			rec := stream.Msg()
			if *co.output == "json" {
				if err := printJSON(rec); err != nil {
					return err
				}
				continue
			}
			fmt.Fprintf(out, "%s  %s\n", stamp(rec.GetTimeUnixNano()), rec.GetBody())
		}
		return stream.Err()
	case "metrics":
		resp, err := d.Observe.Metrics(ctx, connect.NewRequest(&managementv1.MetricsRequest{Ref: ref}))
		if err != nil {
			return err
		}
		// The series is the backend's standard PromQL JSON either way.
		fmt.Fprintln(out, string(resp.Msg.GetSeries()))
		return nil
	case "trace":
		resp, err := d.Observe.Trace(ctx, connect.NewRequest(&managementv1.TraceRequest{Ref: ref}))
		if err != nil {
			return err
		}
		fmt.Fprintln(out, string(resp.Msg.GetTrace()))
		return nil
	default:
		return fmt.Errorf("unknown dimension %q", dim)
	}
}
