package ctl

import (
	"context"
	"flag"
	"fmt"

	"connectrpc.com/connect"

	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// cmdObserve serves the shared dimensions 2-5 as top-level verbs:
// events|logs|metrics|trace <kind> <id> (or kind/id).
func cmdObserve(ctx context.Context, dim string, args []string) error {
	fs := flag.NewFlagSet(dim, flag.ExitOnError)
	co := commonFlags(fs)
	follow := fs.Bool("follow", false, "keep streaming new entries")
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	ref, rest, err := targetRef(pos)
	if err != nil || len(rest) != 0 {
		return fmt.Errorf("usage: %s <kind> <id>", dim)
	}
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
			if done, err := co.emit(ev); err != nil {
				return err
			} else if done {
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
			if done, err := co.emit(rec); err != nil {
				return err
			} else if done {
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
