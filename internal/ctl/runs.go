package ctl

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"

	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// cmdRun is the run surface. A run is a record too: the observe
// dimensions apply with a bare run id.
func cmdRun(ctx context.Context, args []string) error {
	word, rest, err := need(args, "list, get, start, watch, result, cancel, events, logs, metrics, trace")
	if err != nil {
		return err
	}
	switch word {
	case "list":
		return runList(ctx, rest)
	case "get":
		return runGet(ctx, rest)
	case "start":
		return runStart(ctx, rest)
	case "watch":
		return runWatch(ctx, rest)
	case "result":
		return runResult(ctx, rest)
	case "cancel":
		return runCancel(ctx, rest)
	case "events", "logs", "metrics", "trace":
		return observeDimension(ctx, word, rest, "run/")
	default:
		return fmt.Errorf("run %q: unknown verb", word)
	}
}

func runList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run list", flag.ExitOnError)
	co := commonFlags(fs)
	status := fs.String("status", "", "status filter (Running, Completed, ...)")
	watch := fs.Bool("w", false, "watch: print the snapshot, then only changes")
	var labels labelFlag
	fs.Var(&labels, "l", "label selector k=v (repeatable)")
	_, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	d, err := co.dial()
	if err != nil {
		return err
	}
	list := func() (*managementv1.ListRunsResponse, error) {
		resp, err := d.Runs.ListRuns(ctx, connect.NewRequest(&managementv1.ListRunsRequest{
			Status: *status, Labels: labels.m,
		}))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}
	if *watch {
		return watchList(ctx, co, []string{"RUN", "PIPELINE", "STATUS", "LABELS"}, func() (map[string]watchRow, error) {
			msg, err := list()
			if err != nil {
				return nil, err
			}
			rows := make(map[string]watchRow, len(msg.GetRuns()))
			for _, r := range msg.GetRuns() {
				rows[r.GetRunId()] = watchRow{
					cols: []string{r.GetRunId(), r.GetPipeline(), r.GetStatus(), labelsCell(r.GetLabels())},
					msg:  r,
				}
			}
			return rows, nil
		})
	}
	msg, err := list()
	if err != nil {
		return err
	}
	if done, err := co.emit(msg); done || err != nil {
		return err
	}
	rows := make([][]string, 0, len(msg.GetRuns()))
	for _, r := range msg.GetRuns() {
		rows = append(rows, []string{r.GetRunId(), r.GetPipeline(), r.GetStatus(), labelsCell(r.GetLabels())})
	}
	table([]string{"RUN", "PIPELINE", "STATUS", "LABELS"}, rows)
	return nil
}

func runGet(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run get", flag.ExitOnError)
	co := commonFlags(fs)
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: run get <run-id>")
	}
	d, err := co.dial()
	if err != nil {
		return err
	}
	resp, err := d.Runs.GetRun(ctx, connect.NewRequest(&managementv1.GetRunRequest{RunId: pos[0]}))
	if err != nil {
		return err
	}
	if done, err := co.emit(resp.Msg); done || err != nil {
		return err
	}
	fmt.Fprintln(out, resp.Msg.GetStatus())
	return nil
}

// runStart submits a run of a REGISTERED pipeline: the image comes from
// the pipeline record unless overridden — re-run without a checkout.
func runStart(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run start", flag.ExitOnError)
	co := commonFlags(fs)
	runId := fs.String("run-id", "", "run id (default: derived from the pipeline and time)")
	params := fs.String("params", "", "typed params as JSON")
	image := fs.String("image", "", "worker image override (default: the pipeline record's)")
	watch := fs.Bool("watch", false, "follow the run and exit with its outcome")
	var labels labelFlag
	fs.Var(&labels, "l", "run label k=v (repeatable)")
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: run start <pipeline>")
	}
	pipelineId := pos[0]
	d, err := co.dial()
	if err != nil {
		return err
	}
	if *image == "" {
		// The pipeline record knows its current worker image.
		if st, err := getPipeline(ctx, d.cc, pipelineId); err == nil {
			*image = st.GetImage()
		}
	}
	id := *runId
	if id == "" {
		id = fmt.Sprintf("%s-%s", pipelineId, time.Now().UTC().Format("20060102-150405"))
	}
	_, err = d.Runs.StartRun(ctx, connect.NewRequest(&managementv1.StartRunRequest{
		RunId:    id,
		Pipeline: pipelineId,
		Params:   []byte(*params),
		Image:    *image,
		Labels:   labels.m,
	}))
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "run %s started (managed: %v)\n", id, *image != "")
	if !*watch {
		fmt.Fprintln(out, id)
		return nil
	}
	return watchToEnd(ctx, d, id)
}

func runWatch(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run watch", flag.ExitOnError)
	co := commonFlags(fs)
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: run watch <run-id>")
	}
	d, err := co.dial()
	if err != nil {
		return err
	}
	return watchToEnd(ctx, d, pos[0])
}

// watchToEnd follows the watch stream to a terminal status, prints the
// result on success, and mirrors the outcome in the error.
func watchToEnd(ctx context.Context, d *door, runId string) error {
	last := ""
	stream, err := d.Runs.WatchRun(ctx, connect.NewRequest(&managementv1.WatchRunRequest{RunId: runId}))
	if err != nil {
		return err
	}
	for stream.Receive() {
		if s := stream.Msg().GetStatus(); s != last {
			fmt.Fprintf(os.Stderr, "run %s: %s\n", runId, s)
			last = s
		}
	}
	if err := stream.Err(); err != nil {
		return fmt.Errorf("watch: %w", err)
	}
	switch last {
	case "Completed":
		resp, err := d.Runs.RunResult(ctx, connect.NewRequest(&managementv1.RunResultRequest{RunId: runId}))
		if err != nil {
			return err
		}
		fmt.Fprintln(out, string(resp.Msg.GetResult()))
		return nil
	default:
		return fmt.Errorf("run %s: %s", runId, strings.ToLower(last))
	}
}

func runResult(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run result", flag.ExitOnError)
	co := commonFlags(fs)
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: run result <run-id>")
	}
	d, err := co.dial()
	if err != nil {
		return err
	}
	resp, err := d.Runs.RunResult(ctx, connect.NewRequest(&managementv1.RunResultRequest{RunId: pos[0]}))
	if err != nil {
		return err
	}
	fmt.Fprintln(out, string(resp.Msg.GetResult()))
	return nil
}

func runCancel(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run cancel", flag.ExitOnError)
	co := commonFlags(fs)
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: run cancel <run-id>")
	}
	d, err := co.dial()
	if err != nil {
		return err
	}
	if _, err := d.Runs.CancelRun(ctx, connect.NewRequest(&managementv1.CancelRunRequest{RunId: pos[0]})); err != nil {
		return err
	}
	fmt.Fprintf(out, "run %s: cancel requested (teardown still runs)\n", pos[0])
	return nil
}
