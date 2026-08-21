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

// cmdRun carries the run LIFECYCLE verbs (kubectl rollout's stance);
// reading runs is the record grammar: get run [id], events run <id>...
func cmdRun(ctx context.Context, args []string) error {
	word, rest, err := need(args, "start, watch, result, cancel, list")
	if err != nil {
		return err
	}
	switch word {
	case "list":
		return runList(ctx, rest)
	case "start":
		return runStart(ctx, rest)
	case "watch":
		return runWatch(ctx, rest)
	case "result":
		return runResult(ctx, rest)
	case "cancel":
		return runCancel(ctx, rest)
	default:
		return fmt.Errorf("run %q: want start, watch, result, cancel or list", word)
	}
}

// runList is sugar over `get run`.
func runList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run list", flag.ExitOnError)
	co := commonFlags(fs)
	status := fs.String("p", "", "status filter (Running, Completed, ...)")
	watch := fs.Bool("w", false, "watch: print the snapshot, then only changes")
	chunk := fs.Int("chunk-size", 500, "list page size (0 — one unpaginated request)")
	var labels labelFlag
	fs.Var(&labels, "l", "label selector k=v (repeatable)")
	if _, err := parseMixed(fs, args); err != nil {
		return err
	}
	return runListWith(ctx, co, *status, labels.m, *watch, *chunk)
}

func runListWith(ctx context.Context, co *common, status string, labels map[string]string, watch bool, chunk int) error {
	d, err := co.dial()
	if err != nil {
		return err
	}
	// The chunked walk is invisible: pages accumulate into one reply.
	list := func() (*managementv1.ListRunsResponse, error) {
		acc := &managementv1.ListRunsResponse{}
		token := ""
		for {
			resp, err := d.Runs.ListRuns(ctx, connect.NewRequest(&managementv1.ListRunsRequest{
				Status: status, Labels: labels,
				PageSize:  int32(chunk), //nolint:gosec // a small flag value
				PageToken: token,
			}))
			if err != nil {
				return nil, err
			}
			acc.Runs = append(acc.Runs, resp.Msg.GetRuns()...)
			token = resp.Msg.GetNextPageToken()
			if chunk == 0 || token == "" {
				return acc, nil
			}
		}
	}
	header := []string{"RUN", "PIPELINE", "STATUS", "LABELS"}
	if watch {
		return watchList(ctx, co, header, func() (map[string]watchRow, error) {
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
	if *co.output == "name" {
		for _, r := range msg.GetRuns() {
			fmt.Fprintln(out, r.GetRunId())
		}
		return nil
	}
	if len(msg.GetRuns()) == 0 {
		fmt.Fprintln(os.Stderr, "No runs found.")
		return nil
	}
	rows := make([][]string, 0, len(msg.GetRuns()))
	for _, r := range msg.GetRuns() {
		rows = append(rows, []string{r.GetRunId(), r.GetPipeline(), r.GetStatus(), labelsCell(r.GetLabels())})
	}
	table(header, rows)
	return nil
}

// runGetOne shows one run — the record grammar's `get run <id>`.
func runGetOne(ctx context.Context, co *common, runId string) error {
	d, err := co.dial()
	if err != nil {
		return err
	}
	resp, err := d.Runs.GetRun(ctx, connect.NewRequest(&managementv1.GetRunRequest{RunId: runId}))
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
	params := fs.String("params", "", "typed params as inline JSON")
	paramsFile := fs.String("params-file", "", "typed params from a JSON/YAML file (- for stdin)")
	image := fs.String("image", "", "worker image override (default: the pipeline record's)")
	watch := fs.Bool("watch", false, "follow the run and exit with its outcome")
	plain := fs.Bool("plain", false, "watch as an append-only feed (the non-TTY form)")
	collapse := fs.Bool("collapse", false, "watch: fold ready resources to one line")
	logsMode := fs.String("logs", logsTail, "watch logs per node: none | tail | all")
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
	// The pipeline record supplies the worker image and, on a TTY with
	// no params given, the schema for the terminal form.
	rec, recErr := getPipeline(ctx, d.cc, pipelineId)
	if *image == "" && recErr == nil {
		*image = rec.GetImage()
	}
	paramsJSON, err := jsonInput("params", *params, *paramsFile)
	if err != nil {
		return err
	}
	if len(paramsJSON) == 0 && recErr == nil && stdinIsTerminal() {
		if schema := paramsSchemaOf(rec.GetManifest()); schema != nil {
			paramsJSON, err = promptParams(os.Stdin, schema)
			if err != nil {
				return err
			}
		}
	}
	id := *runId
	if id == "" {
		id = fmt.Sprintf("%s-%s", pipelineId, time.Now().UTC().Format("20060102-150405"))
	}
	_, err = d.Runs.StartRun(ctx, connect.NewRequest(&managementv1.StartRunRequest{
		RunId:    id,
		Pipeline: pipelineId,
		Params:   paramsJSON,
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
	return watchToEnd(ctx, d, id, watchOptions{plain: *plain, collapse: *collapse, logs: *logsMode})
}

func runWatch(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run watch", flag.ExitOnError)
	co := commonFlags(fs)
	plain := fs.Bool("plain", false, "watch as an append-only feed (the non-TTY form)")
	collapse := fs.Bool("collapse", false, "fold ready resources to one line")
	logsMode := fs.String("logs", logsTail, "logs per node: none | tail | all")
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
	return watchToEnd(ctx, d, pos[0], watchOptions{plain: *plain, collapse: *collapse, logs: *logsMode})
}

// watchToEnd runs the rich watch (a plain feed off a terminal) to a
// terminal status, prints the result on success, and mirrors the
// outcome in the error.
func watchToEnd(ctx context.Context, d *door, runId string, opts watchOptions) error {
	switch opts.logs {
	case logsNone, logsTail, logsAll:
	default:
		return fmt.Errorf("--logs %q: want none, tail or all", opts.logs)
	}
	if !stdoutIsTerminal() {
		opts.plain = true
	}
	last, err := richWatch(ctx, d, runId, opts)
	if err != nil {
		return err
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
