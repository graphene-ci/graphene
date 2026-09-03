package runcmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/graphene-ci/pipeline/pkg/cliconfig"
	workerplanev1 "github.com/graphene-ci/pipeline/pkg/proto/workerplane/v1"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	"github.com/graphene-ci/graphene/internal/ctl/getcmd"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// New builds the `run` command tree.
func New(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run lifecycle: start, watch, result, cancel, list",
	}
	cmd.AddCommand(newStart(f), newWatch(f), newResult(f), newCancel(f), newList(f), newStatus(f))
	return cmd
}

func runIdCompletion(f *cmdutil.Factory) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(cmd *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
		if len(args) == 0 {
			return f.LiveIDs("run"), cobra.ShellCompDirectiveNoFileComp
		}
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
}

func newStart(f *cmdutil.Factory) *cobra.Command {
	var (
		runId, params, paramsFile, image string
		labels                           map[string]string
		watch                            bool
		wopts                            = watchOptions{logs: logsTail}
	)
	cmd := &cobra.Command{
		Use:   "start <pipeline>",
		Short: "Start a run of a pushed pipeline",
		Long: `Start a run of an already pushed pipeline: the worker image comes
from the pipeline record, no checkout needed. Params validate against
the manifest at the door. On a terminal with no params given, an
interactive form walks the schema field by field.`,
		Args: cobra.ExactArgs(1),
		ValidArgsFunction: func(cmd *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return f.LiveIDs("pipeline"), cobra.ShellCompDirectiveNoFileComp
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			pipelineId := args[0]
			d, err := f.Dial()
			if err != nil {
				return err
			}
			// The pipeline record supplies the worker image and, on a
			// TTY with no params given, the schema for the terminal form.
			rec, recErr := GetPipelineRecord(ctx, d.CC, pipelineId)
			if image == "" && recErr == nil {
				image = rec.GetImage()
			}
			paramsJSON, err := cmdutil.JSONInput("params", params, paramsFile)
			if err != nil {
				return err
			}
			if len(paramsJSON) == 0 && recErr == nil && cmdutil.StdinIsTerminal() {
				if schema := paramsSchemaOf(rec.GetManifest()); schema != nil {
					paramsJSON, err = cmdutil.PromptSchema(os.Stdin, "params", schema)
					if err != nil {
						return err
					}
				}
			}
			id := runId
			if id == "" {
				id = fmt.Sprintf("%s-%s", pipelineId, time.Now().UTC().Format("20060102-150405"))
			}
			_, err = d.Runs.StartRun(ctx, connect.NewRequest(&managementv1.StartRunRequest{
				RunId:    id,
				Pipeline: pipelineId,
				Params:   paramsJSON,
				Image:    image,
				Labels:   labels,
			}))
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "run %s started (managed: %v)\n", id, image != "")
			if !watch {
				fmt.Fprintln(cmdutil.Out, id)
				return nil
			}
			return watchToEnd(ctx, f, d, id, wopts)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&runId, "run-id", "", "run id (default: derived from the pipeline and time)")
	fl.StringVar(&params, "params", "", "typed params as inline JSON")
	fl.StringVar(&paramsFile, "params-file", "", "typed params from a JSON/YAML file (- for stdin)")
	fl.StringVar(&image, "image", "", "worker image override (default: the pipeline record's)")
	fl.StringToStringVarP(&labels, "label", "l", nil, "run label k=v (repeatable)")
	fl.BoolVar(&watch, "watch", false, "follow the run and exit with its outcome")
	bindWatchFlags(cmd, &wopts)
	return cmd
}

func bindWatchFlags(cmd *cobra.Command, o *watchOptions) {
	fl := cmd.Flags()
	fl.BoolVar(&o.plain, "plain", false, "watch as an append-only feed (the non-TTY form)")
	fl.BoolVar(&o.collapse, "collapse", false, "watch: fold ready resources to one line")
	fl.StringVar(&o.logs, "logs", logsTail, "watch logs per node: none | tail | all")
}

func newWatch(f *cmdutil.Factory) *cobra.Command {
	var wopts = watchOptions{logs: logsTail}
	cmd := &cobra.Command{
		Use:   "watch <run-id>",
		Short: "Follow a run: a live tree of its resources",
		Long: `The live view of a run: the ownership tree of its resources with
phases, elapsed and retry counters, each node carrying its recent
history events and a log tail, the run's own strip at the bottom. Off
a terminal (or --plain) the same model prints as an append-only feed.`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: runIdCompletion(f),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			return watchToEnd(cmd.Context(), f, d, args[0], wopts)
		},
	}
	bindWatchFlags(cmd, &wopts)
	return cmd
}

// watchToEnd runs the rich watch (a plain feed off a terminal) to a
// terminal status, prints the result on success, and mirrors the
// outcome in the error.
func watchToEnd(ctx context.Context, f *cmdutil.Factory, d *cmdutil.Door, runId string, opts watchOptions) error {
	switch opts.logs {
	case logsNone, logsTail, logsAll:
	default:
		return fmt.Errorf("--logs %q: want none, tail or all", opts.logs)
	}
	if !cmdutil.StdoutIsTerminal() {
		opts.plain = true
	}
	last, err := richWatch(ctx, d, runId, opts)
	if err != nil {
		return err
	}
	switch last {
	case "Completed":
		result, err := fetchResult(ctx, d, runId)
		if err != nil {
			return err
		}
		fmt.Fprintln(cmdutil.Out, string(result))
		return nil
	default:
		return fmt.Errorf("run %s: %s", runId, strings.ToLower(last))
	}
}

// fetchResult wraps RunResult's failure modes into plain words — the
// raw error is Temporal's workflow-execution phrasing.
func fetchResult(ctx context.Context, d *cmdutil.Door, runId string) ([]byte, error) {
	resp, err := d.Runs.RunResult(ctx, connect.NewRequest(&managementv1.RunResultRequest{RunId: runId}))
	if err == nil {
		return resp.Msg.GetResult(), nil
	}
	switch connect.CodeOf(err) {
	case connect.CodeNotFound:
		return nil, fmt.Errorf("no run %s", runId)
	case connect.CodeFailedPrecondition:
		msg := err.Error()
		switch {
		case strings.Contains(msg, "terminated"):
			return nil, fmt.Errorf("run %s was terminated — no result", runId)
		case strings.Contains(msg, "canceled"):
			return nil, fmt.Errorf("run %s was canceled — no result", runId)
		}
		return nil, fmt.Errorf("run %s did not complete — no result", runId)
	case connect.CodeCanceled, connect.CodeUnknown, connect.CodeInvalidArgument,
		connect.CodeDeadlineExceeded, connect.CodeAlreadyExists,
		connect.CodePermissionDenied, connect.CodeResourceExhausted,
		connect.CodeAborted, connect.CodeOutOfRange, connect.CodeUnimplemented,
		connect.CodeInternal, connect.CodeUnavailable, connect.CodeDataLoss,
		connect.CodeUnauthenticated:
		return nil, err
	}
	return nil, err
}

func newResult(f *cmdutil.Factory) *cobra.Command {
	return &cobra.Command{
		Use:               "result <run-id>",
		Short:             "Wait for a run and print its typed result",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: runIdCompletion(f),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			result, err := fetchResult(cmd.Context(), d, args[0])
			if err != nil {
				return err
			}
			fmt.Fprintln(cmdutil.Out, string(result))
			return nil
		},
	}
}

func newCancel(f *cmdutil.Factory) *cobra.Command {
	return &cobra.Command{
		Use:               "cancel <run-id>",
		Short:             "Ask a run to stop; teardown still runs",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: runIdCompletion(f),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			if _, err := d.Runs.CancelRun(cmd.Context(), connect.NewRequest(&managementv1.CancelRunRequest{RunId: args[0]})); err != nil {
				return err
			}
			fmt.Fprintf(cmdutil.Out, "run %s: cancel requested (teardown still runs)\n", args[0])
			return nil
		},
	}
}

func newStatus(f *cmdutil.Factory) *cobra.Command {
	return &cobra.Command{
		Use:               "status <run-id>",
		Short:             "Show what the run is doing right now (pending activities, attempts, last failure)",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: runIdCompletion(f),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			resp, err := d.Runs.RunStatus(cmd.Context(), connect.NewRequest(&managementv1.RunStatusRequest{RunId: args[0]}))
			if err != nil {
				return err
			}
			msg := resp.Msg
			fmt.Fprintf(cmdutil.Out, "run %s: %s\n", args[0], msg.GetStatus())
			if len(msg.GetPending()) == 0 {
				fmt.Fprintln(cmdutil.Out, "  no activity in flight")
				return nil
			}
			for _, p := range msg.GetPending() {
				fmt.Fprintf(cmdutil.Out, "  %s (%s, attempt %d)\n", p.GetActivityType(), strings.ToLower(strings.TrimPrefix(p.GetState(), "PENDING_ACTIVITY_STATE_")), p.GetAttempt())
				if d := p.GetHeartbeatDetail(); d != "" {
					fmt.Fprintf(cmdutil.Out, "    doing: %s\n", d)
				}
				if lf := p.GetLastFailure(); lf != "" {
					fmt.Fprintf(cmdutil.Out, "    last failure: %s\n", lf)
				}
				if ms := p.GetLastHeartbeatUnixMs(); ms > 0 {
					fmt.Fprintf(cmdutil.Out, "    last heartbeat: %s ago\n", time.Since(time.UnixMilli(ms)).Round(time.Second))
				}
			}
			return nil
		},
	}
}

func newList(f *cmdutil.Factory) *cobra.Command {
	var (
		status string
		labels map[string]string
		watch  bool
		chunk  int
	)
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List runs (same as: get run)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return getcmd.RunList(cmd.Context(), f, status, labels, watch, chunk)
		},
	}
	fl := cmd.Flags()
	fl.StringVarP(&status, "phase", "p", "", "status filter (Running, Completed, ...)")
	fl.StringToStringVarP(&labels, "selector", "l", nil, "label selector k=v (repeatable)")
	fl.BoolVarP(&watch, "watch", "w", false, "watch: print the snapshot, then only changes")
	fl.IntVar(&chunk, "chunk-size", 500, "list page size (0 — one unpaginated request)")
	return cmd
}

// GetPipelineRecord reads the record over the gRPC half of the same door.
func GetPipelineRecord(ctx context.Context, cc cliconfig.Context, pipelineId string) (*workerplanev1.GetPipelineResponse, error) {
	creds := credentials.NewTLS(nil)
	if cc.Insecure {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(cc.Server,
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(bearer{cc: cc}),
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	return workerplanev1.NewManifestAPIClient(conn).GetPipeline(ctx,
		&workerplanev1.GetPipelineRequest{PipelineId: pipelineId})
}

type bearer struct{ cc cliconfig.Context }

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	md := map[string]string{"authorization": "Bearer " + b.cc.Token}
	if b.cc.Namespace != "" {
		md["x-graphene-namespace"] = b.cc.Namespace
	}
	return md, nil
}

func (b bearer) RequireTransportSecurity() bool { return !b.cc.Insecure }
