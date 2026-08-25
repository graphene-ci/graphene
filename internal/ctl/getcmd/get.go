// Package getcmd is `graphenectl get`: listing records of a kind and
// reading one in full — dimension 1, the state.
package getcmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// options is the kubectl-style bag: flags land here, Run reads them.
type options struct {
	phase  string
	owner  string
	labels map[string]string
	watch  bool
	chunk  int
}

// New builds the command.
func New(f *cmdutil.Factory) *cobra.Command {
	o := &options{}
	cmd := &cobra.Command{
		Use:   "get all|<kind> [id]",
		Short: "List records of a kind, or read one in full",
		Long: `List records of a kind — or all of them — and read one record in
full: dimension 1 of the five, the state. A run is a kind like any
other (get run); the listing then shows run columns.`,
		Args: cobra.RangeArgs(1, 2),
		ValidArgsFunction: func(cmd *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
			switch len(args) {
			case 0:
				return append(f.LiveKinds(), "all"), cobra.ShellCompDirectiveNoFileComp
			case 1:
				return f.LiveIds(args[0]), cobra.ShellCompDirectiveNoFileComp
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.run(cmd.Context(), f, args)
		},
	}
	fl := cmd.Flags()
	fl.StringVarP(&o.phase, "phase", "p", "", "lifecycle filter: a record phase (creating, ready, ...) or a run status (Running, Completed, ...)")
	fl.StringVar(&o.owner, "owner", "", "owner ref filter (run/x, stand/p, agent/vm-1)")
	fl.StringToStringVarP(&o.labels, "selector", "l", nil, "label selector k=v (repeatable)")
	fl.BoolVarP(&o.watch, "watch", "w", false, "watch: print the snapshot, then only changes")
	fl.IntVar(&o.chunk, "chunk-size", 500, "list page size (0 — one unpaginated request)")
	return cmd
}

func (o *options) run(ctx context.Context, f *cmdutil.Factory, args []string) error {
	switch {
	case len(args) == 1 && !strings.Contains(args[0], "/"):
		kind := args[0]
		if kind == "run" {
			return RunList(ctx, f, o.phase, o.labels, o.watch, o.chunk)
		}
		if kind == "all" {
			kind = ""
		}
		return o.list(ctx, f, kind)
	default:
		ref, rest, err := cmdutil.TargetRef(args)
		if err != nil {
			return err
		}
		if len(rest) != 0 {
			return fmt.Errorf("get: unexpected arguments after the target: %v", rest)
		}
		if strings.HasPrefix(ref, "run/") {
			return runGetOne(ctx, f, strings.TrimPrefix(ref, "run/"))
		}
		return o.getOne(ctx, f, ref)
	}
}

func (o *options) list(ctx context.Context, f *cmdutil.Factory, kind string) error {
	d, err := f.Dial()
	if err != nil {
		return err
	}
	// The chunked walk is invisible: pages accumulate into one reply,
	// kubectl's --chunk-size stance.
	list := func() (*managementv1.ListResponse, error) {
		acc := &managementv1.ListResponse{}
		token := ""
		for {
			resp, err := d.Resources.List(ctx, connect.NewRequest(&managementv1.ListRequest{
				Selector:  &managementv1.Selector{Kind: kind, Phase: o.phase, Owner: o.owner, Labels: o.labels},
				PageSize:  int32(o.chunk), //nolint:gosec // a small flag value
				PageToken: token,
			}))
			if err != nil {
				return nil, err
			}
			acc.Resources = append(acc.Resources, resp.Msg.GetResources()...)
			token = resp.Msg.GetNextPageToken()
			if o.chunk == 0 || token == "" {
				return acc, nil
			}
		}
	}
	header := []string{"REF", "PHASE", "OWNER", "LABELS"}
	cols := func(r *managementv1.Resource) []string {
		return []string{r.GetRef(), r.GetPhase(), r.GetOwner(), cmdutil.LabelsCell(r.GetLabels())}
	}
	switch f.Output {
	case "wide":
		header = []string{"REF", "PHASE", "OWNER", "PENDING", "DELETING", "LABELS"}
		cols = func(r *managementv1.Resource) []string {
			return []string{r.GetRef(), r.GetPhase(), r.GetOwner(),
				fmt.Sprint(r.GetPendingCommands()), fmt.Sprint(r.GetMarkedForDeletion()), cmdutil.LabelsCell(r.GetLabels())}
		}
	case "name":
		header = []string{"REF"}
		cols = func(r *managementv1.Resource) []string { return []string{r.GetRef()} }
	}
	if o.watch {
		return f.WatchList(ctx, header, func() (map[string]cmdutil.WatchRow, error) {
			msg, err := list()
			if err != nil {
				return nil, err
			}
			rows := make(map[string]cmdutil.WatchRow, len(msg.GetResources()))
			for _, r := range msg.GetResources() {
				rows[r.GetRef()] = cmdutil.WatchRow{Cols: cols(r), Msg: r}
			}
			return rows, nil
		})
	}
	msg, err := list()
	if err != nil {
		return err
	}
	if done, err := f.Emit(msg); done || err != nil {
		return err
	}
	if f.Output == "name" {
		for _, r := range msg.GetResources() {
			fmt.Fprintln(cmdutil.Out, r.GetRef())
		}
		return nil
	}
	if len(msg.GetResources()) == 0 {
		fmt.Fprintln(os.Stderr, "No records found.")
		return nil
	}
	rows := make([][]string, 0, len(msg.GetResources()))
	for _, r := range msg.GetResources() {
		rows = append(rows, cols(r))
	}
	cmdutil.Table(header, rows)
	return nil
}

func (o *options) getOne(ctx context.Context, f *cmdutil.Factory, ref string) error {
	d, err := f.Dial()
	if err != nil {
		return err
	}
	resp, err := d.Resources.Get(ctx, connect.NewRequest(&managementv1.GetRequest{Ref: ref}))
	if err != nil {
		// The raw not-found is Temporal's "workflow not found" phrasing.
		if connect.CodeOf(err) == connect.CodeNotFound {
			return fmt.Errorf("no record %s", ref)
		}
		return err
	}
	if done, err := f.Emit(resp.Msg); done || err != nil {
		return err
	}
	r := resp.Msg.GetResource()
	fmt.Fprintf(cmdutil.Out, "ref:    %s\nphase:  %s\nowner:  %s\nlabels: %s\n",
		r.GetRef(), r.GetPhase(), r.GetOwner(), cmdutil.LabelsCell(r.GetLabels()))
	cmdutil.PrintJSONBlock("spec", r.GetSpec())
	cmdutil.PrintJSONBlock("state", r.GetState())
	return nil
}

func runGetOne(ctx context.Context, f *cmdutil.Factory, runId string) error {
	d, err := f.Dial()
	if err != nil {
		return err
	}
	resp, err := d.Runs.GetRun(ctx, connect.NewRequest(&managementv1.GetRunRequest{RunId: runId}))
	if err != nil {
		if connect.CodeOf(err) == connect.CodeNotFound {
			return fmt.Errorf("no run %s", runId)
		}
		return err
	}
	if done, err := f.Emit(resp.Msg); done || err != nil {
		return err
	}
	fmt.Fprintln(cmdutil.Out, resp.Msg.GetStatus())
	return nil
}

// RunList lists runs — shared with `run list`.
func RunList(ctx context.Context, f *cmdutil.Factory, status string, labels map[string]string, watch bool, chunk int) error {
	d, err := f.Dial()
	if err != nil {
		return err
	}
	// Runs list through ResourcesAPI under the system kind "run".
	query := "kind=run"
	if status != "" {
		query += ", phase=" + status
	}
	for k, v := range labels {
		query += fmt.Sprintf(", label.%s=%s", k, v)
	}
	list := func() (*managementv1.ListResponse, error) {
		acc := &managementv1.ListResponse{}
		token := ""
		for {
			resp, err := d.Resources.List(ctx, connect.NewRequest(&managementv1.ListRequest{
				Query:     query,
				PageSize:  int32(chunk), //nolint:gosec // a small flag value
				PageToken: token,
			}))
			if err != nil {
				return nil, err
			}
			acc.Resources = append(acc.Resources, resp.Msg.GetResources()...)
			token = resp.Msg.GetNextPageToken()
			if chunk == 0 || token == "" {
				return acc, nil
			}
		}
	}
	runId := func(r *managementv1.Resource) string {
		return strings.TrimPrefix(r.GetRef(), "run/")
	}
	pipelineOf := func(r *managementv1.Resource) string {
		return r.GetLabels()["graphene.io/pipeline"]
	}
	userLabels := func(r *managementv1.Resource) map[string]string {
		out := make(map[string]string, len(r.GetLabels()))
		for k, v := range r.GetLabels() {
			if k != "graphene.io/pipeline" {
				out[k] = v
			}
		}
		return out
	}
	header := []string{"RUN", "PIPELINE", "STATUS", "LABELS"}
	if watch {
		return f.WatchList(ctx, header, func() (map[string]cmdutil.WatchRow, error) {
			msg, err := list()
			if err != nil {
				return nil, err
			}
			rows := make(map[string]cmdutil.WatchRow, len(msg.GetResources()))
			for _, r := range msg.GetResources() {
				rows[runId(r)] = cmdutil.WatchRow{
					Cols: []string{runId(r), pipelineOf(r), r.GetPhase(), cmdutil.LabelsCell(userLabels(r))},
					Msg:  r,
				}
			}
			return rows, nil
		})
	}
	msg, err := list()
	if err != nil {
		return err
	}
	if done, err := f.Emit(msg); done || err != nil {
		return err
	}
	if f.Output == "name" {
		for _, r := range msg.GetResources() {
			fmt.Fprintln(cmdutil.Out, runId(r))
		}
		return nil
	}
	if len(msg.GetResources()) == 0 {
		fmt.Fprintln(os.Stderr, "No runs found.")
		return nil
	}
	rows := make([][]string, 0, len(msg.GetResources()))
	for _, r := range msg.GetResources() {
		rows = append(rows, []string{runId(r), pipelineOf(r), r.GetPhase(), cmdutil.LabelsCell(userLabels(r))})
	}
	cmdutil.Table(header, rows)
	return nil
}
