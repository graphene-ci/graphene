// Package getcmd is `graphenectl get`: listing records of a kind and
// reading one in full — dimension 1, the state.
package getcmd

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	"github.com/graphene-ci/graphene/internal/ctl/ui"
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
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("name a kind — `get agent`, `get run`, `get all`; `graphenectl kinds` lists what this installation has")
			}
			return cobra.MaximumNArgs(2)(cmd, args)
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
			switch len(args) {
			case 0:
				return append(f.LiveKinds(), "all"), cobra.ShellCompDirectiveNoFileComp
			case 1:
				return f.LiveIDs(args[0]), cobra.ShellCompDirectiveNoFileComp
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.run(cmd.Context(), f, args)
		},
	}
	fl := cmd.Flags()
	fl.StringVarP(&o.phase, "phase", "p", "", "phase filter: creating, ready, deleted, ... for records; running, completed, failed, canceled, terminated, timed-out for runs")
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
			for _, r := range resp.Msg.GetResources() {
				// "all" means the installation's records; the dictionary
				// of kinds is `get kind` / `kinds`, and would bury them.
				if kind == "" && r.GetKind() == "kind" {
					continue
				}
				acc.Resources = append(acc.Resources, r)
			}
			token = resp.Msg.GetNextPageToken()
			if o.chunk == 0 || token == "" {
				sort.SliceStable(acc.Resources, func(i, j int) bool {
					return acc.Resources[i].GetRef() < acc.Resources[j].GetRef()
				})
				return acc, nil
			}
		}
	}
	header := []string{"REF", "PHASE", "OWNER", "AGE", "LABELS"}
	cols := func(r *managementv1.Resource) []string {
		return []string{ui.Ref(r.GetRef()), ui.Phase(r.GetPhase()), ui.Ref(r.GetOwner()), cmdutil.Age(r.GetStartedAt()), ui.Dim(cmdutil.LabelsCell(cmdutil.UserLabels(r.GetLabels())))}
	}
	switch f.Output {
	case "wide":
		header = []string{"REF", "PHASE", "OWNER", "AGE", "PENDING", "DELETING", "LABELS"}
		cols = func(r *managementv1.Resource) []string {
			return []string{ui.Ref(r.GetRef()), ui.Phase(r.GetPhase()), ui.Ref(r.GetOwner()), cmdutil.Age(r.GetStartedAt()),
				fmt.Sprint(r.GetPendingCommands()), fmt.Sprint(r.GetMarkedForDeletion()), ui.Dim(cmdutil.LabelsCell(r.GetLabels()))}
		}
	case "name":
		header = []string{"REF"}
		cols = func(r *managementv1.Resource) []string { return []string{r.GetRef()} }
	}
	if o.watch {
		// AGE ticks on its own: a watch reports CHANGES, and a second
		// passing is not one — the column stays out of it.
		if f.Output != "name" {
			const age = 3
			header = append(header[:age:age], header[age+1:]...)
			withAge := cols
			cols = func(r *managementv1.Resource) []string {
				c := withAge(r)
				return append(c[:age:age], c[age+1:]...)
			}
		}
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
	if len(msg.GetResources()) == 0 {
		// An empty listing is only honest about a kind that exists.
		if kind != "" {
			if err := f.CheckKind(ctx, d, kind); err != nil {
				return err
			}
		}
		if f.Output != "name" {
			fmt.Fprintln(os.Stderr, emptyListing(kind, o))
		}
		return nil
	}
	if f.Output == "name" {
		for _, r := range msg.GetResources() {
			fmt.Fprintln(cmdutil.Out, r.GetRef())
		}
		return nil
	}
	// Rows arrive sorted by ref, so kinds are contiguous: a blank line
	// between them turns one long column into readable groups.
	table := ui.NewTable(header...).Flex(len(header) - 1)
	lastKind := ""
	for _, r := range msg.GetResources() {
		if lastKind != "" && r.GetKind() != lastKind {
			table.Break()
		}
		lastKind = r.GetKind()
		table.Row(cols(r)...)
	}
	return table.Render(cmdutil.Out, ui.Width())
}

// emptyListing says what was NOT found, in the words of the question: the
// kind, and the filters that narrowed it — a filter is the usual reason a
// listing is empty.
func emptyListing(kind string, o *options) string {
	what := "records"
	if kind != "" {
		what = kind + " records"
	}
	var filters []string
	if o.phase != "" {
		filters = append(filters, "phase "+o.phase)
	}
	if o.owner != "" {
		filters = append(filters, "owner "+o.owner)
	}
	if len(o.labels) > 0 {
		filters = append(filters, "labels "+cmdutil.LabelsCell(o.labels))
	}
	if len(filters) > 0 {
		return fmt.Sprintf("No live %s match %s.", what, strings.Join(filters, ", "))
	}
	return fmt.Sprintf("No live %s.", what)
}

func (o *options) getOne(ctx context.Context, f *cmdutil.Factory, ref string) error {
	d, err := f.Dial()
	if err != nil {
		return err
	}
	r, err := d.Lookup(ctx, ref)
	if err != nil {
		return err
	}
	if done, err := f.Emit(&managementv1.GetResponse{Resource: r}); done || err != nil {
		return err
	}
	deleting := ""
	if r.GetMarkedForDeletion() {
		deleting = ui.Purple("marked — finalize is on its way")
	}
	pending := ""
	if n := r.GetPendingCommands(); n > 0 {
		pending = fmt.Sprint(n)
	}
	if err := ui.Fields(cmdutil.Out,
		[2]string{"ref", ui.Bold(r.GetRef())},
		[2]string{"phase", ui.Phase(r.GetPhase())},
		[2]string{"owner", ui.Ref(r.GetOwner())},
		[2]string{"age", cmdutil.Age(r.GetStartedAt())},
		[2]string{"labels", cmdutil.LabelsCell(r.GetLabels())},
		[2]string{"pending", pending},
		[2]string{"deletion", deleting},
	); err != nil {
		return err
	}
	if flows := r.GetFlows(); len(flows) > 0 {
		fmt.Fprintln(cmdutil.Out, ui.Bold(ui.Gray("flows:")))
		for _, f := range flows {
			proto := f.GetProtocol()
			if f.GetPort() > 0 {
				proto += ":" + strconv.Itoa(int(f.GetPort()))
			}
			arrow := ui.Cyan("→")
			if f.GetVirtual() {
				arrow = ui.Gray("⇢")
			}
			fmt.Fprintf(cmdutil.Out, "  %s %s  %s  %s\n", arrow, ui.Ref(f.GetTo()), ui.Cyan(proto), ui.Gray(f.GetLabel()))
		}
	}
	specFolded, err := ui.Block(cmdutil.Out, "spec", r.GetSpec())
	if err != nil {
		return err
	}
	stateFolded, err := ui.Block(cmdutil.Out, "state", r.GetState())
	if err != nil {
		return err
	}
	if specFolded || stateFolded {
		fmt.Fprintln(os.Stderr, ui.Gray("… long parts are folded; -o yaml shows the whole record"))
	}
	return nil
}

// runGetOne reads one run as a RECORD: its row's facts and its state —
// the result of a completed run, the error and the partial result of one
// that did not complete.
func runGetOne(ctx context.Context, f *cmdutil.Factory, runId string) error {
	d, err := f.Dial()
	if err != nil {
		return err
	}
	r, err := d.Lookup(ctx, "run/"+runId)
	if err != nil {
		return err
	}
	if done, err := f.Emit(&managementv1.GetResponse{Resource: r}); done || err != nil {
		return err
	}
	if err := ui.Fields(cmdutil.Out,
		[2]string{"run", ui.Bold(runId)},
		[2]string{"pipeline", pipelineOf(r)},
		[2]string{"status", ui.Phase(r.GetPhase())},
		[2]string{"started", cmdutil.When(r.GetStartedAt())},
		[2]string{"took", cmdutil.Took(r.GetStartedAt(), r.GetFinishedAt())},
		[2]string{"image", r.GetLabels()["graphene.io/image"]},
		[2]string{"trigger", r.GetLabels()["graphene.io/trigger"]},
		[2]string{"labels", cmdutil.LabelsCell(cmdutil.UserLabels(r.GetLabels()))},
	); err != nil {
		return err
	}
	folded, err := ui.Block(cmdutil.Out, "state", r.GetState())
	if err != nil {
		return err
	}
	if folded {
		fmt.Fprintln(os.Stderr, ui.Gray("… long parts are folded; -o yaml shows the whole record"))
	}
	return nil
}

const pipelineLabel = "graphene.io/pipeline"

func pipelineOf(r *managementv1.Resource) string { return r.GetLabels()[pipelineLabel] }

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
	// The default table shows what a person labelled the run with; the
	// installation's own labels (image, trigger) come with -o wide.
	labelsOf := func(r *managementv1.Resource) string {
		if f.Output == "wide" {
			all := make(map[string]string, len(r.GetLabels()))
			for k, v := range r.GetLabels() {
				if k != pipelineLabel {
					all[k] = v
				}
			}
			return cmdutil.LabelsCell(all)
		}
		return cmdutil.LabelsCell(cmdutil.UserLabels(r.GetLabels()))
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
					Cols: []string{runId(r), pipelineOf(r), ui.Phase(r.GetPhase()), ui.Dim(labelsOf(r))},
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
	table := ui.NewTable("RUN", "PIPELINE", "STATUS", "STARTED", "TOOK", "LABELS").Right(3, 4).Flex(5)
	for _, r := range msg.GetResources() {
		table.Row(runId(r), pipelineOf(r), ui.Phase(r.GetPhase()),
			cmdutil.Age(r.GetStartedAt())+" ago", cmdutil.Took(r.GetStartedAt(), r.GetFinishedAt()), ui.Dim(labelsOf(r)))
	}
	return table.Render(cmdutil.Out, ui.Width())
}
