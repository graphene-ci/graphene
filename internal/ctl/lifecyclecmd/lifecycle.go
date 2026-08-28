// Package lifecyclecmd carries the verbs that change a record's life:
// tree, delete, transfer, invoke.
package lifecyclecmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// targetCompletion completes <kind> then <id>.
func targetCompletion(f *cmdutil.Factory) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(cmd *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
		switch len(args) {
		case 0:
			return f.LiveKinds(), cobra.ShellCompDirectiveNoFileComp
		case 1:
			return f.LiveIds(args[0]), cobra.ShellCompDirectiveNoFileComp
		}
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
}

// commandCompletion completes the COMMAND of an invoke — from the
// installation's registry, never from a list kept here.
func commandCompletion(f *cmdutil.Factory) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		switch len(args) {
		case 0:
			return f.LiveKinds(), cobra.ShellCompDirectiveNoFileComp
		case 1:
			// Either "<kind> <id>" or "<kind>/<id> <command>".
			if kind, _, ok := strings.Cut(args[0], "/"); ok {
				return f.LiveCommands(kind), cobra.ShellCompDirectiveNoFileComp
			}
			return f.LiveIds(args[0]), cobra.ShellCompDirectiveNoFileComp
		case 2:
			return f.LiveCommands(args[0]), cobra.ShellCompDirectiveNoFileComp
		}
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
}

// NewTree builds `tree`.
func NewTree(f *cmdutil.Factory) *cobra.Command {
	return &cobra.Command{
		Use:   "tree [owner-ref]",
		Short: "The ownership tree under an owner; no owner — the forest's roots",
		Long: `The ownership tree under one owner: the recursive walk cascade
deletion uses, read-only — what dies with this owner. With no owner,
the roots of the forest: every record nobody owns, with its subtree.`,
		Args:              cobra.RangeArgs(0, 2),
		ValidArgsFunction: targetCompletion(f),
		RunE: func(cmd *cobra.Command, args []string) error {
			ref := ""
			if len(args) > 0 {
				var rest []string
				var err error
				ref, rest, err = cmdutil.TargetRef(args)
				if err != nil || len(rest) != 0 {
					return fmt.Errorf("usage: tree [owner-ref]")
				}
			}
			d, err := f.Dial()
			if err != nil {
				return err
			}
			resp, err := d.Resources.Tree(cmd.Context(), connect.NewRequest(&managementv1.TreeRequest{Owner: ref}))
			if err != nil {
				return err
			}
			if done, err := f.Emit(resp.Msg); done || err != nil {
				return err
			}
			fmt.Fprintln(cmdutil.Out, ref)
			for _, root := range resp.Msg.GetRoots() {
				printTree(root, "  ")
			}
			return nil
		},
	}
}

func printTree(node *managementv1.TreeNode, indent string) {
	r := node.GetResource()
	fmt.Fprintf(cmdutil.Out, "%s%s (%s)\n", indent, r.GetRef(), r.GetPhase())
	for _, child := range node.GetChildren() {
		printTree(child, indent+"  ")
	}
}

// NewDelete builds `delete`.
func NewDelete(f *cmdutil.Factory) *cobra.Command {
	var wait bool
	cmd := &cobra.Command{
		Use:   "delete <kind> <id>",
		Short: "Signal deletion; --wait blocks until gone",
		Long: `Signal deletion: the record's finalize tears the real resource
down, then the record reaches deleted. Owned children die first.`,
		Args:              cobra.RangeArgs(1, 2),
		ValidArgsFunction: targetCompletion(f),
		RunE: func(cmd *cobra.Command, args []string) error {
			ref, rest, err := cmdutil.TargetRef(args)
			if err != nil || len(rest) != 0 {
				return fmt.Errorf("usage: delete <kind> <id>")
			}
			return runDelete(cmd.Context(), f, ref, wait)
		},
	}
	cmd.Flags().BoolVar(&wait, "wait", false, "block until the record is gone (finalize done)")
	return cmd
}

func runDelete(ctx context.Context, f *cmdutil.Factory, ref string, wait bool) error {
	d, err := f.Dial()
	if err != nil {
		return err
	}
	if _, err := d.Resources.Delete(ctx, connect.NewRequest(&managementv1.DeleteRequest{Ref: ref})); err != nil {
		return err
	}
	if !wait {
		fmt.Fprintf(cmdutil.Out, "%s: deletion signaled\n", ref)
		return nil
	}
	fmt.Fprintf(os.Stderr, "%s: deleting...\n", ref)
	for {
		resp, err := d.Resources.Get(ctx, connect.NewRequest(&managementv1.GetRequest{Ref: ref}))
		if err != nil {
			// The record is gone entirely — that is the goal.
			if connect.CodeOf(err) == connect.CodeNotFound {
				break
			}
			return fmt.Errorf("wait: %w", err)
		}
		if resp.Msg.GetResource().GetPhase() == "deleted" {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	fmt.Fprintf(cmdutil.Out, "%s: deleted\n", ref)
	return nil
}

// NewTransfer builds `transfer`.
func NewTransfer(f *cmdutil.Factory) *cobra.Command {
	var keep time.Duration
	cmd := &cobra.Command{
		Use:   "transfer <kind> <id> <new-owner>",
		Short: "Give a record to a new owner; --keep bounds a stand stay",
		Long: `Ownership moves one way: you can give a record away, never take it
back. Transfer to a stand (stand/<pipelineId>) lets a resource outlive
its run; --keep bounds the stay — the stand's own timer collects it.`,
		Args:              cobra.RangeArgs(2, 3),
		ValidArgsFunction: targetCompletion(f),
		RunE: func(cmd *cobra.Command, args []string) error {
			ref, rest, err := cmdutil.TargetRef(args)
			if err != nil || len(rest) != 1 {
				return fmt.Errorf("usage: transfer <kind> <id> <new-owner>")
			}
			d, err := f.Dial()
			if err != nil {
				return err
			}
			_, err = d.Resources.Transfer(cmd.Context(), connect.NewRequest(&managementv1.TransferRequest{
				Ref:         ref,
				NewOwner:    rest[0],
				KeepSeconds: int64(keep / time.Second),
			}))
			if err != nil {
				return err
			}
			fmt.Fprintf(cmdutil.Out, "%s -> %s\n", ref, rest[0])
			return nil
		},
	}
	cmd.Flags().DurationVar(&keep, "keep", 0, "TTL under the new owner (stands only); 0 keeps until deleted")
	return cmd
}

// NewInvoke builds `invoke`.
func NewInvoke(f *cmdutil.Factory) *cobra.Command {
	var data, dataFile string
	cmd := &cobra.Command{
		Use:   "invoke <kind> <id> <command>",
		Short: "Send one of the record's own commands",
		Long: `Send a command to a record. Which commands a kind has, and what each
one takes, is the INSTALLATION's answer — ` + "`graphenectl kinds`" + ` lists them,
completion offers them, and on a terminal a command with no --data asks
for its fields.`,
		Args:              cobra.RangeArgs(2, 3),
		ValidArgsFunction: commandCompletion(f),
		RunE: func(cmd *cobra.Command, args []string) error {
			ref, rest, err := cmdutil.TargetRef(args)
			if err != nil || len(rest) != 1 {
				return fmt.Errorf("usage: invoke <kind> <id> <command>")
			}
			payload, err := cmdutil.JSONInput("data", data, dataFile)
			if err != nil {
				return err
			}
			// No payload given, a human on the other end: walk the
			// command's own schema. The client knows no fields — it
			// asks the registry what this command takes.
			if len(payload) == 0 && cmdutil.StdinIsTerminal() {
				kind, _, _ := strings.Cut(ref, "/")
				entry, err := f.KindEntryOf(cmd.Context(), kind)
				if err != nil {
					return err
				}
				for _, c := range entry.Commands {
					if c.Name != rest[0] {
						continue
					}
					if schema := cmdutil.ParseSchema(c.PayloadSchema); schema != nil {
						if payload, err = cmdutil.PromptSchema(os.Stdin, rest[0], schema); err != nil {
							return err
						}
					}
					break
				}
			}
			d, err := f.Dial()
			if err != nil {
				return err
			}
			resp, err := d.Resources.Invoke(cmd.Context(), connect.NewRequest(&managementv1.InvokeRequest{
				Ref:     ref,
				Command: rest[0],
				Payload: payload,
			}))
			if err != nil {
				return err
			}
			if done, err := f.Emit(resp.Msg); done || err != nil {
				return err
			}
			fmt.Fprintln(cmdutil.Out, string(resp.Msg.GetResult()))
			return nil
		},
	}
	cmd.Flags().StringVar(&data, "data", "", "command payload as inline JSON")
	cmd.Flags().StringVar(&dataFile, "data-file", "", "command payload from a JSON/YAML file (- for stdin)")
	return cmd
}
