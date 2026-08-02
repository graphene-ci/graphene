package ctl

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	appctl "github.com/graphene-ci/graphene/internal/app/ctl"
	"github.com/graphene-ci/graphene/internal/utils/cmdflags"
)

// Resources are addressed positionally: `get Kernel acme/k1` names one,
// `get Kernel acme` names the subtree under that prefix. The kind's
// definition says how many segments a full path takes, so the two cases
// need no flag to tell them apart.

// GetFlags selects what to read.
type GetFlags struct {
	Target   *TargetFlags
	Address  appctl.Address
	Selector []string
	Format   string
}

// Get reads resources and prints them.
func Get(ctx context.Context, out io.Writer, flags *GetFlags) error {
	if flags == nil {
		return errFlagsRequired
	}

	if flags.Address.Kind == "" {
		return errKindRequired
	}

	format, err := appctl.ParseFormat(flags.Format)
	if err != nil {
		return fmt.Errorf("output: %w", err)
	}

	client, err := connect(flags.Target)
	if err != nil {
		return err
	}

	defer func() { _ = client.Close() }()

	resources, err := readAddress(ctx, client, flags.Address, flags.Selector)
	if err != nil {
		return err
	}

	if err := appctl.Write(out, format, resources); err != nil {
		return fmt.Errorf("render: %w", err)
	}

	return nil
}

// readAddress fetches one resource for a full path, or the subtree under a
// shorter one.
func readAddress(
	ctx context.Context,
	client *appctl.Client,
	addr appctl.Address,
	selector []string,
) ([]*graphenepbv1.Resource, error) {
	exact, err := client.Exact(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", addr, err)
	}

	if exact {
		res, err := client.Get(ctx, addr.Kind, addr.Path)
		if err != nil {
			return nil, fmt.Errorf("get: %w", err)
		}

		return []*graphenepbv1.Resource{res}, nil
	}

	match, err := appctl.ParseSelector(selector)
	if err != nil {
		return nil, fmt.Errorf("selector: %w", err)
	}

	resources, err := client.List(ctx, addr.Kind, addr.Path, match)
	if err != nil {
		return nil, fmt.Errorf("list: %w", err)
	}

	return resources, nil
}

func newGetCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "get <kind> [path]",
		Short: "Read resources",
		Example: "  graphen ctl get Kernel acme\n" +
			"  graphen ctl get Kernel acme/k1\n" +
			"  graphen ctl get Execution acme/prod --selector spec.placement=k1 -o name",
		Args:              cobra.RangeArgs(1, 2), //nolint:mnd // kind, optional path
		ValidArgsFunction: completeAddress,
		RunE: func(command *cobra.Command, args []string) error {
			flags, err := getFlags(command, args)
			if err != nil {
				return err
			}

			return Get(command.Context(), command.OutOrStdout(), flags)
		},
	}

	addSelectorFlag(command)
	addFormatFlag(command)

	return command
}

func getFlags(command *cobra.Command, args []string) (*GetFlags, error) {
	target, err := newTargetFlags(command)
	if err != nil {
		return nil, err
	}

	selector, err := cmdflags.StringSlice(command, "selector")
	if err != nil {
		return nil, err
	}

	format, err := cmdflags.String(command, "output")
	if err != nil {
		return nil, err
	}

	return &GetFlags{Target: target, Address: addressFromArgs(args), Selector: selector, Format: format}, nil
}

func addressFromArgs(args []string) appctl.Address {
	kind, path := "", ""
	if len(args) > 0 {
		kind = args[0]
	}

	if len(args) > 1 {
		path = args[1]
	}

	return appctl.ParseAddress(kind, path)
}

// ApplyFlags is the input of apply: a file, or "-" for standard input.
type ApplyFlags struct {
	Target *TargetFlags
	File   string
}

// Apply writes the resources of a YAML stream.
func Apply(ctx context.Context, in io.Reader, out io.Writer, flags *ApplyFlags) error {
	if flags == nil {
		return errFlagsRequired
	}

	raw, err := readInput(in, flags.File)
	if err != nil {
		return err
	}

	client, err := connect(flags.Target)
	if err != nil {
		return err
	}

	defer func() { _ = client.Close() }()

	applied, applyErr := client.Apply(ctx, raw)
	for _, key := range applied {
		if _, err := fmt.Fprintf(out, "applied %s %s\n",
			key.GetKind(), strings.Join(key.GetPath(), "/")); err != nil {
			return fmt.Errorf("write: %w", err)
		}
	}

	if applyErr != nil {
		return fmt.Errorf("apply: %w", applyErr)
	}

	return nil
}

func readInput(in io.Reader, file string) ([]byte, error) {
	if file == "" || file == "-" {
		raw, err := io.ReadAll(in)
		if err != nil {
			return nil, fmt.Errorf("read stdin: %w", err)
		}

		return raw, nil
	}

	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", file, err)
	}

	return raw, nil
}

func newApplyCommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "apply",
		Short:   "Write resources from YAML",
		Example: "  graphen ctl apply -f role.yaml\n  graphen ctl get Role acme/admin | graphen ctl apply",
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			target, err := newTargetFlags(command)
			if err != nil {
				return err
			}

			file, err := cmdflags.String(command, "file")
			if err != nil {
				return err
			}

			return Apply(command.Context(), command.InOrStdin(), command.OutOrStdout(),
				&ApplyFlags{Target: target, File: file})
		},
	}

	command.Flags().StringP("file", "f", "-", "YAML file with resources, or - for stdin")

	cmdflags.RegisterCompletion(command, "file", completeYAMLFile)

	return command
}

// DeleteFlags identifies what to remove.
type DeleteFlags struct {
	Target   *TargetFlags
	Address  appctl.Address
	Revision uint64
}

// Delete removes one resource.
func Delete(ctx context.Context, out io.Writer, flags *DeleteFlags) error {
	if flags == nil {
		return errFlagsRequired
	}

	if flags.Address.Kind == "" {
		return errKindRequired
	}

	client, err := connect(flags.Target)
	if err != nil {
		return err
	}

	defer func() { _ = client.Close() }()

	if err := client.Delete(ctx, flags.Address.Kind, flags.Address.Path, flags.Revision); err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	if _, err := fmt.Fprintf(out, "deleted %s\n", flags.Address); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	return nil
}

func newDeleteCommand() *cobra.Command {
	command := &cobra.Command{
		Use:               "delete <kind> <path>",
		Short:             "Remove a resource",
		Example:           "  graphen ctl delete Role acme/kernel-default",
		Args:              cobra.ExactArgs(2), //nolint:mnd // kind and path
		ValidArgsFunction: completeAddress,
		RunE: func(command *cobra.Command, args []string) error {
			target, err := newTargetFlags(command)
			if err != nil {
				return err
			}

			revision, err := cmdflags.Uint64(command, "revision")
			if err != nil {
				return err
			}

			return Delete(command.Context(), command.OutOrStdout(),
				&DeleteFlags{Target: target, Address: addressFromArgs(args), Revision: revision})
		},
	}

	command.Flags().Uint64("revision", 0, "expected revision (0 reads the current one first)")

	return command
}

// WatchFlags selects the stream to follow.
type WatchFlags struct {
	Target   *TargetFlags
	Address  appctl.Address
	Selector []string
}

// Watch follows a kind until interrupted.
func Watch(ctx context.Context, out io.Writer, flags *WatchFlags) error {
	if flags == nil {
		return errFlagsRequired
	}

	if flags.Address.Kind == "" {
		return errKindRequired
	}

	selector, err := appctl.ParseSelector(flags.Selector)
	if err != nil {
		return fmt.Errorf("selector: %w", err)
	}

	client, err := connect(flags.Target)
	if err != nil {
		return err
	}

	defer func() { _ = client.Close() }()

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := client.Watch(ctx, flags.Address.Kind, flags.Address.Path, selector,
		func(event *graphenepbv1.WatchEvent) error {
			return appctl.WriteEvent(out, event)
		}); err != nil {
		return fmt.Errorf("watch %s: %w", flags.Address, err)
	}

	return nil
}

func newWatchCommand() *cobra.Command {
	command := &cobra.Command{
		Use:               "watch <kind> [path]",
		Short:             "Follow changes of a kind",
		Example:           "  graphen ctl watch Kernel acme\n  graphen ctl watch Execution --selector spec.placement=k1",
		Args:              cobra.RangeArgs(1, 2), //nolint:mnd // kind, optional path prefix
		ValidArgsFunction: completeAddress,
		RunE: func(command *cobra.Command, args []string) error {
			target, err := newTargetFlags(command)
			if err != nil {
				return err
			}

			selector, err := cmdflags.StringSlice(command, "selector")
			if err != nil {
				return err
			}

			return Watch(command.Context(), command.OutOrStdout(),
				&WatchFlags{Target: target, Address: addressFromArgs(args), Selector: selector})
		},
	}

	addSelectorFlag(command)

	return command
}

// DefinitionsFlags lists the kinds a kernel knows.
type DefinitionsFlags struct {
	Target *TargetFlags
}

// Definitions prints the kind table.
func Definitions(ctx context.Context, out io.Writer, flags *DefinitionsFlags) error {
	if flags == nil {
		return errFlagsRequired
	}

	client, err := connect(flags.Target)
	if err != nil {
		return err
	}

	defer func() { _ = client.Close() }()

	defs, err := client.Definitions(ctx)
	if err != nil {
		return fmt.Errorf("definitions: %w", err)
	}

	if err := appctl.WriteDefinitions(out, defs); err != nil {
		return fmt.Errorf("render: %w", err)
	}

	return nil
}

func newDefinitionsCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "definitions",
		Short:   "List the kinds this kernel knows",
		Example: "  graphen ctl definitions",
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			target, err := newTargetFlags(command)
			if err != nil {
				return err
			}

			return Definitions(command.Context(), command.OutOrStdout(), &DefinitionsFlags{Target: target})
		},
	}
}

func addSelectorFlag(command *cobra.Command) {
	command.Flags().StringSlice("selector", nil, "field match, e.g. spec.placement=k1")
	cmdflags.RegisterCompletion(command, "selector", completeSelector)
}

func addFormatFlag(command *cobra.Command) {
	command.Flags().StringP("output", "o", string(appctl.FormatYAML), "output format: yaml, json or name")
	cmdflags.RegisterCompletion(command, "output", completeFormat)
}
