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
)

// GetFlags selects what to read: a full path reads one resource, a shorter
// prefix lists a subtree.
type GetFlags struct {
	Target   *TargetFlags
	Kind     string
	Path     []string
	Selector []string
	Exact    bool
}

// Get reads resources and prints them as YAML.
func Get(ctx context.Context, out io.Writer, flags *GetFlags) error {
	if flags == nil {
		return errFlagsRequired
	}

	if flags.Kind == "" {
		return errKindRequired
	}

	client, err := connect(flags.Target)
	if err != nil {
		return err
	}

	defer func() { _ = client.Close() }()

	if flags.Exact {
		res, err := client.Get(ctx, flags.Kind, flags.Path)
		if err != nil {
			return err //nolint:wrapcheck // the client already names the operation
		}

		return appctl.WriteResources(out, []*graphenepbv1.Resource{res}) //nolint:wrapcheck // same
	}

	selector, err := appctl.ParseSelector(flags.Selector)
	if err != nil {
		return err //nolint:wrapcheck // same
	}

	resources, err := client.List(ctx, flags.Kind, flags.Path, selector)
	if err != nil {
		return err //nolint:wrapcheck // same
	}

	return appctl.WriteResources(out, resources) //nolint:wrapcheck // same
}

func newGetCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "get",
		Short: "Read resources",
		Example: "  graphen ctl get --kind Kernel --path acme\n" +
			"  graphen ctl get --kind Kernel --path acme,k1 --exact",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			flags, err := getFlags(command)
			if err != nil {
				return err
			}

			return Get(command.Context(), command.OutOrStdout(), flags)
		},
	}

	command.Flags().String("kind", "", "resource kind")
	command.Flags().StringSlice("path", nil, "path segments (a prefix lists, a full path with --exact reads)")
	command.Flags().StringSlice("selector", nil, "field match, e.g. spec.placement=k1")
	command.Flags().Bool("exact", false, "read exactly this path instead of listing under it")

	return command
}

func getFlags(command *cobra.Command) (*GetFlags, error) {
	target, err := newTargetFlags(command)
	if err != nil {
		return nil, err
	}

	kind, err := command.Flags().GetString("kind")
	if err != nil {
		return nil, fmt.Errorf("read --kind: %w", err)
	}

	path, err := command.Flags().GetStringSlice("path")
	if err != nil {
		return nil, fmt.Errorf("read --path: %w", err)
	}

	selector, err := command.Flags().GetStringSlice("selector")
	if err != nil {
		return nil, fmt.Errorf("read --selector: %w", err)
	}

	exact, err := command.Flags().GetBool("exact")
	if err != nil {
		return nil, fmt.Errorf("read --exact: %w", err)
	}

	return &GetFlags{Target: target, Kind: kind, Path: path, Selector: selector, Exact: exact}, nil
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

	applied, err := client.Apply(ctx, raw)
	for _, key := range applied {
		if _, werr := fmt.Fprintf(out, "applied %s\n", keyText(key)); werr != nil {
			return fmt.Errorf("write: %w", werr)
		}
	}

	if err != nil {
		return err //nolint:wrapcheck // the client already names the operation
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
		Example: "  graphen ctl apply -f role.yaml\n  cat role.yaml | graphen ctl apply",
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			target, err := newTargetFlags(command)
			if err != nil {
				return err
			}

			file, err := command.Flags().GetString("file")
			if err != nil {
				return fmt.Errorf("read --file: %w", err)
			}

			return Apply(command.Context(), command.InOrStdin(), command.OutOrStdout(),
				&ApplyFlags{Target: target, File: file})
		},
	}

	command.Flags().StringP("file", "f", "-", "YAML file with resources, or - for stdin")

	return command
}

// DeleteFlags identifies what to remove.
type DeleteFlags struct {
	Target   *TargetFlags
	Kind     string
	Path     []string
	Revision uint64
}

// Delete removes one resource.
func Delete(ctx context.Context, out io.Writer, flags *DeleteFlags) error {
	if flags == nil {
		return errFlagsRequired
	}

	if flags.Kind == "" {
		return errKindRequired
	}

	client, err := connect(flags.Target)
	if err != nil {
		return err
	}

	defer func() { _ = client.Close() }()

	if err := client.Delete(ctx, flags.Kind, flags.Path, flags.Revision); err != nil {
		return err //nolint:wrapcheck // the client already names the operation
	}

	if _, err := fmt.Fprintf(out, "deleted %s\n",
		keyText(&graphenepbv1.Key{Kind: flags.Kind, Path: flags.Path})); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	return nil
}

func newDeleteCommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "delete",
		Short:   "Remove a resource",
		Example: "  graphen ctl delete --kind Role --path acme,kernel-default",
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			target, err := newTargetFlags(command)
			if err != nil {
				return err
			}

			kind, err := command.Flags().GetString("kind")
			if err != nil {
				return fmt.Errorf("read --kind: %w", err)
			}

			path, err := command.Flags().GetStringSlice("path")
			if err != nil {
				return fmt.Errorf("read --path: %w", err)
			}

			revision, err := command.Flags().GetUint64("revision")
			if err != nil {
				return fmt.Errorf("read --revision: %w", err)
			}

			return Delete(command.Context(), command.OutOrStdout(),
				&DeleteFlags{Target: target, Kind: kind, Path: path, Revision: revision})
		},
	}

	command.Flags().String("kind", "", "resource kind")
	command.Flags().StringSlice("path", nil, "full path segments")
	command.Flags().Uint64("revision", 0, "expected revision (0 reads the current one first)")

	return command
}

// WatchFlags selects the stream to follow.
type WatchFlags struct {
	Target   *TargetFlags
	Kind     string
	Path     []string
	Selector []string
}

// Watch follows a kind until interrupted.
func Watch(ctx context.Context, out io.Writer, flags *WatchFlags) error {
	if flags == nil {
		return errFlagsRequired
	}

	if flags.Kind == "" {
		return errKindRequired
	}

	selector, err := appctl.ParseSelector(flags.Selector)
	if err != nil {
		return err //nolint:wrapcheck // the client already names the operation
	}

	client, err := connect(flags.Target)
	if err != nil {
		return err
	}

	defer func() { _ = client.Close() }()

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := client.Watch(ctx, flags.Kind, flags.Path, selector, func(event *graphenepbv1.WatchEvent) error {
		return appctl.WriteEvent(out, event)
	}); err != nil {
		return fmt.Errorf("watch %s: %w", flags.Kind, err)
	}

	return nil
}

func newWatchCommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "watch",
		Short:   "Follow changes of a kind",
		Example: "  graphen ctl watch --kind Kernel --path acme",
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			flags, err := getFlags(command)
			if err != nil {
				return err
			}

			return Watch(command.Context(), command.OutOrStdout(),
				&WatchFlags{Target: flags.Target, Kind: flags.Kind, Path: flags.Path, Selector: flags.Selector})
		},
	}

	command.Flags().String("kind", "", "resource kind")
	command.Flags().StringSlice("path", nil, "path prefix segments")
	command.Flags().StringSlice("selector", nil, "field match, e.g. spec.placement=k1")
	command.Flags().Bool("exact", false, "unused for watch; accepted for symmetry with get")

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
		return err //nolint:wrapcheck // the client already names the operation
	}

	return appctl.WriteDefinitions(out, defs) //nolint:wrapcheck // same
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

func keyText(key *graphenepbv1.Key) string {
	var out strings.Builder

	out.WriteString(key.GetKind())

	for _, seg := range key.GetPath() {
		out.WriteByte('/')
		out.WriteString(seg)
	}

	return out.String()
}
