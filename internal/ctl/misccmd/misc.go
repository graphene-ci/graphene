// Package misccmd carries the installation nouns: pipeline, secret, ns.
package misccmd

import (
	"fmt"
	"io"
	"os"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	"github.com/graphene-ci/graphene/internal/ctl/runcmd"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// NewPipeline builds the `pipeline` tree.
func NewPipeline(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pipeline",
		Short: "The pipeline record: show",
	}
	show := &cobra.Command{
		Use:   "show <pipeline-id>",
		Short: "The current worker image, the manifest and its digest",
		Args:  cobra.ExactArgs(1),
		ValidArgsFunction: func(cmd *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return f.LiveIds("pipeline"), cobra.ShellCompDirectiveNoFileComp
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cc, err := f.Resolve()
			if err != nil {
				return err
			}
			resp, err := runcmd.GetPipelineRecord(cmd.Context(), cc, args[0])
			if err != nil {
				return err
			}
			if done, err := f.Emit(resp); done || err != nil {
				return err
			}
			fmt.Fprintf(cmdutil.Out, "pipeline %s\nimage    %s\ndigest   %s\n", args[0], resp.GetImage(), resp.GetDigest())
			if len(resp.GetManifest()) > 0 {
				cmdutil.PrintJSONBlock("manifest", resp.GetManifest())
			}
			return nil
		},
	}
	cmd.AddCommand(show)
	return cmd
}

// NewSecret builds the `secret` tree.
func NewSecret(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "secret",
		Short: "Secrets: set a value (the rest is `get secret` / `delete secret`)",
	}
	var value, valueFile string
	set := &cobra.Command{
		Use:   "set <name>",
		Short: "Set a secret value (no flags: the value comes from stdin)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var v string
			switch {
			case value != "" && valueFile != "":
				return fmt.Errorf("--value and --value-file are mutually exclusive")
			case valueFile != "":
				// Raw bytes on purpose: a secret is never YAML-converted.
				raw, err := cmdutil.ReadFile(valueFile)
				if err != nil {
					return err
				}
				v = string(raw)
			case value != "":
				v = value
			default:
				raw, err := io.ReadAll(os.Stdin)
				if err != nil {
					return err
				}
				v = strings.TrimRight(string(raw), "\n")
			}
			if v == "" {
				return fmt.Errorf("empty secret value")
			}
			d, err := f.Dial()
			if err != nil {
				return err
			}
			resp, err := d.Secrets.SetSecret(cmd.Context(), connect.NewRequest(&managementv1.SetSecretRequest{
				Name: args[0], Value: v,
			}))
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "secret %s set (version %d)\n", args[0], resp.Msg.GetVersion())
			return nil
		},
	}
	set.Flags().StringVar(&value, "value", "", "secret value inline")
	set.Flags().StringVar(&valueFile, "value-file", "", "secret value from a file, raw bytes")

	cmd.AddCommand(set)
	return cmd
}

// NewNs builds the `ns` tree.
func NewNs(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ns",
		Short: "Namespaces: list, create",
	}
	list := &cobra.Command{
		Use:   "list",
		Short: "List the namespaces",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			resp, err := d.Ns.ListNamespaces(cmd.Context(), connect.NewRequest(&managementv1.ListNamespacesRequest{}))
			if err != nil {
				return err
			}
			if done, err := f.Emit(resp.Msg); done || err != nil {
				return err
			}
			for _, name := range resp.Msg.GetNames() {
				fmt.Fprintln(cmdutil.Out, name)
			}
			return nil
		},
	}
	var retention int
	create := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a namespace",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			if _, err := d.Ns.CreateNamespace(cmd.Context(), connect.NewRequest(&managementv1.CreateNamespaceRequest{
				Name: args[0], RetentionDays: int32(retention), //nolint:gosec // a small flag value
			})); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "namespace %s created\n", args[0])
			return nil
		},
	}
	create.Flags().IntVar(&retention, "retention-days", 0, "workflow retention (0 — the server default)")
	cmd.AddCommand(list, create)
	return cmd
}
