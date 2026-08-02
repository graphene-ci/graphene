package ctl

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/app/clientconfig"
	"github.com/graphene-ci/graphene/internal/utils/cmdflags"
)

// Contexts are how one client reaches several kernels: each pairs a
// kernel with an identity, and one of them is current. `kernel install`
// writes one; these commands are for everything else.

// ListContexts prints the known contexts, marking the current one.
func ListContexts(out io.Writer, configPath string) error {
	cfg, path, err := clientconfig.Load(configPath)
	if err != nil {
		return fmt.Errorf("ctl context: %w", err)
	}

	names := cfg.Names()
	if len(names) == 0 {
		_, err := fmt.Fprintf(out, "no contexts in %s\n", path)

		return writeErr(err)
	}

	for _, name := range names {
		marker := " "
		if name == cfg.CurrentContext {
			marker = "*"
		}

		resolved, err := cfg.Resolve(name)
		if err != nil {
			_, werr := fmt.Fprintf(out, "%s %s\t(broken: %v)\n", marker, name, err)

			return writeErr(werr)
		}

		endpoint := resolved.Kernel.Address
		if endpoint == "" {
			endpoint = resolved.Kernel.Socket
		}

		if _, err := fmt.Fprintf(out, "%s %s\t%s\t%s\n",
			marker, name, endpoint, resolved.Context.Tenant); err != nil {
			return writeErr(err)
		}
	}

	return nil
}

// UseContext selects a context.
func UseContext(out io.Writer, configPath, name string) error {
	cfg, path, err := clientconfig.Load(configPath)
	if err != nil {
		return fmt.Errorf("ctl context: %w", err)
	}

	if err := cfg.Use(name); err != nil {
		return fmt.Errorf("ctl context: %w", err)
	}

	if err := clientconfig.Save(cfg, path); err != nil {
		return fmt.Errorf("ctl context: %w", err)
	}

	_, err = fmt.Fprintf(out, "using %s\n", name)

	return writeErr(err)
}

// RemoveContext drops a context.
func RemoveContext(out io.Writer, configPath, name string) error {
	cfg, path, err := clientconfig.Load(configPath)
	if err != nil {
		return fmt.Errorf("ctl context: %w", err)
	}

	cfg.Remove(name)

	if err := clientconfig.Save(cfg, path); err != nil {
		return fmt.Errorf("ctl context: %w", err)
	}

	_, err = fmt.Fprintf(out, "removed %s\n", name)

	return writeErr(err)
}

func writeErr(err error) error {
	if err != nil {
		return fmt.Errorf("ctl context: write: %w", err)
	}

	return nil
}

func newContextCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "context",
		Short: "Manage the kernels this client knows",
	}

	command.AddCommand(
		&cobra.Command{
			Use:     "list",
			Short:   "List contexts (the current one is marked)",
			Args:    cobra.NoArgs,
			RunE:    func(cmd *cobra.Command, _ []string) error { return runContext(cmd, ListContexts) },
			Example: "  graphen ctl context list",
		},
		&cobra.Command{
			Use:               "use <name>",
			Short:             "Select a context",
			Args:              cobra.ExactArgs(1),
			ValidArgsFunction: completeContext,
			Example:           "  graphen ctl context use prod",
			RunE: func(cmd *cobra.Command, args []string) error {
				config, err := cmdflags.String(cmd, "config")
				if err != nil {
					return err
				}

				return UseContext(cmd.OutOrStdout(), config, args[0])
			},
		},
		&cobra.Command{
			Use:               "remove <name>",
			Short:             "Forget a context",
			Args:              cobra.ExactArgs(1),
			ValidArgsFunction: completeContext,
			Example:           "  graphen ctl context remove prod",
			RunE: func(cmd *cobra.Command, args []string) error {
				config, err := cmdflags.String(cmd, "config")
				if err != nil {
					return err
				}

				return RemoveContext(cmd.OutOrStdout(), config, args[0])
			},
		},
	)

	return command
}

func runContext(command *cobra.Command, body func(io.Writer, string) error) error {
	config, err := cmdflags.String(command, "config")
	if err != nil {
		return err
	}

	return body(command.OutOrStdout(), config)
}

func completeContext(command *cobra.Command, _ []string, prefix string) ([]string, cobra.ShellCompDirective) {
	config, err := cmdflags.String(command, "config")
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	cfg, _, err := clientconfig.Load(config)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	var out []string

	for _, name := range cfg.Names() {
		if strings.HasPrefix(name, prefix) {
			out = append(out, name)
		}
	}

	return out, cobra.ShellCompDirectiveNoFileComp
}
