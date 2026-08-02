package kernel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/app/install"
	"github.com/graphene-ci/graphene/internal/utils/cmdflags"
)

var (
	errScopeUnknown = errors.New("--scope must be system or user")
	errNeedRoot     = errors.New("system scope needs root (run with sudo, or use --scope user)")
)

// InstallFlags configures the installation.
type InstallFlags struct {
	Scope   string
	Tenant  string
	Name    string
	TCP     string
	Force   bool
	NoStart bool
	Print   bool
}

// Validate checks the flag values.
func (flags *InstallFlags) Validate() error {
	if flags == nil {
		return errFlagsRequired
	}

	if flags.Scope != string(install.ScopeSystem) && flags.Scope != string(install.ScopeUser) {
		return errScopeUnknown
	}

	if flags.Scope == string(install.ScopeSystem) && os.Geteuid() != 0 && !flags.Print {
		return errNeedRoot
	}

	return nil
}

// completions renders the shell completion scripts of the whole command
// tree. Only this layer can: the scripts describe every command, and the
// tree is what the binary was built with.
func completions(command *cobra.Command) map[install.Shell][]byte {
	root := command.Root()
	out := map[install.Shell][]byte{}

	// With descriptions: readline lays candidates out in as many columns
	// as the terminal fits, so bare names get packed several per line.
	// A description makes each candidate too wide to share a line, which
	// is what produces the one-per-line list — and it says what the
	// candidate means.
	renderers := map[install.Shell]func(io.Writer) error{
		install.ShellBash: func(w io.Writer) error { return root.GenBashCompletionV2(w, true) },
		install.ShellZsh:  func(w io.Writer) error { return root.GenZshCompletion(w) },
		install.ShellFish: func(w io.Writer) error { return root.GenFishCompletion(w, true) },
	}

	for shell, render := range renderers {
		var buf bytes.Buffer
		if err := render(&buf); err != nil {
			continue // a shell we cannot render for is simply skipped
		}

		out[shell] = buf.Bytes()
	}

	return out
}

// Install puts the kernel under systemd.
func Install(ctx context.Context, out io.Writer, flags *InstallFlags, command *cobra.Command) error {
	if err := flags.Validate(); err != nil {
		return err
	}

	scope := install.Scope(flags.Scope)

	layout, err := install.NewLayout(scope)
	if err != nil {
		return fmt.Errorf("kernel install: %w", err)
	}

	if flags.Print {
		return printPlan(out, &layout, flags)
	}

	result, err := install.Install(ctx, &install.Options{
		Scope:       scope,
		Tenant:      flags.Tenant,
		Name:        flags.Name,
		TCP:         flags.TCP,
		Force:       flags.Force,
		SkipEnable:  flags.NoStart,
		Completions: completions(command),
	})
	if err != nil && !errors.Is(err, install.ErrNoSystemd) {
		return fmt.Errorf("kernel install: %w", err)
	}

	report(out, &result)

	if errors.Is(err, install.ErrNoSystemd) {
		_, _ = fmt.Fprintf(out, "\nsystemd is not available here: the files are in place, "+
			"start the kernel yourself with\n  %s kernel run --config %s\n",
			result.Layout.Binary, result.Layout.Config)
	}

	return nil
}

func printPlan(out io.Writer, layout *install.Layout, flags *InstallFlags) error {
	unit, err := install.RenderUnit(layout)
	if err != nil {
		return fmt.Errorf("kernel install: %w", err)
	}

	config, err := install.RenderConfig(layout, install.ConfigOptions{
		Tenant: flags.Tenant,
		Name:   flags.Name,
		TCP:    flags.TCP,
	})
	if err != nil {
		return fmt.Errorf("kernel install: %w", err)
	}

	_, err = fmt.Fprintf(out, "# %s\n%s\n# %s\n%s", layout.Unit, unit, layout.Config, config)
	if err != nil {
		return fmt.Errorf("kernel install: write: %w", err)
	}

	return nil
}

func report(out io.Writer, result *install.Result) {
	layout := result.Layout

	_, _ = fmt.Fprintf(out, "installed (%s scope)\n", layout.Scope)
	_, _ = fmt.Fprintf(out, "  binary %s\n  config %s\n  data   %s\n  unit   %s\n  socket %s\n",
		layout.Binary, layout.Config, layout.Data, layout.Unit, layout.Socket)

	if result.Started {
		_, _ = fmt.Fprintf(out, "\nservice %s is enabled and running\n", install.UnitName)
	}

	if len(result.Completions) > 0 {
		shells := make([]string, 0, len(result.Completions))
		for _, shell := range result.Completions {
			shells = append(shells, string(shell))
		}

		sort.Strings(shells)

		_, _ = fmt.Fprintf(out, "\nshell completion installed for %s (open a new shell to use it)\n",
			strings.Join(shells, ", "))
	}

	if result.Token != "" {
		_, _ = fmt.Fprintf(out, "\nbootstrap token (shown once, also in %s):\n  %s\n",
			layout.TokenFile, result.Token)
	}

	_, _ = fmt.Fprintf(out, "\ntalk to it:\n  export GRAPHEN_TOKEN=$(cat %s)\n  %s ctl definitions --socket %s\n",
		layout.TokenFile, layout.Binary, layout.Socket)
}

func newInstallCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "install",
		Short: "Install the kernel as a systemd service",
		Long: "Install the kernel as a systemd service.\n\n" +
			"The system scope owns the machine (/etc, /var/lib, /usr/local/bin) and needs root;\n" +
			"the user scope installs into the XDG directories and needs no privileges at all.",
		Example: "  graphen kernel install --scope user\n" +
			"  sudo graphen kernel install --scope system --tcp 0.0.0.0:9000\n" +
			"  graphen kernel install --print",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			flags, err := installFlags(command)
			if err != nil {
				return err
			}

			return Install(command.Context(), command.OutOrStdout(), flags, command)
		},
	}

	command.Flags().String("scope", string(install.ScopeUser), "install scope: system or user")
	command.Flags().String("tenant", "default", "tenant this kernel belongs to")
	command.Flags().String("name", "local", "name of this kernel")
	command.Flags().String("tcp", "", "also serve this address over TLS, e.g. 0.0.0.0:9000")
	command.Flags().Bool("force", false, "overwrite an existing configuration and token")
	command.Flags().Bool("no-start", false, "install the files without enabling the service")
	command.Flags().Bool("print", false, "print the unit and configuration instead of installing")

	cmdflags.RegisterCompletion(command, "scope", completeScope)

	return command
}

func installFlags(command *cobra.Command) (*InstallFlags, error) {
	values, err := cmdflags.Strings(command, "scope", "tenant", "name", "tcp")
	if err != nil {
		return nil, err
	}

	force, err := cmdflags.Bool(command, "force")
	if err != nil {
		return nil, err
	}

	noStart, err := cmdflags.Bool(command, "no-start")
	if err != nil {
		return nil, err
	}

	printOnly, err := cmdflags.Bool(command, "print")
	if err != nil {
		return nil, err
	}

	return &InstallFlags{
		Scope: values[0], Tenant: values[1], Name: values[2], TCP: values[3],
		Force: force, NoStart: noStart, Print: printOnly,
	}, nil
}

// UninstallFlags selects the scope to remove.
type UninstallFlags struct {
	Scope string
}

// Validate checks the flag values.
func (flags *UninstallFlags) Validate() error {
	if flags == nil {
		return errFlagsRequired
	}

	if flags.Scope != string(install.ScopeSystem) && flags.Scope != string(install.ScopeUser) {
		return errScopeUnknown
	}

	if flags.Scope == string(install.ScopeSystem) && os.Geteuid() != 0 {
		return errNeedRoot
	}

	return nil
}

// Uninstall removes the service, keeping the data.
func Uninstall(ctx context.Context, out io.Writer, flags *UninstallFlags) error {
	if err := flags.Validate(); err != nil {
		return err
	}

	layout, err := install.Uninstall(ctx, install.Scope(flags.Scope))
	if err != nil {
		return fmt.Errorf("kernel uninstall: %w", err)
	}

	_, _ = fmt.Fprintf(out, "removed the %s service\n", flags.Scope)
	_, _ = fmt.Fprintf(out, "kept:\n  data   %s\n  config %s\n", layout.Data, layout.Config)

	return nil
}

func newUninstallCommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "uninstall",
		Short:   "Remove the systemd service (data and configuration are kept)",
		Example: "  graphen kernel uninstall --scope user",
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			scope, err := cmdflags.String(command, "scope")
			if err != nil {
				return err
			}

			return Uninstall(command.Context(), command.OutOrStdout(), &UninstallFlags{Scope: scope})
		},
	}

	command.Flags().String("scope", string(install.ScopeUser), "install scope: system or user")

	cmdflags.RegisterCompletion(command, "scope", completeScope)

	return command
}

// Status prints what systemd says about the service.
func Status(ctx context.Context, out io.Writer, scope string) error {
	text, err := install.Status(ctx, install.Scope(scope))
	if err != nil {
		return fmt.Errorf("kernel status: %w", err)
	}

	if _, err := io.WriteString(out, text); err != nil {
		return fmt.Errorf("kernel status: write: %w", err)
	}

	return nil
}

func newStatusCommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "status",
		Short:   "Show the service status",
		Example: "  graphen kernel status --scope user",
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			scope, err := cmdflags.String(command, "scope")
			if err != nil {
				return err
			}

			return Status(command.Context(), command.OutOrStdout(), scope)
		},
	}

	command.Flags().String("scope", string(install.ScopeUser), "install scope: system or user")

	cmdflags.RegisterCompletion(command, "scope", completeScope)

	return command
}

func completeScope(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	return []string{
		string(install.ScopeUser) + "\tXDG directories, no privileges",
		string(install.ScopeSystem) + "\tFHS directories, needs root",
	}, cobra.ShellCompDirectiveNoFileComp
}
