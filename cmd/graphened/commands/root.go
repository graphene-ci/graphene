// Package commands is graphened's surface: everything about running a
// kernel on this machine.
//
// It is deliberately small and deliberately LOCAL. A kernel is a kernel —
// there are no roles and no modes — and this binary is the whole of one.
// What it is not is a client: talking to a kernel somewhere else is
// gctl's job, and the two are separate binaries because they are separate
// programs. This one holds a file, listens on a port, runs for years and
// starts as a service; that one runs for a second from somebody's laptop
// and has no business touching the file.
package commands

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/app/config"
)

// version is what this build calls itself, stamped at link time.
var version = "dev"

// The one thing that cannot come from the configuration: where the
// configuration is.
//
//nolint:gochecknoglobals // a validated value cannot be a const; treated as one
var configPath string

// Root is graphened.
func Root() *cobra.Command {
	root := &cobra.Command{
		Use:           "graphened",
		Short:         "A graphene kernel",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version,
	}

	root.PersistentFlags().StringVar(&configPath, "config", config.DefaultPath(),
		"the file the kernel is configured by")

	root.AddCommand(runCommand())
	root.AddCommand(configureCommand())
	root.AddCommand(serviceCommands()...)

	return root
}

// Execute runs graphened and reports whether it failed.
func Execute() int {
	if err := Root().Execute(); err != nil {
		_, _ = os.Stderr.WriteString("graphened: " + err.Error() + "\n")

		return 1
	}

	return 0
}
