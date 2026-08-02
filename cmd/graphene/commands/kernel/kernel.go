package kernel

import "github.com/spf13/cobra"

// The cobra command tree is assembled from package-level commands.
//
//nolint:gochecknoglobals // see above
var Cmd = newCommand()

func newCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "kernel",
		Short: "Manage the Graphene kernel",
	}

	command.AddCommand(
		newRunCommand(),
		newCACommand(),
		newInstallCommand(),
		newUninstallCommand(),
		newStatusCommand(),
	)

	return command
}
