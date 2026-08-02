package block

import "github.com/spf13/cobra"

// The cobra command tree is assembled from package-level commands.
//
//nolint:gochecknoglobals // see above
var Cmd = newCommand()

func newCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "block",
		Short: "Manage Graphene blocks",
	}

	command.AddCommand(
		newInitCommand(),
		newGenCommand(),
		newBuildCommand(),
	)

	return command
}
