package block

import "github.com/spf13/cobra"

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
