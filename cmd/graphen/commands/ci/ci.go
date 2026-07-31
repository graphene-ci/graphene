package ci

import "github.com/spf13/cobra"

var Cmd = newCommand()

func newCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "ci",
		Short: "Manage Graphene CI projects",
	}

	command.AddCommand(
		newInitCommand(),
		newBuildCommand(),
		newPlanCommand(),
		newRunCommand(),
	)

	return command
}
