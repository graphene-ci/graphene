package ci

import "github.com/spf13/cobra"

// The cobra command tree is assembled from package-level commands.
//
//nolint:gochecknoglobals // see above
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
