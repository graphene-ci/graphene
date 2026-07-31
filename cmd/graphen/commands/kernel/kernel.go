package kernel

import "github.com/spf13/cobra"

var Cmd = newCommand()

func newCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "kernel",
		Short: "Manage the Graphene kernel",
	}

	command.AddCommand(newRunCommand())

	return command
}
