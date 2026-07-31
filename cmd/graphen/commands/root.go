package commands

import (
	"os"

	"github.com/graphene-ci/graphene/cmd/graphen/commands/block"
	"github.com/graphene-ci/graphene/cmd/graphen/commands/ci"
	"github.com/graphene-ci/graphene/cmd/graphen/commands/kernel"
	"github.com/spf13/cobra"
)

var rootCmd = newRootCommand()

func newRootCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "graphen",
		Short: "Graphene CI command-line interface",
	}

	command.AddCommand(
		kernel.Cmd,
		ci.Cmd,
		block.Cmd,
	)

	return command
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func Root() *cobra.Command {
	return rootCmd
}
