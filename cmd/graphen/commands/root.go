package commands

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/cmd/graphen/commands/block"
	"github.com/graphene-ci/graphene/cmd/graphen/commands/ci"
	"github.com/graphene-ci/graphene/cmd/graphen/commands/ctl"
	"github.com/graphene-ci/graphene/cmd/graphen/commands/kernel"
)

// The cobra command tree is assembled from package-level commands.
//
//nolint:gochecknoglobals // see above
var rootCmd = newRootCommand()

func newRootCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "graphen",
		Short: "Graphene CI command-line interface",
	}

	command.AddCommand(
		kernel.Cmd,
		ctl.Cmd,
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
