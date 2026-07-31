package ci

import "github.com/spf13/cobra"

// BuildFlags configures Build.
type BuildFlags = ConfigFlags

// Build builds a Graphene CI project.
func Build(flags *BuildFlags) error {
	return flags.Validate()
}

func newBuildCommand() *cobra.Command {
	return newConfigCommand(
		"build",
		"Build a Graphene CI project",
		"  graphen ci build --config ./.graphen-ci/.graphen-ci.yaml",
		Build,
	)
}
