package block

import "github.com/spf13/cobra"

// BuildFlags configures Build.
type BuildFlags = ConfigFlags

// Build builds a Graphene block.
func Build(flags *BuildFlags) error {
	return flags.Validate()
}

func newBuildCommand() *cobra.Command {
	return newConfigCommand(
		"build",
		"Build a Graphene block",
		"  graphene block build --config ./.graphene-block.yaml",
		Build,
	)
}
