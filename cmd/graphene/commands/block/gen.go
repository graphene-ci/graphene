package block

import "github.com/spf13/cobra"

// GenFlags configures Gen.
type GenFlags = ConfigFlags

// Gen generates Graphene block artifacts.
func Gen(flags *GenFlags) error {
	return flags.Validate()
}

func newGenCommand() *cobra.Command {
	return newConfigCommand(
		"gen",
		"Generate Graphene block artifacts",
		"  graphene block gen --config ./.graphene-block.yaml",
		Gen,
	)
}
