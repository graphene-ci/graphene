package ci

import "github.com/spf13/cobra"

// PlanFlags configures Plan.
type PlanFlags = ConfigFlags

// Plan previews a Graphene CI run.
func Plan(flags *PlanFlags) error {
	return flags.Validate()
}

func newPlanCommand() *cobra.Command {
	return newConfigCommand(
		"plan",
		"Plan a Graphene CI run",
		"  graphen ci plan --config ./.graphen-ci/.graphen-ci.yaml",
		Plan,
	)
}
