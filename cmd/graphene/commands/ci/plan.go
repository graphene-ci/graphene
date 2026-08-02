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
		"  graphene ci plan --config ./.graphene-ci/.graphene-ci.yaml",
		Plan,
	)
}
