package ci

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

type PlanFlags struct {
	Config string
}

func newPlanFlags(command *cobra.Command) (*PlanFlags, error) {
	config, err := command.Flags().GetString("config")
	if err != nil {
		return nil, fmt.Errorf("read --config: %w", err)
	}

	return &PlanFlags{Config: config}, nil
}

func (flags *PlanFlags) Validate() error {
	if flags == nil {
		return errors.New("CI plan flags are required")
	}
	if strings.TrimSpace(flags.Config) == "" {
		return errors.New("--config must not be empty")
	}

	return nil
}

func Plan(flags *PlanFlags) error {
	if err := flags.Validate(); err != nil {
		return err
	}

	return nil
}

func newPlanCommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "plan",
		Short:   "Plan a Graphene CI run",
		Example: "  graphen ci plan --config ./.graphen-ci/.graphen-ci.yaml",
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			flags, err := newPlanFlags(command)
			if err != nil {
				return err
			}

			return Plan(flags)
		},
	}

	command.Flags().String(
		"config",
		"./.graphen-ci/.graphen-ci.yaml",
		"path to the CI configuration file",
	)

	return command
}
