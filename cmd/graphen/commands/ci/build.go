package ci

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

type BuildFlags struct {
	Config string
}

func newBuildFlags(command *cobra.Command) (*BuildFlags, error) {
	config, err := command.Flags().GetString("config")
	if err != nil {
		return nil, fmt.Errorf("read --config: %w", err)
	}

	return &BuildFlags{Config: config}, nil
}

func (flags *BuildFlags) Validate() error {
	if flags == nil {
		return errors.New("CI build flags are required")
	}
	if strings.TrimSpace(flags.Config) == "" {
		return errors.New("--config must not be empty")
	}

	return nil
}

func Build(flags *BuildFlags) error {
	if err := flags.Validate(); err != nil {
		return err
	}

	return nil
}

func newBuildCommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "build",
		Short:   "Build a Graphene CI project",
		Example: "  graphen ci build --config ./.graphen-ci/.graphen-ci.yaml",
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			flags, err := newBuildFlags(command)
			if err != nil {
				return err
			}

			return Build(flags)
		},
	}

	command.Flags().String(
		"config",
		"./.graphen-ci/.graphen-ci.yaml",
		"path to the CI configuration file",
	)

	return command
}
