package block

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
		return errors.New("block build flags are required")
	}
	if strings.TrimSpace(flags.Config) == "" {
		return errors.New("--config is required")
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
		Short:   "Build a Graphene block",
		Example: "  graphen block build --config ./.graphen-block.yaml",
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			flags, err := newBuildFlags(command)
			if err != nil {
				return err
			}

			return Build(flags)
		},
	}

	command.Flags().String("config", "", "path to the block configuration file")

	if err := command.MarkFlagRequired("config"); err != nil {
		panic(err)
	}

	return command
}
