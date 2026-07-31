package block

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

type GenFlags struct {
	Config string
}

func newGenFlags(command *cobra.Command) (*GenFlags, error) {
	config, err := command.Flags().GetString("config")
	if err != nil {
		return nil, fmt.Errorf("read --config: %w", err)
	}

	return &GenFlags{Config: config}, nil
}

func (flags *GenFlags) Validate() error {
	if flags == nil {
		return errors.New("block gen flags are required")
	}
	if strings.TrimSpace(flags.Config) == "" {
		return errors.New("--config is required")
	}

	return nil
}

func Gen(flags *GenFlags) error {
	if err := flags.Validate(); err != nil {
		return err
	}

	return nil
}

func newGenCommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "gen",
		Short:   "Generate Graphene block artifacts",
		Example: "  graphen block gen --config ./.graphen-block.yaml",
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			flags, err := newGenFlags(command)
			if err != nil {
				return err
			}

			return Gen(flags)
		},
	}

	command.Flags().String("config", "", "path to the block configuration file")

	if err := command.MarkFlagRequired("config"); err != nil {
		panic(err)
	}

	return command
}
