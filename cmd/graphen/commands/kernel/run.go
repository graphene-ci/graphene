package kernel

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

type RunFlags struct {
	Config string
}

func newRunFlags(command *cobra.Command) (*RunFlags, error) {
	config, err := command.Flags().GetString("config")
	if err != nil {
		return nil, fmt.Errorf("read --config: %w", err)
	}

	return &RunFlags{Config: config}, nil
}

func (flags *RunFlags) Validate() error {
	if flags == nil {
		return errors.New("kernel run flags are required")
	}
	if strings.TrimSpace(flags.Config) == "" {
		return errors.New("--config is required")
	}

	return nil
}

func Run(flags *RunFlags) error {
	if err := flags.Validate(); err != nil {
		return err
	}

	return nil
}

func newRunCommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "run",
		Short:   "Run the Graphene kernel",
		Example: "  graphen kernel run --config ./graphen-kernel.yaml",
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			flags, err := newRunFlags(command)
			if err != nil {
				return err
			}

			return Run(flags)
		},
	}

	command.Flags().String("config", "", "path to the kernel configuration file")

	if err := command.MarkFlagRequired("config"); err != nil {
		panic(err)
	}

	return command
}
