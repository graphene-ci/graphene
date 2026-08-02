package ci

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

const defaultConfigPath = "./.graphene-ci/.graphene-ci.yaml"

var (
	errFlagsRequired  = errors.New("flags are required")
	errConfigRequired = errors.New("--config must not be empty")
	errLangRequired   = errors.New("--lang must not be empty")
	errPathRequired   = errors.New("--path is required")
)

// ConfigFlags is the flag set shared by the config-driven subcommands.
type ConfigFlags struct {
	Config string
}

func newConfigFlags(command *cobra.Command) (*ConfigFlags, error) {
	config, err := command.Flags().GetString("config")
	if err != nil {
		return nil, fmt.Errorf("read --config: %w", err)
	}

	return &ConfigFlags{Config: config}, nil
}

// Validate checks the flag values.
func (flags *ConfigFlags) Validate() error {
	if flags == nil {
		return errFlagsRequired
	}

	if strings.TrimSpace(flags.Config) == "" {
		return errConfigRequired
	}

	return nil
}

// newConfigCommand builds a subcommand with a defaulted --config flag.
func newConfigCommand(use, short, example string, run func(*ConfigFlags) error) *cobra.Command {
	command := &cobra.Command{
		Use:     use,
		Short:   short,
		Example: example,
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			flags, err := newConfigFlags(command)
			if err != nil {
				return err
			}

			return run(flags)
		},
	}

	command.Flags().String(
		"config",
		defaultConfigPath,
		"path to the CI configuration file",
	)

	return command
}
