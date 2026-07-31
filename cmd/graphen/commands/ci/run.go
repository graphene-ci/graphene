package ci

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

type RunFlags struct {
	Config string
	Watch  bool
}

func newRunFlags(command *cobra.Command) (*RunFlags, error) {
	config, err := command.Flags().GetString("config")
	if err != nil {
		return nil, fmt.Errorf("read --config: %w", err)
	}

	watch, err := command.Flags().GetBool("watch")
	if err != nil {
		return nil, fmt.Errorf("read --watch: %w", err)
	}

	return &RunFlags{
		Config: config,
		Watch:  watch,
	}, nil
}

func (flags *RunFlags) Validate() error {
	if flags == nil {
		return errFlagsRequired
	}

	if strings.TrimSpace(flags.Config) == "" {
		return errConfigRequired
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
		Short:   "Run a Graphene CI project",
		Example: "  graphen ci run --config ./.graphen-ci/.graphen-ci.yaml --watch",
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			flags, err := newRunFlags(command)
			if err != nil {
				return err
			}

			return Run(flags)
		},
	}

	command.Flags().String(
		"config",
		"./.graphen-ci/.graphen-ci.yaml",
		"path to the CI configuration file",
	)
	command.Flags().Bool("watch", false, "watch the CI run until completion")

	return command
}
