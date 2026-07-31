package block

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

type InitFlags struct {
	Lang string
	Path string
}

func newInitFlags(command *cobra.Command) (*InitFlags, error) {
	lang, err := command.Flags().GetString("lang")
	if err != nil {
		return nil, fmt.Errorf("read --lang: %w", err)
	}

	path, err := command.Flags().GetString("path")
	if err != nil {
		return nil, fmt.Errorf("read --path: %w", err)
	}

	return &InitFlags{
		Lang: lang,
		Path: path,
	}, nil
}

func (flags *InitFlags) Validate() error {
	if flags == nil {
		return errFlagsRequired
	}

	if strings.TrimSpace(flags.Lang) == "" {
		return errLangRequired
	}

	if strings.TrimSpace(flags.Path) == "" {
		return errPathRequired
	}

	return nil
}

func Init(flags *InitFlags) error {
	if err := flags.Validate(); err != nil {
		return err
	}

	return nil
}

func newInitCommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "init",
		Short:   "Initialize a Graphene block",
		Example: "  graphen block init --lang go --path ./someBlock",
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			flags, err := newInitFlags(command)
			if err != nil {
				return err
			}

			return Init(flags)
		},
	}

	command.Flags().String("lang", "go", "block implementation language")
	command.Flags().String("path", "", "directory for the new block")

	if err := command.MarkFlagRequired("path"); err != nil {
		panic(err)
	}

	return command
}
