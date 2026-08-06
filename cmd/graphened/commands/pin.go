package commands

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/app/config"
	"github.com/graphene-ci/graphene/internal/link"
)

// pinCommand prints what this kernel is recognized by.
//
// It exists because there is no certificate authority: whoever points at
// this kernel — a client, or a kernel below it — has to be told which one
// it is, and this is the one line they are told. It prints nothing else
// and nothing decorative, so it can be read by a person and by a script
// setting up a machine.
//
// It does not make key material that is not there. A pin from a kernel
// that has never started would be a pin for a key nobody is serving with.
func pinCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "pin",
		Short: "Print the pin whoever points at this kernel must be told",
		Long: "Print this kernel's pin: the hash of the key it answers " +
			"with.\n\nGive it to whoever points at this kernel — `gctl " +
			"kernels save`, or the `upstream.pin` line of a subordinate's " +
			"configuration. It is public: it identifies this kernel and " +
			"grants nothing.",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			read, err := config.Read(configPath)
			if err != nil {
				return err
			}

			pinned, err := link.PinIn(keptIn(read))
			if err != nil {
				return fmt.Errorf(
					"%w; a kernel makes its key at the first start, so start it once", err)
			}

			// Stdout and not command.Println, which cobra sends to
			// STDERR. This is a value meant to be piped into a
			// configuration file, and one that went to stderr would
			// arrive on somebody's terminal and nowhere else.
			fmt.Fprintln(command.OutOrStdout(), pinned)

			return nil
		},
	}
}

// keptIn is where this kernel's key material lives, which is where
// everything else it owns lives: beside its store, or — for a kernel that
// keeps none — in the directory it runs things out of.
func keptIn(read config.Config) string {
	if up, forwards := read.Upstream(); forwards {
		return up.Work()
	}

	local, _ := read.Local()

	return filepath.Dir(local.Store())
}
