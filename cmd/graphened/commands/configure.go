package commands

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/app/config"
)

// What is opened when nobody said what to open with.
const fallbackEditor = "vi"

// configureCommand opens the kernel's file in an editor.
//
// AN EDITOR AND NOT FLAGS, because the file is the configuration —
// there is no second way to say any of this, and a flag for each line
// would be a second way that disagreed with the first. It is also what
// makes the whole thing recoverable: a kernel that will not start is
// fixed by editing the file, and this is that, with the file found for
// you.
//
// The file is created first if it is not there, so an editor is never
// opened on nothing. What it is created with is every default, which is
// the same configuration a kernel with no file would have run with.
func configureCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "configure",
		Short: "Edit the kernel's configuration",
		Long: "Open the kernel's file in $EDITOR, creating it with every " +
			"default if it is not there yet. A running kernel takes a " +
			"changed address at once; everything else applies at the next " +
			"start.",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			// Read and write it back rather than write a fresh one: a file
			// that is already there keeps what it says, including a
			// credential that cannot be produced again.
			current, err := config.Read(configPath)
			if err != nil {
				return err
			}

			if _, err := os.Stat(configPath); os.IsNotExist(err) {
				if err := config.Write(configPath, current); err != nil {
					return err
				}
			}

			return edit(command, configPath)
		},
	}
}

// edit hands the file to whatever the person edits with.
//
// The editor inherits this process's terminal, which is the only way a
// full-screen one works at all.
func edit(command *cobra.Command, at string) error {
	chosen := os.Getenv("EDITOR")
	if chosen == "" {
		chosen = os.Getenv("VISUAL")
	}

	if chosen == "" {
		chosen = fallbackEditor
	}

	// The program is the operator's own EDITOR, run on their own terminal
	// as themselves. Somebody who can set it can already run anything
	// this command could; refusing to honor it would only mean editing
	// the file with something else.
	editor := exec.CommandContext(command.Context(), chosen, at) //nolint:gosec // the operator's own editor
	editor.Stdin, editor.Stdout, editor.Stderr = os.Stdin, os.Stdout, os.Stderr

	if err := editor.Run(); err != nil {
		return fmt.Errorf("%s %s: %w", chosen, at, err)
	}

	// Read it back, so a file that no longer parses is said so NOW —
	// while the person who broke it is still here — rather than at the
	// next start by a service manager nobody is watching.
	if _, err := config.Read(at); err != nil {
		return err
	}

	return nil
}
