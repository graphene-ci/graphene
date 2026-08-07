package commands

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/app/config"
	"github.com/graphene-ci/graphene/internal/link"
)

// rekeyCommands replace the key this kernel is recognized by.
//
// TWO STEPS, and the reason is the order the world has to learn things
// in. A pin names a key, so replacing the key means telling everything
// that points at this kernel a new one. Minting first and telling
// afterwards leaves every client refusing this kernel until the last one
// is edited; telling first is impossible unless the pin can be known
// before it is served.
//
// So: prepare prints the next pin without serving it, the pin goes
// wherever this kernel is pointed at — beside the old one, because a
// client accepts several — and commit starts serving it at the next
// start. At no point can a correctly configured client not connect, and
// at no point is a key served that nobody was told about.
func rekeyCommands() *cobra.Command {
	rekey := &cobra.Command{
		Use:   "rekey",
		Short: "Replace the key this kernel is recognized by",
		Long: "Replace this kernel's key in two steps, so that nothing " +
			"pointing at it is left unable to connect:\n\n" +
			"  graphened rekey prepare   print the next pin; nothing serves it yet\n" +
			"  <add that pin beside the old one, wherever this kernel is pointed at>\n" +
			"  graphened rekey commit    serve it, from the next start\n" +
			"  <drop the old pin at leisure>",
	}

	rekey.AddCommand(prepareCommand(), commitCommand())

	return rekey
}

func prepareCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "prepare",
		Short: "Make the next key and print its pin",
		Long: "Make the key this kernel will serve next and print its " +
			"pin. Nothing serves it yet, so this is safe to run and to " +
			"undo by doing nothing.\n\nRunning it twice prints the same " +
			"pin rather than making a second key: somebody halfway " +
			"through handing one out should not have it change under them.",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			read, err := config.Read(configPath)
			if err != nil {
				return err
			}

			pinned, err := link.Prepare(keptIn(read))
			if err != nil {
				return err
			}

			fmt.Fprintln(command.OutOrStdout(), pinned)

			return nil
		},
	}
}

func commitCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "commit",
		Short: "Start serving the prepared key",
		Long: "Make the prepared key the one this kernel answers with. It " +
			"takes effect at the NEXT START: swapping the key under a " +
			"running listener would drop every connection at a moment " +
			"nobody chose.\n\nAnything still pinned only to the old key " +
			"stops being able to reach this kernel, which is why prepare " +
			"prints the new pin first.",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			read, err := config.Read(configPath)
			if err != nil {
				return err
			}

			pinned, err := link.Commit(keptIn(read))
			if err != nil {
				return err
			}

			fmt.Fprintln(command.OutOrStdout(), pinned)
			command.PrintErrln("restart the kernel for it to answer with this key")

			return nil
		},
	}
}
