package commands

import (
	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/client"
)

// kernelsCommand is which kernels this client knows, and which one it
// means.
//
// A CLIENT'S list and not a kernel's: these are addresses somebody
// carries between machines. The credentials in the file are not printed —
// this output is pasted into issues.
func kernelsCommand() *cobra.Command {
	kernels := &cobra.Command{
		Use:   "kernels",
		Short: "List the kernels this client knows",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			all, err := client.Read(contextsPath)
			if err != nil {
				return err
			}

			current, _ := all.Current()

			return tabulated(command.OutOrStdout(), func(write func(...string)) error {
				write("", "NAME", "ADDRESS")

				for _, one := range all.All() {
					write(marked(one, current), one.Name(), one.Address())
				}

				return nil
			})
		},
	}

	kernels.AddCommand(useCommand(), saveCommand(), forgetCommand())

	return kernels
}

// useCommand picks which kernel the other commands mean.
func useCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "use <name>",
		Short: "Talk to this kernel from now on",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			all, err := client.Read(contextsPath)
			if err != nil {
				return err
			}

			return all.Use(args[0])
		},
	}
}

// saveCommand writes down a kernel somebody else's machine is running.
//
// The credential is an ARGUMENT and not a prompt, because this is the
// command a person runs from a script as often as by hand. It lands in a
// 0600 file and in that shell's history, which is the same trade every
// tool of this shape makes.
func saveCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "save <name> <address> <token>",
		Short: "Write down a kernel to talk to",
		Long: "Save a kernel under a name. The first one saved becomes the " +
			"one commands mean.",
		Args: cobra.ExactArgs(3),
		RunE: func(command *cobra.Command, args []string) error {
			all, err := client.Read(contextsPath)
			if err != nil {
				return err
			}

			one, err := client.NewContext(args[0], args[1], args[2])
			if err != nil {
				return err
			}

			return all.Save(one)
		},
	}
}

// forgetCommand drops one.
func forgetCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "forget <name>",
		Short: "Forget a kernel",
		Long: "Drop a kernel from this client's list. Forgetting the one " +
			"in use leaves none in use rather than quietly switching to " +
			"another.",
		Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			all, err := client.Read(contextsPath)
			if err != nil {
				return err
			}

			return all.Forget(args[0])
		},
	}
}

// marked points at the current one.
func marked(one, current client.Context) string {
	if one.Name() == current.Name() {
		return "*"
	}

	return ""
}
