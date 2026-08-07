package commands

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/app/join"
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

	kernels.AddCommand(useCommand(), saveCommand(), forgetCommand(), joinCommand())

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

// savedFields is what `save` is told: a name, an address, a token and a
// pin. Four things because a context is four things, and one missing is
// a context that cannot be used.
const savedFields = 4

// saveCommand writes down a kernel somebody else's machine is running.
//
// The credential is an ARGUMENT and not a prompt, because this is the
// command a person runs from a script as often as by hand. It lands in a
// 0600 file and in that shell's history, which is the same trade every
// tool of this shape makes.
func saveCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "save <name> <address> <token> <pin>",
		Short: "Write down a kernel to talk to",
		Long: "Save a kernel under a name. The first one saved becomes the " +
			"one commands mean.\n\n" +
			"The pin says WHICH kernel is at that address — the kernel " +
			"prints its own with `graphened pin`. Without it a client " +
			"cannot tell the kernel it means from whoever else answers " +
			"there, and it is about to send that kernel a credential.",
		Args: cobra.ExactArgs(savedFields),
		RunE: func(command *cobra.Command, args []string) error {
			all, err := client.Read(contextsPath)
			if err != nil {
				return err
			}

			one, err := client.NewContext(args[0], args[1], args[2], args[3])
			if err != nil {
				return err
			}

			return all.Save(one)
		},
	}
}

// joinCommand makes a kernel allowed to be one.
//
// It writes two resources on the kernel this client is talking to: a role
// holding exactly what a kernel does, and an identity holding that role.
// What comes back is a credential, once — the store keeps a digest and
// cannot hand it back — which goes into the new kernel's `upstream.token`
// beside the pin.
//
// The grants are not asked for and cannot be chosen. They are what a
// kernel does, spelled in one place in the binary: its own record, its
// own processes' status, and read access to bytes. Typed by hand they
// would be reinvented per installation and wrong in a way nobody notices
// until a machine stops reporting.
func joinCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "join <name>",
		Short: "Let a kernel join this one, and print its credential",
		Long: "Write the role and identity a subordinate kernel needs, and " +
			"print its token.\n\nThe token is shown ONCE: what is stored is " +
			"a digest of it. Put it in that kernel's configuration under " +
			"`upstream.token`, beside the pin this kernel prints with " +
			"`graphened pin`.",
		Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			on, err := reached()
			if err != nil {
				return err
			}

			token, err := join.Join(command.Context(), on.Records(), args[0])
			if err != nil {
				return err
			}

			// Stdout, because this is a value meant to be piped into a
			// configuration file rather than read off a terminal.
			fmt.Fprintln(command.OutOrStdout(), token)

			return nil
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
