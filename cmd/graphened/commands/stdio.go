package commands

import (
	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/app"
	"github.com/graphene-ci/graphene/internal/app/config"
)

// stdioCommand relays the pipe it was started in to the kernel running
// on this machine.
//
// It is how a kernel is reached with no port open:
//
//	gctl kernels save build-01 "exec:ssh build-01 graphened stdio" <token>
//
// ssh is already an authenticated, encrypted way onto that machine, and
// this answers in the pipe it lands in. Nothing is listening, so nothing
// can be found by scanning, and the credential that got there is the
// operator's own.
//
// IT SERVES NOTHING AND DECIDES NOTHING. What crosses the pipe is what
// would have crossed a socket, and the kernel at the other end reads the
// caller's credential exactly as it does over a port. The kernel has to
// be RUNNING — that is the case this exists for, and an earlier version
// that opened the store itself could not handle it: one store is one
// process, so it waited for a lock the running kernel was holding.
//
// A CLIENT'S WAY IN AND NOT A KERNEL'S. A kernel that forwards to another
// one holds the link open for as long as it runs and has to survive it
// dropping; a pipe that dies is a kernel that has stopped forwarding, and
// what to do about that is a design — reconnect, back off, say so in the
// health — rather than a flag. Kernels talk to kernels over a port.
func stdioCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "stdio",
		Short: "Answer on standard input and output instead of a port",
		Long: "Relay this process's pipes to the kernel running on this " +
			"machine, for reaching one without a port:\n\n" +
			"  gctl kernels save one \"exec:ssh host graphened stdio\" <token>\n\n" +
			"The kernel must be running: this serves nothing itself, it " +
			"carries bytes to the socket that kernel opened.",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			read, err := config.Read(configPath)
			if err != nil {
				return err
			}

			return app.Stdio(command.Context(), keptIn(read),
				command.InOrStdin(), command.OutOrStdout())
		},
	}
}
