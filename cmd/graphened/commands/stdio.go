package commands

import (
	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/app"
)

// stdioCommand answers on the pipe it was started in.
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
// A CLIENT'S WAY IN AND NOT A KERNEL'S. A kernel that forwards to another
// one holds the link open for as long as it runs and has to survive it
// dropping; a pipe that dies is a kernel that has stopped forwarding, and
// what to do about that is a design — reconnect, back off, say so in the
// health — rather than a flag. Kernels talk to kernels over a port.
func stdioCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "stdio",
		Short: "Answer on standard input and output instead of a port",
		Long: "Serve one client on this process's own pipes, for reaching " +
			"a kernel that listens on nothing:\n\n" +
			"  gctl kernels save one \"exec:ssh host graphened stdio\" <token>\n\n" +
			"One client, for as long as the pipe is open. The kernel this " +
			"talks to is the one this machine's configuration describes.",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return app.Stdio(command.Context(), boot(), command.InOrStdin(), command.OutOrStdout())
		},
	}
}
