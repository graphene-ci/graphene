package commands

import (
	"io"

	"github.com/spf13/cobra"

	"github.com/gopherex/xlog"

	"github.com/graphene-ci/graphene/internal/app"
	"github.com/graphene-ci/graphene/internal/app/daemon"
)

// runCommand starts a kernel and keeps it running.
//
// It runs THROUGH the service manager, and that is the same call whether
// there is one or not: from a terminal it blocks until interrupted, from
// systemd it reports itself started and waits to be told to stop. A
// kernel that behaved differently under a manager would be a kernel
// nobody could reproduce a problem with by hand.
//
// Everything it actually does belongs to the application rather than to
// the command that started it: a second way in — a test, a supervisor,
// another binary — gets the same kernel without reaching through cobra
// for it.
func runCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Run the kernel",
		Long: "Open the store, publish what a kernel needs to work, and " +
			"serve until told to stop.",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			running, err := daemon.New(boot(), logger(command.OutOrStdout()))
			if err != nil {
				return err
			}

			return running.Run()
		},
	}
}

// boot is the one thing that cannot come from the configuration — where
// the configuration is — plus what this build calls itself.
func boot() app.Bootstrap {
	return app.Bootstrap{Config: configPath, Version: version}
}

// logger is what a kernel writes to.
//
// JSON, because a kernel's output is read by something before it is read
// by somebody. The library turns it human-readable by itself when the
// writer is a terminal, so the choice costs a person nothing.
func logger(out io.Writer) *xlog.Logger {
	return xlog.NewJSON(xlog.WithWriter(out))
}
