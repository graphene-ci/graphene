package commands

import (
	"fmt"

	"github.com/kardianos/service"
	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/app/daemon"
)

// The machine's half of graphened: getting a kernel installed, started,
// stopped and asked after.
//
// Each is three lines because the library underneath is the one that
// knows the difference between systemd, launchd and the Windows service
// manager, and this program has no business having an opinion about it.
func serviceCommands() []*cobra.Command {
	return []*cobra.Command{
		acting("install", "Install the kernel as a service",
			"Describe the kernel to this machine's service manager. It is "+
				"not started: whether it runs now is the decision of "+
				"whoever installed it.",
			(*daemon.Daemon).Install),

		acting("uninstall", "Remove the kernel from the service manager",
			"Take the kernel out of this machine's service manager. The "+
				"store and the configuration are left alone.",
			(*daemon.Daemon).Uninstall),

		acting("start", "Start the installed kernel",
			"Ask the service manager to start the kernel. It returns as "+
				"soon as the manager has been told.",
			(*daemon.Daemon).Start),

		acting("stop", "Stop the installed kernel",
			"Ask the service manager to stop the kernel. It winds down "+
				"rather than being killed, so calls in flight finish.",
			(*daemon.Daemon).Stop),

		acting("restart", "Restart the installed kernel",
			"Stop the kernel and start it again. This is how a change to "+
				"anything but the address takes effect.",
			(*daemon.Daemon).Restart),

		statusCommand(),
	}
}

// acting is one command that asks the service manager to do one thing.
func acting(use, short, long string, do func(*daemon.Daemon) error) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: short,
		Long:  long,
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			installed, err := managed(command)
			if err != nil {
				return err
			}

			return do(installed)
		},
	}
}

// statusCommand says what the machine thinks the kernel is doing.
//
// It answers about the SERVICE and not about the kernel's health: whether
// it is running, not whether it is well. The second question is asked
// over the wire, by anything speaking grpc.health.v1, and it has to be —
// a kernel can be running and unable to reach its store.
func statusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Say whether the kernel is installed and running",
		Long: "Ask the service manager what state the kernel is in. This is " +
			"whether it is running, not whether it is well: a kernel that " +
			"cannot reach its store is running and not well, and that " +
			"question is answered over the wire by grpc.health.v1.",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			installed, err := managed(command)
			if err != nil {
				return err
			}

			state, err := installed.Status()
			if err != nil {
				return err
			}

			_, err = fmt.Fprintln(command.OutOrStdout(), reads(state))

			return err
		},
	}
}

// reads turns a status into the word for it.
func reads(state service.Status) string {
	switch state {
	case service.StatusRunning:
		return "running"
	case service.StatusStopped:
		return "stopped"
	default:
		return "not installed"
	}
}

// managed describes this kernel to the service manager.
func managed(command *cobra.Command) (*daemon.Daemon, error) {
	return daemon.New(boot(), logger(command.OutOrStdout()))
}
