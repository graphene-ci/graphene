// Package commands is gctl's surface: everything about talking TO a
// kernel, and nothing about being one.
//
// It runs for a second from somebody's laptop and has no business
// touching a kernel's file — except to FIND one on this machine, which is
// the one thing that would otherwise mean copying an address and a
// credential somebody already has.
//
// The shape is kubectl's, because it is the shape people already know:
//
//	gctl get Process              every Process
//	gctl get Process local/one    one of them
//	gctl watch Process            the same, as it changes
//	gctl delete Process local/one
//	gctl apply -f process.yaml
//	gctl kernels                  which kernels this client knows
package commands

import (
	"context"
	"os"

	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/app/config"
	"github.com/graphene-ci/graphene/internal/client"
)

// version is what this build calls itself, stamped at link time.
var version = "dev"

// The two things that cannot come from anything else: where the contexts
// are, and which of them a command means.
//
//nolint:gochecknoglobals // a validated value cannot be a const; treated as one
var (
	contextsPath string
	kernelName   string
)

// Root is gctl.
func Root() *cobra.Command {
	root := &cobra.Command{
		Use:           "gctl",
		Short:         "Talk to a graphene kernel",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version,
	}

	root.PersistentFlags().StringVar(&contextsPath, "contexts", client.DefaultPath(),
		"the file this client keeps kernels in")
	root.PersistentFlags().StringVar(&kernelName, "kernel", "",
		"which saved kernel to talk to, instead of the current one")

	root.AddCommand(
		getCommand(),
		watchCommand(),
		deleteCommand(),
		applyCommand(),
		kindsCommand(),
		kernelsCommand(),
	)

	return root
}

// Execute runs gctl and reports whether it failed.
func Execute() int {
	if err := Root().Execute(); err != nil {
		_, _ = os.Stderr.WriteString("gctl: " + err.Error() + "\n")

		return 1
	}

	return 0
}

// reached opens the kernel a command means.
//
// Named, or the current one, or the one running on this machine — and in
// the last case it is SAVED, so it is discovered once and is an ordinary
// context afterwards.
func reached() (*client.Kernel, error) {
	all, err := client.Read(contextsPath)
	if err != nil {
		return nil, err
	}

	one, err := client.Reach(all, kernelName, config.DefaultPath())
	if err != nil {
		return nil, err
	}

	opened, err := client.Dial(one)
	if err != nil {
		return nil, err
	}

	// The connection lives as long as the command does, which is a second
	// or a watch that runs until somebody stops it.
	cobra.OnFinalize(func() { _ = opened.Close() })

	return opened, nil
}

// calling is the context one call is made with: the caller's credential
// on it, and the command's cancellation behind it.
func calling(command *cobra.Command, on *client.Kernel) context.Context {
	return on.As(command.Context())
}
