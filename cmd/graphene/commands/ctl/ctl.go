// Package ctl is the command surface for talking to a kernel: reading,
// applying and watching resources through the same API everything else
// uses.
package ctl

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	appctl "github.com/graphene-ci/graphene/internal/app/ctl"
	"github.com/graphene-ci/graphene/internal/utils/cmdflags"
)

// The cobra command tree is assembled from package-level commands.
//
//nolint:gochecknoglobals // see above
var Cmd = newCommand()

var (
	errFlagsRequired = errors.New("flags are required")
	errNoTarget      = errors.New(
		"no kernel found: pass --address or --socket, set GRAPHENE_ADDRESS/GRAPHENE_SOCKET, " +
			"or install one with `graphene kernel install`")
	errNoToken = errors.New(
		"no token found: pass --token, set GRAPHENE_TOKEN, or install a kernel whose token file this user can read")
	errKindRequired = errors.New("--kind is required")
)

// TargetFlags are the connection inputs shared by every subcommand: what
// was typed, plus which client configuration and context to fall back to.
type TargetFlags struct {
	Address string
	Socket  string
	CAFile  string
	Token   string
	Config  string
	Context string
}

// Validate reports whether a kernel can be reached at all — after the
// client configuration and the local installation have had their say.
func (flags *TargetFlags) Validate() error {
	if flags == nil {
		return errFlagsRequired
	}

	resolved, err := flags.target()
	if err != nil {
		return err
	}

	if resolved.Address == "" && resolved.Socket == "" {
		return errNoTarget
	}

	if resolved.Token == "" {
		return errNoToken
	}

	return nil
}

// target resolves what was typed against the client configuration and,
// last, against a kernel installed on this machine.
func (flags *TargetFlags) target() (appctl.Target, error) {
	target, err := appctl.Resolve(appctl.Target{
		Address: flags.Address,
		Socket:  flags.Socket,
		CAFile:  flags.CAFile,
		Token:   flags.Token,
	}, flags.Config, flags.Context)
	if err != nil {
		return appctl.Target{}, fmt.Errorf("ctl: %w", err)
	}

	return target, nil
}

// connect validates and dials.
func connect(flags *TargetFlags) (*appctl.Client, error) {
	if err := flags.Validate(); err != nil {
		return nil, err
	}

	target, err := flags.target()
	if err != nil {
		return nil, err
	}

	client, err := appctl.Connect(target)
	if err != nil {
		return nil, fmt.Errorf("ctl: %w", err)
	}

	return client, nil
}

func newCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "ctl",
		Short: "Talk to a Graphene kernel",
	}

	command.PersistentFlags().String("address", "", "kernel address (host:port)")
	command.PersistentFlags().String("socket", "", "kernel unix socket path")
	command.PersistentFlags().String("ca-file", "", "certificate authority to pin (graphene kernel ca)")
	command.PersistentFlags().String("token", "", "bearer token; otherwise taken from the context")
	command.PersistentFlags().String("config", "",
		"client configuration file (default: $GRAPHENE_CONFIG or the user config dir)")
	command.PersistentFlags().String("context", "", "context to use (default: the selected one)")

	command.AddCommand(
		newGetCommand(),
		newApplyCommand(),
		newDeleteCommand(),
		newWatchCommand(),
		newDefinitionsCommand(),
		newUndefineCommand(),
		newBlobCommand(),
		newContextCommand(),
	)

	return command
}

// newTargetFlags reads the connection flags.
func newTargetFlags(command *cobra.Command) (*TargetFlags, error) {
	values, err := cmdflags.Strings(command, "address", "socket", "ca-file", "token", "config", "context")
	if err != nil {
		return nil, err
	}

	return &TargetFlags{
		Address: values[0],
		Socket:  values[1],
		CAFile:  values[2],
		Token:   values[3],
		Config:  values[4],
		Context: values[5],
	}, nil
}
