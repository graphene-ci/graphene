// Package ctl is the command surface for talking to a kernel: reading,
// applying and watching resources through the same API everything else
// uses.
package ctl

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	appctl "github.com/graphene-ci/graphene/internal/app/ctl"
)

// The cobra command tree is assembled from package-level commands.
//
//nolint:gochecknoglobals // see above
var Cmd = newCommand()

var (
	errFlagsRequired = errors.New("flags are required")
	errNoTarget      = errors.New(
		"no kernel found: pass --address or --socket, set GRAPHEN_ADDRESS/GRAPHEN_SOCKET, " +
			"or install one with `graphen kernel install`")
	errNoToken = errors.New(
		"no token found: pass --token, set GRAPHEN_TOKEN, or install a kernel whose token file this user can read")
	errKindRequired = errors.New("--kind is required")
)

// TargetFlags are the connection inputs shared by every subcommand.
type TargetFlags struct {
	Address string
	Socket  string
	CAFile  string
	Token   string
}

// Validate checks the connection inputs AFTER discovery: what matters is
// whether a kernel can be reached at all, not whether it was typed out.
func (flags *TargetFlags) Validate() error {
	if flags == nil {
		return errFlagsRequired
	}

	resolved := flags.target()

	if strings.TrimSpace(resolved.Address) == "" && strings.TrimSpace(resolved.Socket) == "" {
		return errNoTarget
	}

	if strings.TrimSpace(resolved.Token) == "" {
		return errNoToken
	}

	return nil
}

// target resolves what was typed against what is installed: a kernel on
// this machine is reachable without naming its socket or its token.
func (flags *TargetFlags) target() appctl.Target {
	return appctl.Discover(appctl.Target{
		Address: flags.Address,
		Socket:  flags.Socket,
		CAFile:  flags.CAFile,
		Token:   flags.Token,
	})
}

// connect validates and dials.
func connect(flags *TargetFlags) (*appctl.Client, error) {
	if err := flags.Validate(); err != nil {
		return nil, err
	}

	client, err := appctl.Connect(flags.target())
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
	command.PersistentFlags().String("ca-file", "", "certificate authority to pin (graphen kernel ca)")
	command.PersistentFlags().String("token", "", "bearer token; defaults to $GRAPHEN_TOKEN")

	command.AddCommand(
		newGetCommand(),
		newApplyCommand(),
		newDeleteCommand(),
		newWatchCommand(),
		newDefinitionsCommand(),
	)

	return command
}

// newTargetFlags reads the connection flags, falling back to the
// environment for the token so it never has to appear in a shell history.
func newTargetFlags(command *cobra.Command) (*TargetFlags, error) {
	address, err := command.Flags().GetString("address")
	if err != nil {
		return nil, fmt.Errorf("read --address: %w", err)
	}

	socket, err := command.Flags().GetString("socket")
	if err != nil {
		return nil, fmt.Errorf("read --socket: %w", err)
	}

	caFile, err := command.Flags().GetString("ca-file")
	if err != nil {
		return nil, fmt.Errorf("read --ca-file: %w", err)
	}

	token, err := command.Flags().GetString("token")
	if err != nil {
		return nil, fmt.Errorf("read --token: %w", err)
	}

	return &TargetFlags{Address: address, Socket: socket, CAFile: caFile, Token: token}, nil
}
