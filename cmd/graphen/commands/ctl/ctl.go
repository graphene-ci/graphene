// Package ctl is the command surface for talking to a kernel: reading,
// applying and watching resources through the same API everything else
// uses.
package ctl

import (
	"errors"
	"fmt"
	"os"
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
	errNoTarget      = errors.New("--address or --socket is required")
	errNoToken       = errors.New("a token is required (--token or GRAPHEN_TOKEN)")
	errKindRequired  = errors.New("--kind is required")
)

// TargetFlags are the connection inputs shared by every subcommand.
type TargetFlags struct {
	Address string
	Socket  string
	CAFile  string
	Token   string
}

// Validate checks the connection inputs.
func (flags *TargetFlags) Validate() error {
	if flags == nil {
		return errFlagsRequired
	}

	if strings.TrimSpace(flags.Address) == "" && strings.TrimSpace(flags.Socket) == "" {
		return errNoTarget
	}

	if strings.TrimSpace(flags.Token) == "" {
		return errNoToken
	}

	return nil
}

func (flags *TargetFlags) target() appctl.Target {
	return appctl.Target{
		Address: flags.Address,
		Socket:  flags.Socket,
		CAFile:  flags.CAFile,
		Token:   flags.Token,
	}
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

	if token == "" {
		token = os.Getenv("GRAPHEN_TOKEN")
	}

	return &TargetFlags{Address: address, Socket: socket, CAFile: caFile, Token: token}, nil
}
