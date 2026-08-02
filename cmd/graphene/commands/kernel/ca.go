package kernel

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/app/config"
	tlsutil "github.com/graphene-ci/graphene/internal/infrastructure/tls"
)

// errNoTLS — the kernel serves no TLS endpoint, so it has no CA to hand out.
var errNoTLS = errors.New("this kernel has no tls section: nothing to print")

// CA prints the certificate authority clients must pin to talk to this
// kernel over TCP. Pinning is explicit on purpose: no trust-on-first-use.
func CA(flags *RunFlags) error {
	if err := flags.Validate(); err != nil {
		return err
	}

	cfg, err := config.Load(flags.Config)
	if err != nil {
		return fmt.Errorf("kernel ca: %w", err)
	}

	if cfg.TLS == nil {
		return errNoTLS
	}

	pem, err := tlsutil.CACertPEM(cfg.TLS.Dir)
	if err != nil {
		return fmt.Errorf("kernel ca: %w", err)
	}

	if _, err := os.Stdout.Write(pem); err != nil {
		return fmt.Errorf("kernel ca: write: %w", err)
	}

	return nil
}

func newCACommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "ca",
		Short:   "Print the kernel's certificate authority",
		Example: "  graphene kernel ca --config ./graphene-kernel.yaml > ca.crt",
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			flags, err := newRunFlags(command)
			if err != nil {
				return err
			}

			return CA(flags)
		},
	}

	command.Flags().String("config", "", "path to the kernel configuration file")

	if err := command.MarkFlagRequired("config"); err != nil {
		panic(err)
	}

	return command
}
