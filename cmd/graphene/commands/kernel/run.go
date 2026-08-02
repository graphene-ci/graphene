package kernel

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/app/config"
	appkernel "github.com/graphene-ci/graphene/internal/app/kernel"
	"github.com/graphene-ci/graphene/internal/utils/cmdflags"
)

var (
	errFlagsRequired  = errors.New("flags are required")
	errConfigRequired = errors.New("--config is required")
)

// RunFlags are the inputs of the run command. Everything else about a
// kernel lives in its configuration file (or environment).
type RunFlags struct {
	Config string
}

func newRunFlags(command *cobra.Command) (*RunFlags, error) {
	path, err := cmdflags.String(command, "config")
	if err != nil {
		return nil, err
	}

	return &RunFlags{Config: path}, nil
}

// Validate checks the flag values.
func (flags *RunFlags) Validate() error {
	if flags == nil {
		return errFlagsRequired
	}

	if strings.TrimSpace(flags.Config) == "" {
		return errConfigRequired
	}

	return nil
}

// Run loads the configuration, assembles the kernel and serves until the
// process is asked to stop.
func Run(flags *RunFlags) error {
	if err := flags.Validate(); err != nil {
		return err
	}

	cfg, err := config.Load(flags.Config)
	if err != nil {
		return fmt.Errorf("kernel run: %w", err)
	}

	log := appkernel.NewLogger(cfg.Log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	kern, err := appkernel.New(ctx, cfg, log)
	if err != nil {
		return fmt.Errorf("kernel run: %w", err)
	}

	defer func() {
		if cerr := kern.Close(); cerr != nil {
			log.Error("shutdown", "error", cerr)
		}
	}()

	log.Info("kernel starting", "tenant", cfg.Identity.Tenant, "name", cfg.Identity.Name)

	if err := kern.Run(ctx); err != nil {
		return fmt.Errorf("kernel run: %w", err)
	}

	log.Info("kernel stopped")

	return nil
}

func newRunCommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "run",
		Short:   "Run the Graphene kernel",
		Example: "  graphene kernel run --config ./graphene-kernel.yaml",
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			flags, err := newRunFlags(command)
			if err != nil {
				return err
			}

			return Run(flags)
		},
	}

	command.Flags().String("config", "", "path to the kernel configuration file")

	if err := command.MarkFlagRequired("config"); err != nil {
		panic(err)
	}

	return command
}
