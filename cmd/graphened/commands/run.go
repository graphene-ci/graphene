package commands

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/gopherex/xshutdown"
	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/app"
)

// How long a kernel is given to wind down before it is taken down.
//
// A shutdown that could hang forever is a service that has to be killed,
// and a service that is killed loses whatever it was in the middle of.
const (
	drain = 15 * time.Second
	// forced is the exit code when the drain did not finish. It is
	// distinct from a plain failure so that whoever reads it knows the
	// difference between "it stopped badly" and "it would not stop".
	forced = 2
)

// runCommand starts a kernel and keeps it running.
func runCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Run the kernel",
		Long: "Open the store, publish what a kernel needs to work, and " +
			"serve until told to stop.",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return run(command.Context(), command.OutOrStdout())
		},
	}
}

// run is the whole of a kernel's life, and every goroutine in it.
//
// The rule is that nothing below the composition root starts a goroutine
// — a watch is pulled rather than pushed for exactly that reason — so the
// concurrency of a running kernel is the list of stop.Go calls here, and
// the list is short enough to read.
//
// The manager is what makes that more than a convention. A goroutine it
// starts is tracked, told to stop through a context that cascades, and
// waited for; one started any other way is none of those things, and the
// difference shows up as a shutdown that does not finish.
func run(ctx context.Context, out io.Writer) error {
	stop := xshutdown.New(ctx,
		xshutdown.WithTimeout(drain),
		xshutdown.WithForceExit(forced),
		xshutdown.WithErrorHandler(func(err error) {
			_, _ = fmt.Fprintf(out, "shutdown: %v\n", err)
		}),
	)

	kernel, err := app.Open(stop.Context(), app.Bootstrap{
		Config:  configPath,
		Version: version,
	})
	if err != nil {
		return err
	}

	// Closing the store is cleanup and not work: it runs after the last
	// worker has wound down, which is the only order in which closing a
	// file out from under one of them cannot happen.
	stop.RegisterFnErr(func(context.Context) error { return kernel.Close() })

	log := slog.New(slog.NewTextHandler(out, nil))

	endpoint := kernel.Endpoint(log)

	// Three workers, and this is the whole list — a kernel that has
	// rebound a hundred times still has these three.
	//
	//  1. Serve, which stands a server up and stands it up again
	//  2. Rebind, which stops the one that is up when it should not be
	//  3. Watch, which keeps the configuration up to date with the file
	//
	// The second is what makes the first return, so it cannot be cleanup
	// instead: cleanup runs after the drain, and the drain is waiting for
	// the call the second one ends.
	stop.Go(func(ctx context.Context) {
		if err := endpoint.Serve(ctx); err != nil {
			log.Error("serve", "err", err)
		}
	})

	stop.Go(func(ctx context.Context) {
		if err := endpoint.Rebind(ctx); err != nil {
			log.Error("rebind", "err", err)
		}
	})

	stop.Go(func(ctx context.Context) {
		if err := kernel.Watch(ctx, log); err != nil {
			log.Error("config watch", "err", err)
		}
	})

	log.Info("kernel", "config", configPath, "running", kernel.Config())

	// Run installs the signals, blocks, and then drains.
	if err := stop.Run(); err != nil {
		return err
	}

	log.Info("stopped")

	return nil
}
