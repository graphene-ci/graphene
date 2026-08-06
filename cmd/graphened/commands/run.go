package commands

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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
	if err := os.MkdirAll(filepath.Dir(storePath), 0o700); err != nil {
		return fmt.Errorf("prepare %s: %w", filepath.Dir(storePath), err)
	}

	stop := xshutdown.New(ctx,
		xshutdown.WithTimeout(drain),
		xshutdown.WithForceExit(forced),
		xshutdown.WithErrorHandler(func(err error) {
			_, _ = fmt.Fprintf(out, "shutdown: %v\n", err)
		}),
	)

	kernel, err := app.Open(stop.Context(), app.Bootstrap{
		Store:   storePath,
		Name:    name,
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

	server, listener, err := kernel.Server(log)
	if err != nil {
		return err
	}

	// Three workers, and this is the whole list.
	//
	//  1. the server, which ends when GracefulStop is called
	//  2. the stop itself, which is what calls it
	//  3. the config watch, which ends when the context does
	//
	// The second cannot be cleanup instead: cleanup runs after the drain,
	// and the drain is waiting for the call the second one ends.
	stop.Go(func(context.Context) {
		if err := server.Serve(listener); err != nil {
			log.Error("serve", "err", err)
		}
	})

	stop.Go(func(ctx context.Context) {
		<-ctx.Done()
		server.GracefulStop()
	})

	stop.Go(func(ctx context.Context) {
		if err := kernel.Follow(ctx); err != nil && ctx.Err() == nil {
			log.Error("config watch", "err", err)
		}
	})

	log.Info("serving", "kernel", name, "address", listener.Addr().String())

	// Run installs the signals, blocks, and then drains.
	if err := stop.Run(); err != nil {
		return err
	}

	log.Info("stopped")

	return nil
}
