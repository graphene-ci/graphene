package commands

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/app"
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
// There are two, and both start HERE. That is the point of the rule:
// nothing below this function spawns one — a watch is pulled rather than
// pushed for exactly that reason — so the concurrency of a running kernel
// is this list, and this list is short enough to read.
//
//  1. Follow, which keeps the configuration up to date with the record
//  2. the signal wait, which is what ends the other one
//
// A third will arrive with the wire, and it will arrive here.
func run(ctx context.Context, out interface{ Write([]byte) (int, error) }) error {
	if err := os.MkdirAll(filepath.Dir(storePath), 0o700); err != nil {
		return fmt.Errorf("prepare %s: %w", filepath.Dir(storePath), err)
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	kernel, err := app.Open(ctx, app.Bootstrap{
		Store:   storePath,
		Name:    name,
		Version: version,
	})
	if err != nil {
		return err
	}

	defer func() { _ = kernel.Close() }()

	_, _ = fmt.Fprintf(out, "kernel %s on %s\n", name, storePath)
	_, _ = fmt.Fprintf(out, "configured: %s\n", kernel.Config())

	// The one loop this command runs. It ends when the context does,
	// which is when a signal arrives.
	followed := make(chan error, 1)

	go func() { followed <- kernel.Follow(ctx) }()

	select {
	case err := <-followed:
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}

	case <-ctx.Done():
		// Wait for Follow to notice, so that shutting down is a thing
		// that finished rather than a thing that was abandoned.
		<-followed
	}

	_, _ = fmt.Fprintln(out, "stopped")

	return nil
}
