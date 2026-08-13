// Command graphene is the one thing a person runs: it sets up the local
// environment, pushes pipelines, starts runs and watches them.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/graphene-ci/graphene/cmd/graphene/commands"
)

func main() {
	// os.Exit skips deferred calls, so the signal handler is released here
	// rather than in a defer that would never run.
	os.Exit(run())
}

// run executes the command tree and returns the process exit code.
func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := commands.Root().ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "graphene:", err)

		return 1
	}

	return 0
}
