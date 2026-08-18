// Command graphene-server is the graphene control plane: the single door
// of an installation. It hosts the server worker with the system
// resource flows (registered from the pipeline library), implements
// their Ops, serves the runs API, the agent sessions, and the Temporal
// gRPC proxy — Temporal itself is visible to nobody else.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/graphene-ci/graphene/internal/config"
	"github.com/graphene-ci/graphene/internal/server"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println("graphene-server", version)
		return
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "graphene-server:", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	log.Info("starting", "version", version)
	err = server.Run(ctx, cfg, log)
	stop()
	if err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, "graphene-server:", err)
		os.Exit(1)
	}
}
