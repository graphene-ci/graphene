// Command graphene-server is the graphene control plane: the single door
// of an installation. It hosts the server worker with the system
// resource flows (registered from the pipeline library), implements
// their Ops, serves the runs API, the agent sessions, the health probes,
// and the Temporal gRPC proxy — Temporal itself is visible to nobody
// else.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/gopherex/xlog"

	"github.com/graphene-ci/graphene/internal/config"
	"github.com/graphene-ci/graphene/internal/logging"
	"github.com/graphene-ci/graphene/internal/server"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println("graphene-server", version)
		return
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "graphene-server:", err)
		os.Exit(1)
	}
	cfg.Version = version
	log, err := logging.New(cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		fmt.Fprintln(os.Stderr, "graphene-server:", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	log.Info("starting", xlog.String("version", version))
	err = server.Run(ctx, cfg, log)
	stop()
	if err != nil && ctx.Err() == nil {
		log.Error("exit", xlog.Err(err))
		os.Exit(1)
	}
}
