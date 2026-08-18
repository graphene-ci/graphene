// Package server is the composition root of the graphene control plane:
// one gRPC door (agent sessions + grpc.health.v1 + Temporal proxy), one
// HTTP door (runs API + probes + registry proxy), the server worker with
// the system entity flows, the stand sweeper, the managed-run reaper,
// and the infra health runners. Every goroutine starts here, under one
// xshutdown manager that drains and cleans up in order.
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gopherex/xlog"
	grpcprobe "github.com/gopherex/xprobe/pkg/transport/grpc"
	"github.com/gopherex/xshutdown"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/graphene-ci/agent/pkg/agentpb"
	"github.com/graphene-ci/graphene/internal/agents"
	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/config"
	"github.com/graphene-ci/graphene/internal/httpapi"
	"github.com/graphene-ci/graphene/internal/logging"
	"github.com/graphene-ci/graphene/internal/managed"
	"github.com/graphene-ci/graphene/internal/ops"
	"github.com/graphene-ci/graphene/internal/probes"
	"github.com/graphene-ci/graphene/internal/secrets"
	"github.com/graphene-ci/graphene/internal/sweeper"
	"github.com/graphene-ci/graphene/internal/temporalproxy"
	"github.com/graphene-ci/graphene/internal/worker"
	"github.com/graphene-ci/pipeline/pkg/id"
)

// Run assembles the server from config and serves until ctx ends.
func Run(ctx context.Context, cfg config.Config, log *xlog.Logger) error {
	stop := xshutdown.New(ctx,
		xshutdown.WithTimeout(20*time.Second),
		xshutdown.WithErrorHandler(func(err error) { log.Error("shutdown", xlog.Err(err)) }),
	)

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalHostPort,
		Namespace: cfg.TemporalNamespace,
		Logger:    logging.Temporal(log),
	})
	if err != nil {
		return fmt.Errorf("temporal: %w", err)
	}
	stop.RegisterFnErr(func(context.Context) error { temporalClient.Close(); return nil })

	authn := auth.New(cfg.Tokens)
	registry := agents.New(cfg.AgentHeartbeat, log.With(xlog.String("component", "agents")))
	store := secrets.Static(cfg.Secrets)
	agentOps := ops.NewAgentOps(registry, store, userDataBuilder(cfg))
	artifactOps := ops.NewArtifactOps(cfg.BlobDir)

	codecOpt, unknownOpt, closeProxy, err := temporalproxy.New(cfg.TemporalHostPort)
	if err != nil {
		return fmt.Errorf("temporal proxy: %w", err)
	}
	stop.RegisterFnErr(func(context.Context) error { return closeProxy() })

	serverWorker, err := worker.New(worker.Deps{
		Client:       temporalClient,
		Registry:     registry,
		AgentOps:     agentOps,
		ArtifactOps:  artifactOps,
		ExternalGRPC: cfg.ExternalGRPC,
		ExternalHTTP: cfg.ExternalHTTP,
		RunToken:     firstToken(cfg, "run"),
		Log:          log.With(xlog.String("component", "worker")),
	})
	if err != nil {
		return fmt.Errorf("assemble worker: %w", err)
	}

	runner := managed.New(temporalClient, cfg.ExternalGRPC, cfg.ExternalHTTP, firstToken(cfg, "run"),
		log.With(xlog.String("component", "managed")))
	sweep := sweeper.New(temporalClient, serverWorker, log.With(xlog.String("component", "sweeper")))

	// Health: cached states fed by runners over the infra dependencies;
	// grpc.health.v1 inside (no token — balancers probe it), HTTP
	// liveness/readiness outside.
	health := probes.New(probes.Deps{
		Temporal:         temporalClient,
		Docker:           runner,
		RegistryUpstream: cfg.RegistryUpstream,
		Log:              log.With(xlog.String("component", "probes")),
	})

	grpcServer := grpc.NewServer(
		codecOpt,
		unknownOpt,
		grpc.ChainStreamInterceptor(authn.StreamInterceptor()),
		grpc.ChainUnaryInterceptor(authn.UnaryInterceptor()),
	)
	agentpb.RegisterAgentAPIServer(grpcServer, registry)
	hv1.RegisterHealthServer(grpcServer, grpcprobe.New(health.Registry))

	httpServer := &http.Server{
		Addr: cfg.ListenHTTP,
		Handler: httpapi.New(httpapi.Deps{
			Auth:             authn,
			Temporal:         temporalClient,
			RegistryUpstream: cfg.RegistryUpstream,
			Secrets:          store,
			BlobDir:          cfg.BlobDir,
			Capabilities:     serverWorker,
			Launcher:         runner,
			Health:           health.HTTPMux(),
			Log:              log.With(xlog.String("component", "http")),
		}),
		ReadHeaderTimeout: 10 * time.Second,
	}

	grpcListener, err := net.Listen("tcp", cfg.ListenGRPC)
	if err != nil {
		return err
	}
	log.Info("serving",
		xlog.String("grpc", grpcListener.Addr().String()),
		xlog.String("http", cfg.ListenHTTP))

	fatal := make(chan error, 2)
	stop.Go(func(context.Context) {
		if err := grpcServer.Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			fatal <- fmt.Errorf("grpc door: %w", err)
		}
	})
	stop.Go(func(context.Context) {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatal <- fmt.Errorf("http door: %w", err)
		}
	})
	stop.Go(func(gctx context.Context) {
		if err := serverWorker.Run(gctx); err != nil && gctx.Err() == nil {
			fatal <- fmt.Errorf("server worker: %w", err)
		}
	})
	stop.Go(func(gctx context.Context) { sweep.Tick(gctx, time.Duration(cfg.SweepSeconds)*time.Second) })
	stop.Go(func(gctx context.Context) { runner.Tick(gctx, time.Duration(cfg.ReapSeconds)*time.Second) })
	stop.Go(health.Run)

	var cause error
	select {
	case <-ctx.Done():
		cause = ctx.Err()
	case err := <-fatal:
		cause = err
	}
	// The doors must close BEFORE Stop drains: Serve/ListenAndServe are
	// tracked goroutines that only return when their server stops.
	grpcServer.GracefulStop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_ = httpServer.Shutdown(shutdownCtx)
	cancel()
	if err := stop.Stop(); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

// userDataBuilder renders the agent install script for a machine: ONE
// script for both paths — a fresh VM's user-data and the ssh install —
// because two scripts would drift. The install token is the agent token
// from the config.
func userDataBuilder(cfg config.Config) func(id.AgentId) (string, error) {
	return func(agentId id.AgentId) (string, error) {
		token := ""
		for _, t := range cfg.Tokens {
			if t.Role == "agent" && t.AgentId == string(agentId) {
				token = t.Token
				break
			}
		}
		if token == "" {
			return "", fmt.Errorf("no agent token configured for agent %q", agentId)
		}
		// The script converges: safe to run twice (ssh install after a
		// user-data boot, a re-run after a failure).
		return fmt.Sprintf(`#!/bin/sh
set -eu
mkdir -p /etc/graphene-agent
cat > /etc/graphene-agent/env <<EOF
GRAPHENE_AGENT_SERVER=%s
GRAPHENE_AGENT_TOKEN=%s
GRAPHENE_AGENT_ID=%s
GRAPHENE_AGENT_REGISTRY=%s
EOF
chmod 600 /etc/graphene-agent/env
if [ ! -x /usr/local/bin/graphene-agent ]; then
  echo "graphene-agent binary must be provisioned to /usr/local/bin/graphene-agent" >&2
  exit 1
fi
if command -v systemctl >/dev/null 2>&1; then
  cat > /etc/systemd/system/graphene-agent.service <<'UNIT'
[Unit]
Description=graphene agent
After=network-online.target

[Service]
EnvironmentFile=/etc/graphene-agent/env
ExecStart=/usr/local/bin/graphene-agent
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
UNIT
  systemctl daemon-reload
  systemctl enable --now graphene-agent
else
  echo "no systemd: start /usr/local/bin/graphene-agent with /etc/graphene-agent/env yourself" >&2
fi
`, cfg.ExternalGRPC, token, agentId, cfg.ListenHTTP), nil
	}
}

func firstToken(cfg config.Config, role string) string {
	for _, t := range cfg.Tokens {
		if t.Role == role {
			return t.Token
		}
	}
	return ""
}
