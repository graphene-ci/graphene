// Package server is the composition root of the graphene control plane:
// one gRPC door (agent sessions + Temporal proxy), one HTTP door (runs
// API + registry proxy), and the server worker with the system entity
// flows. Every goroutine of the server starts in Run.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"go.temporal.io/sdk/client"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/graphene-ci/agent/pkg/agentpb"
	"github.com/graphene-ci/graphene/internal/agents"
	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/config"
	"github.com/graphene-ci/graphene/internal/httpapi"
	"github.com/graphene-ci/graphene/internal/ops"
	"github.com/graphene-ci/graphene/internal/secrets"
	"github.com/graphene-ci/graphene/internal/temporalproxy"
	"github.com/graphene-ci/graphene/internal/worker"
	"github.com/graphene-ci/pipeline/pkg/id"
)

// Run assembles the server from config and serves until ctx ends.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalHostPort,
		Namespace: cfg.TemporalNamespace,
	})
	if err != nil {
		return fmt.Errorf("temporal: %w", err)
	}
	defer temporalClient.Close()

	authn := auth.New(cfg.Tokens)
	registry := agents.New(cfg.AgentHeartbeat, log)
	store := secrets.Static(cfg.Secrets)
	machineOps := ops.NewMachineOps(registry, store, userDataBuilder(cfg))
	artifactOps := ops.NewArtifactOps(cfg.BlobDir)

	codecOpt, unknownOpt, closeProxy, err := temporalproxy.New(cfg.TemporalHostPort)
	if err != nil {
		return fmt.Errorf("temporal proxy: %w", err)
	}
	defer func() { _ = closeProxy() }()

	grpcServer := grpc.NewServer(
		codecOpt,
		unknownOpt,
		grpc.ChainStreamInterceptor(authn.StreamInterceptor()),
		grpc.ChainUnaryInterceptor(authn.UnaryInterceptor()),
	)
	agentpb.RegisterAgentAPIServer(grpcServer, registry)

	serverWorker, err := worker.New(worker.Deps{
		Client:       temporalClient,
		Registry:     registry,
		MachineOps:   machineOps,
		ArtifactOps:  artifactOps,
		ExternalGRPC: cfg.ExternalGRPC,
		RunToken:     firstToken(cfg, "run"),
		Log:          log,
	})
	if err != nil {
		return fmt.Errorf("assemble worker: %w", err)
	}

	httpServer := &http.Server{
		Addr: cfg.ListenHTTP,
		Handler: httpapi.New(httpapi.Deps{
			Auth:             authn,
			Temporal:         temporalClient,
			RegistryUpstream: cfg.RegistryUpstream,
			Log:              log,
		}),
		ReadHeaderTimeout: 10 * time.Second,
	}

	grpcListener, err := net.Listen("tcp", cfg.ListenGRPC)
	if err != nil {
		return err
	}
	log.Info("serving", "grpc", grpcListener.Addr(), "http", cfg.ListenHTTP)

	group, gctx := errgroup.WithContext(ctx)
	group.Go(func() error { return grpcServer.Serve(grpcListener) })
	group.Go(func() error {
		err := httpServer.ListenAndServe()
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	})
	group.Go(func() error { return serverWorker.Run(gctx) })
	group.Go(func() error {
		<-gctx.Done()
		grpcServer.GracefulStop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
		return gctx.Err()
	})
	return group.Wait()
}

// userDataBuilder renders the agent install script for a machine: ONE
// script for both paths — a fresh VM's user-data and the ssh install —
// because two scripts would drift. The install token is the machine's
// agent token from the config.
func userDataBuilder(cfg config.Config) func(id.MachineId) (string, error) {
	return func(machineId id.MachineId) (string, error) {
		token := ""
		for _, t := range cfg.Tokens {
			if t.Role == "agent" && t.MachineId == string(machineId) {
				token = t.Token
				break
			}
		}
		if token == "" {
			return "", fmt.Errorf("no agent token configured for machine %q", machineId)
		}
		// The script converges: safe to run twice (ssh install after a
		// user-data boot, a re-run after a failure).
		return fmt.Sprintf(`#!/bin/sh
set -eu
mkdir -p /etc/graphene-agent
cat > /etc/graphene-agent/env <<EOF
GRAPHENE_AGENT_SERVER=%s
GRAPHENE_AGENT_TOKEN=%s
GRAPHENE_AGENT_MACHINE_ID=%s
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
`, cfg.ExternalGRPC, token, machineId, cfg.ListenHTTP), nil
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
