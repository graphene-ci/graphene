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

	agentpb "github.com/graphene-ci/agent/pkg/proto/agent/v1"
	"github.com/graphene-ci/graphene/internal/agents"
	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/config"
	"github.com/graphene-ci/graphene/internal/httpapi"
	"github.com/graphene-ci/graphene/internal/infrastructure/blob"
	"github.com/graphene-ci/graphene/internal/infrastructure/s3"
	"github.com/graphene-ci/graphene/internal/logging"
	"github.com/graphene-ci/graphene/internal/nsbundle"
	"github.com/graphene-ci/graphene/internal/probes"
	"github.com/graphene-ci/graphene/internal/secrets"
	"github.com/graphene-ci/graphene/internal/services"
	"github.com/graphene-ci/graphene/internal/temporalproxy"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
	"github.com/graphene-ci/pipeline/pkg/id"
	workerplanev1 "github.com/graphene-ci/pipeline/pkg/proto/workerplane/v1"
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
	secretStore := secrets.NewNamespaced(cfg.Secrets)

	blobStore, err := buildBlobStore(ctx, cfg)
	if err != nil {
		return fmt.Errorf("blob store: %w", err)
	}

	codecOpt, unknownOpt, closeProxy, err := temporalproxy.New(cfg.TemporalHostPort)
	if err != nil {
		return fmt.Errorf("temporal proxy: %w", err)
	}
	stop.RegisterFnErr(func(context.Context) error { return closeProxy() })

	// One runtime bundle per namespace: client, server worker, managed
	// reaper, stand sweeper — started lazily, bounded by the manager ctx.
	bundles := nsbundle.New(stop.Context(), nsbundle.Deps{
		TemporalHostPort: cfg.TemporalHostPort,
		TemporalLogger:   logging.Temporal(log),
		Registry:         registry,
		Secrets:          secretStore,
		Blobs:            blobStore,
		ExternalGRPC:     cfg.ExternalGRPC,
		RunTokenFor:      func(ns string) string { return runTokenFor(cfg, ns) },
		UserDataFor:      userDataBuilder(cfg),
		SweepEvery:       time.Duration(cfg.SweepSeconds) * time.Second,
		ReapEvery:        time.Duration(cfg.ReapSeconds) * time.Second,
		Log:              log,
	})
	// The default namespace exists on every installation.
	if err := bundles.CreateNamespace(ctx, temporalClient, "default", 0); err != nil {
		return fmt.Errorf("default namespace: %w", err)
	}
	defaultBundle, err := bundles.Get("default")
	if err != nil {
		return err
	}

	// Health: cached states fed by runners over the infra dependencies;
	// grpc.health.v1 inside (no token — balancers probe it), HTTP
	// liveness/readiness outside.
	health := probes.New(probes.Deps{
		Temporal:         temporalClient,
		Docker:           defaultBundle.Runner,
		RegistryUpstream: cfg.RegistryUpstream,
		Log:              log.With(xlog.String("component", "probes")),
	})

	management := &services.Management{
		Bundles: bundles,
		Base:    temporalClient,
		Secrets: secretStore,
		Log:     log.With(xlog.String("component", "management")),
	}
	workerPlane := &services.WorkerPlane{
		Bundles: bundles,
		Secrets: secretStore,
		Blobs:   blobStore,
		Log:     log.With(xlog.String("component", "workerplane")),
	}

	grpcServer := grpc.NewServer(
		codecOpt,
		unknownOpt,
		grpc.ChainStreamInterceptor(authn.StreamInterceptor()),
		grpc.ChainUnaryInterceptor(authn.UnaryInterceptor()),
	)
	agentpb.RegisterAgentAPIServer(grpcServer, registry)
	hv1.RegisterHealthServer(grpcServer, grpcprobe.New(health.Registry))
	workerplanev1.RegisterSecretsAPIServer(grpcServer, workerPlane)
	workerplanev1.RegisterCapabilitiesAPIServer(grpcServer, workerPlane)
	workerplanev1.RegisterBlobsAPIServer(grpcServer, workerPlane)
	managementv1.RegisterRunsAPIServer(grpcServer, management)
	managementv1.RegisterResourcesAPIServer(grpcServer, management)
	managementv1.RegisterNamespacesAPIServer(grpcServer, management)
	managementv1.RegisterSecretsAPIServer(grpcServer, management)

	httpServer := &http.Server{
		Addr: cfg.ListenHTTP,
		Handler: httpapi.New(httpapi.Deps{
			Auth:             authn,
			RegistryUpstream: cfg.RegistryUpstream,
			Health:           health.HTTPMux(),
			Log:              log.With(xlog.String("component", "http")),
		}),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// The browser port: the management plane over ConnectRPC
	// (connect + gRPC-web + gRPC on one handler; unencrypted HTTP/2 for
	// the gRPC protocol without TLS).
	connectProtocols := new(http.Protocols)
	connectProtocols.SetHTTP1(true)
	connectProtocols.SetUnencryptedHTTP2(true)
	connectServer := &http.Server{
		Addr:              cfg.ListenConnect,
		Handler:           services.ConnectHandler(management, authn),
		Protocols:         connectProtocols,
		ReadHeaderTimeout: 10 * time.Second,
	}

	grpcListener, err := net.Listen("tcp", cfg.ListenGRPC)
	if err != nil {
		return err
	}
	log.Info("serving",
		xlog.String("grpc", grpcListener.Addr().String()),
		xlog.String("http", cfg.ListenHTTP),
		xlog.String("connect", cfg.ListenConnect))

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
	stop.Go(func(context.Context) {
		if err := connectServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatal <- fmt.Errorf("connect port: %w", err)
		}
	})
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
	_ = connectServer.Shutdown(shutdownCtx)
	cancel()
	if err := stop.Stop(); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

// buildBlobStore picks the configured byte store.
func buildBlobStore(ctx context.Context, cfg config.Config) (blob.Store, error) {
	switch cfg.BlobBackend {
	case "s3":
		return s3.New(ctx, s3.Options{
			Endpoint:  cfg.BlobS3.Endpoint,
			Bucket:    cfg.BlobS3.Bucket,
			AccessKey: cfg.BlobS3.AccessKey,
			SecretKey: cfg.BlobS3.SecretKey,
			UseSSL:    cfg.BlobS3.UseSSL,
		})
	default:
		return blob.NewFS(cfg.BlobDir), nil
	}
}

// runTokenFor returns the run token of a namespace.
func runTokenFor(cfg config.Config, namespace string) string {
	for _, t := range cfg.Tokens {
		if t.Role == "run" && (t.Namespace == namespace || t.Namespace == "*") {
			return t.Token
		}
	}
	return ""
}

// userDataBuilder renders the agent install script for a machine: ONE
// script for both paths — a fresh VM's user-data and the ssh install —
// because two scripts would drift. The install token is the agent token
// from the config.
func userDataBuilder(cfg config.Config) func(string, id.AgentId) (string, error) {
	return func(namespace string, agentId id.AgentId) (string, error) {
		token := ""
		for _, t := range cfg.Tokens {
			if t.Role == "agent" && t.AgentId == string(agentId) && (t.Namespace == namespace || t.Namespace == "*") {
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
