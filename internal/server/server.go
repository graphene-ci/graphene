// Package server is the composition root of the graphene control plane:
// ONE door (cmux splits the single listener by content: gRPC for agent
// sessions, grpc.health.v1, the Temporal proxy, and the worker and
// management planes; plain HTTP for the ConnectRPC browser surface,
// probes, and the registry proxy), the server worker with the system
// entity flows, the stand sweeper, the managed-run reaper, and the
// infra health runners. Every goroutine starts here, under one
// xshutdown manager that drains and cleans up in order.
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gopherex/xlog"
	grpcprobe "github.com/gopherex/xprobe/pkg/transport/grpc"
	"github.com/gopherex/xshutdown"
	"github.com/soheilhy/cmux"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"

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
	"github.com/graphene-ci/graphene/internal/telemetry"
	"github.com/graphene-ci/graphene/internal/temporalproxy"
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
		External:         cfg.External,
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
	observe := &services.Observe{
		Bundles:    bundles,
		Management: management,
		Log:        log.With(xlog.String("component", "observe")),
	}
	telemetryHTTP := &http.Client{Timeout: 30 * time.Second}
	if cfg.QueryLogs != "" {
		observe.LogsBackend = &telemetry.LogsQL{Base: cfg.QueryLogs, Client: telemetryHTTP}
	}
	if cfg.QueryMetrics != "" {
		observe.MetricsBackend = &telemetry.PromQL{Base: cfg.QueryMetrics, Client: telemetryHTTP}
	}
	if cfg.QueryTraces != "" {
		observe.TracesBackend = &telemetry.Jaeger{Base: cfg.QueryTraces, Client: telemetryHTTP}
	}
	otlp := &services.OTLP{
		Traces:  cfg.OtelTraces,
		Logs:    cfg.OtelLogs,
		Metrics: cfg.OtelMetrics,
		Log:     log.With(xlog.String("component", "otlp")),
	}
	stop.RegisterFnErr(func(context.Context) error { otlp.Close(); return nil })

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
	workerplanev1.RegisterEventsAPIServer(grpcServer, workerPlane)
	workerplanev1.RegisterManifestAPIServer(grpcServer, workerPlane)
	// The standard OTLP surface behind the same door: exporters in
	// workers and agents point at the address they already know.
	coltracepb.RegisterTraceServiceServer(grpcServer, otlp)
	collogspb.RegisterLogsServiceServer(grpcServer, otlp.OTLPLogs())
	colmetricspb.RegisterMetricsServiceServer(grpcServer, otlp.OTLPMetrics())

	// The plain-HTTP half of the door: the ConnectRPC management
	// surface (its own path prefixes), with probes and the registry
	// proxy behind everything else. Unencrypted HTTP/2 stays on so a
	// TLS proxy in front can speak h2c for every protocol at once.
	rootMux := http.NewServeMux()
	services.MountConnect(rootMux, management, observe, authn)
	rootMux.Handle("/", httpapi.New(httpapi.Deps{
		Auth:             authn,
		RegistryUpstream: cfg.RegistryUpstream,
		Health:           health.HTTPMux(),
		Log:              log.With(xlog.String("component", "http")),
	}))
	httpProtocols := new(http.Protocols)
	httpProtocols.SetHTTP1(true)
	httpProtocols.SetUnencryptedHTTP2(true)
	httpServer := &http.Server{
		Handler:           rootMux,
		Protocols:         httpProtocols,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// ONE listener; cmux splits it by content. gRPC is matched by its
	// content-type header (the SendSettings variant, so clients that
	// wait for the server preface don't hang); everything else is HTTP.
	rootListener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	mux := cmux.New(rootListener)
	grpcListener := mux.MatchWithWriters(
		cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpListener := mux.Match(cmux.Any())
	log.Info("serving", xlog.String("door", rootListener.Addr().String()))

	// closing suppresses the accept errors every listener close causes.
	var closing atomic.Bool
	fatal := make(chan error, 3)
	report := func(err error) {
		if err != nil && !closing.Load() {
			fatal <- err
		}
	}
	stop.Go(func(context.Context) {
		if err := grpcServer.Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			report(fmt.Errorf("grpc door: %w", err))
		}
	})
	stop.Go(func(context.Context) {
		if err := httpServer.Serve(httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			report(fmt.Errorf("http door: %w", err))
		}
	})
	stop.Go(func(context.Context) {
		if err := mux.Serve(); err != nil {
			report(fmt.Errorf("door: %w", err))
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
	// The door must close BEFORE Stop drains: the Serve goroutines are
	// tracked and only return when their server stops.
	closing.Store(true)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_ = httpServer.Shutdown(shutdownCtx)
	cancel()
	grpcServer.Stop()
	_ = rootListener.Close()
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
`, cfg.External, token, agentId, cfg.External), nil
	}
}
