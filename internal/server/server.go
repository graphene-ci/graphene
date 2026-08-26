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
	dockerclient "github.com/docker/docker/client"
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
	"github.com/graphene-ci/graphene/internal/authz"
	"github.com/graphene-ci/graphene/internal/config"
	"github.com/graphene-ci/graphene/internal/httpapi"
	"github.com/graphene-ci/graphene/internal/infrastructure/blob"
	"github.com/graphene-ci/graphene/internal/infrastructure/s3"
	"github.com/graphene-ci/graphene/internal/logging"
	"github.com/graphene-ci/graphene/internal/materialize"
	"github.com/graphene-ci/graphene/internal/nsbundle"
	"github.com/graphene-ci/graphene/internal/nsflow"
	"github.com/graphene-ci/graphene/internal/probes"
	"github.com/graphene-ci/graphene/internal/runtimes"
	"github.com/graphene-ci/graphene/internal/secrets"
	"github.com/graphene-ci/graphene/internal/services"
	"github.com/graphene-ci/graphene/internal/telemetry"
	"github.com/graphene-ci/graphene/internal/temporalproxy"
	"github.com/graphene-ci/graphene/internal/worker"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/obs"
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

	minter := auth.NewMinter(cfg.SigningKey)
	authn := auth.New(cfg.Tokens).WithMinter(minter)
	registry := agents.New(cfg.AgentHeartbeat, log.With(xlog.String("component", "agents")))
	var secretStore *secrets.Namespaced
	if cfg.SecretsKey != "" {
		secretStore, err = secrets.NewPersistent(cfg.SecretsStore, cfg.SecretsKey, cfg.Secrets)
		if err != nil {
			return fmt.Errorf("secret store: %w", err)
		}
	} else {
		log.Warn("secrets are IN MEMORY — set secrets.key for persistence")
		secretStore = secrets.NewNamespaced(cfg.Secrets)
	}
	blobStore, err := buildBlobStore(ctx, cfg)
	if err != nil {
		return fmt.Errorf("blob store: %w", err)
	}

	codecOpt, unknownOpt, closeProxy, err := temporalproxy.New(cfg.TemporalHostPort)
	if err != nil {
		return fmt.Errorf("temporal proxy: %w", err)
	}
	stop.RegisterFnErr(func(context.Context) error { return closeProxy() })

	// The telemetry half of the door — built before the bundles: the
	// managed runner tails run containers into it.
	otlp := &services.OTLP{
		Traces:  cfg.OtelTraces,
		Logs:    cfg.OtelLogs,
		Metrics: cfg.OtelMetrics,
		Log:     log.With(xlog.String("component", "otlp")),
	}
	stop.RegisterFnErr(func(context.Context) error { otlp.Close(); return nil })

	// The server observes its OWN records. It exports through its own
	// door — the door IS the collector, so the signals of a system
	// record travel the same path, carry the same attributes and land
	// in the same backends as a run's. Without this, dimensions 3-5 of
	// every system entity are empty by construction while looking like
	// a feature that simply has no data yet.
	if cfg.OtelLogs != "" || cfg.OtelTraces != "" {
		shutdownObs, oerr := obs.Setup(ctx, obs.Config{
			Endpoint:  loopbackDoor(cfg.Listen),
			Token:     firstAdminToken(cfg),
			Insecure:  true,
			Namespace: "default",
			Role:      "server",
		})
		if oerr != nil {
			log.Warn("server telemetry not wired", xlog.Err(oerr))
		} else {
			stop.RegisterFnErr(shutdownObs)
			log.Info("server telemetry wired", xlog.String("door", loopbackDoor(cfg.Listen)))
		}
	}

	// The source-first contour: server-side builds on the same docker
	// host the managed contour drives. Best effort — no docker, no
	// materialization, everything else keeps working.
	var materializer *materialize.Materializer
	if dc, derr := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation()); derr == nil {
		materializer = &materialize.Materializer{
			Docker:   dc,
			Runtimes: runtimes.New(cfg.Runtimes),
			Registry: cfg.External,
			Token:    runTokenFor(cfg, "default"),
			Insecure: true, // TODO(tls): follow the door
			Blobs:    blobStore,
			Log:      log.With(xlog.String("component", "materialize")),
		}
	} else {
		log.Warn("materialization disabled: no docker", xlog.Err(derr))
	}

	// One runtime bundle per namespace: client, server worker, managed
	// reaper, stand sweeper — started lazily, bounded by the manager ctx.
	bundles := nsbundle.New(stop.Context(), nsbundle.Deps{
		TemporalHostPort: cfg.TemporalHostPort,
		TemporalLogger:   logging.Temporal(log),
		Registry:         registry,
		Secrets:          secretStore,
		Materializer:     materializer,
		Blobs:            blobStore,
		External:         cfg.External,
		RunTokenFor:      func(ns string) string { return runTokenFor(cfg, ns) },
		MintRunToken: func(ns, runId string) string {
			// A run's token lives as long as a run may — long enough for
			// the slowest pipeline, short enough to be worthless after.
			token, err := minter.Mint(authz.Subject{Kind: authz.SubjectServiceAccount, Name: "run/" + runId},
				ns, "run", runTokenLife)
			if err != nil {
				log.Warn("cannot mint a run token; falling back to the configured one", xlog.Err(err))
				return ""
			}
			return token
		},
		UserDataFor: userDataBuilder(cfg),
		SweepEvery:  time.Duration(cfg.SweepSeconds) * time.Second,
		ReapEvery:   time.Duration(cfg.ReapSeconds) * time.Second,
		LogSink:     otlp.ForwardLogs,
		// Trigger firings start runs through the same door logic.
		MakeRunStarter: func(b *nsbundle.Bundle) worker.RunStarter {
			return func(ctx context.Context, runId, pipelineId string, params []byte, image string, labels map[string]string, trigger string) error {
				return services.StartRunOnBundle(ctx, b, log, runId, pipelineId, params, image, labels, trigger)
			}
		},
		Log: log,
	})
	// Two namespaces exist on every installation: the SYSTEM one, which
	// holds what describes the installation itself, and the default
	// working one, which holds a first project.
	for _, name := range []string{nsflow.SystemNamespace, "default"} {
		if err := bundles.CreateNamespace(ctx, temporalClient, name, 0); err != nil {
			return fmt.Errorf("%s namespace: %w", name, err)
		}
	}
	systemBundle, err := bundles.Get(nsflow.SystemNamespace)
	if err != nil {
		return err
	}
	defaultBundle, err := bundles.Get("default")
	if err != nil {
		return err
	}
	// Registering a namespace and stopping one are the system
	// namespace's own side effects — its records decide them.
	systemBundle.Worker.SetNamespaceOps(
		func(ctx context.Context, name string, retentionDays int32) error {
			return bundles.CreateNamespace(ctx, temporalClient, name, retentionDays)
		},
		func(_ context.Context, name string) error { return bundles.Retire(name) },
	)
	bundles.SetDeclaredCheck(systemBundle.Worker.NamespaceDeclared)
	// Every namespace the durable core already holds is ADOPTED: the
	// installation predates this record, so the record catches up.
	registered, err := bundles.ListNamespaces(ctx, temporalClient)
	if err != nil {
		return fmt.Errorf("list namespaces: %w", err)
	}
	for _, name := range registered {
		// One that already has a record — living or retired — is left
		// alone: adoption is for what the records have never seen.
		if systemBundle.Worker.NamespaceKnown(ctx, name) {
			continue
		}
		if err := systemBundle.Worker.DeclareNamespace(ctx, name, nsflow.Spec{}); err != nil {
			return fmt.Errorf("namespace %s: %w", name, err)
		}
	}

	// Variables are RECORDS: the configured ones are declared into the
	// default namespace, the same way a person would declare them.
	for name, value := range cfg.Vars {
		if err := defaultBundle.Worker.DeclareVar(ctx, name, value); err != nil {
			return fmt.Errorf("var %s: %w", name, err)
		}
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
		Bundles:  bundles,
		Base:     temporalClient,
		Secrets:  secretStore,
		Version:  cfg.Version,
		Blobs:    blobStore,
		Runtimes: runtimes.New(cfg.Runtimes),
		// Authorization reads the namespace's roles and bindings — the
		// default namespace's worker is the store.
		Authz:  authz.NewResolver(systemBundle.Worker),
		Minter: minter,
		Log:    log.With(xlog.String("component", "management")),
	}

	if cfg.OIDCIssuer != "" {
		management.OIDC = &auth.OIDC{
			Issuer:        cfg.OIDCIssuer,
			Audience:      cfg.OIDCAudience,
			UsernameClaim: cfg.OIDCUsernameClaim,
			GroupsClaim:   cfg.OIDCGroupsClaim,
		}
		log.Info("identity provider wired", xlog.String("issuer", cfg.OIDCIssuer))
	} else {
		log.Info("no identity provider: this installation authenticates service accounts only")
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
	workerplanev1.RegisterRunsAPIServer(grpcServer, workerPlane)
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
		Bundles:          bundles,
		Secrets:          secretStore,
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
// loopbackDoor renders the server's own door as a dial target: it
// listens on ":7233" and reaches itself on "127.0.0.1:7233".
func loopbackDoor(listen string) string {
	_, port, err := net.SplitHostPort(listen)
	if err != nil || port == "" {
		return listen
	}
	return net.JoinHostPort("127.0.0.1", port)
}

// firstAdminToken is the credential the server presents to itself; the
// telemetry path is authenticated like every other.
func firstAdminToken(cfg config.Config) string {
	for _, t := range cfg.Tokens {
		if t.Role == "admin" {
			return t.Token
		}
	}
	return ""
}

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
GRAPHENE_AGENT_INSECURE=1
EOF
chmod 600 /etc/graphene-agent/env
if [ ! -x /usr/local/bin/graphene-agent ]; then
  # The binary comes from the same door the agent will dial.
  # TODO(tls): https once the door serves it.
  url="http://%s/agent/binary"
  auth="Authorization: Bearer %s"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL -H "$auth" "$url" -o /usr/local/bin/graphene-agent
  elif command -v wget >/dev/null 2>&1; then
    wget -q --header "$auth" -O /usr/local/bin/graphene-agent "$url"
  else
    echo "neither curl nor wget on the machine" >&2
    exit 1
  fi
  chmod 755 /usr/local/bin/graphene-agent
fi
# The container runtime the agent drives; best-effort via the distro's
# package manager when absent.
if ! command -v runc >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update -qq >/dev/null 2>&1 || true
    apt-get install -y -qq runc >/dev/null 2>&1 || true
  elif command -v dnf >/dev/null 2>&1; then
    dnf install -y -q runc >/dev/null 2>&1 || true
  elif command -v yum >/dev/null 2>&1; then
    yum install -y -q runc >/dev/null 2>&1 || true
  fi
  command -v runc >/dev/null 2>&1 || echo "WARNING: runc is still missing — machine activities will not run" >&2
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
  systemctl enable graphene-agent >/dev/null 2>&1 || true
  systemctl restart graphene-agent
else
  echo "no systemd: start /usr/local/bin/graphene-agent with /etc/graphene-agent/env yourself" >&2
fi
`, cfg.External, token, agentId, cfg.External, cfg.External, token), nil
	}
}

// runTokenLife bounds a run token: longer than the slowest pipeline,
// shorter than anything worth stealing.
const runTokenLife = 24 * time.Hour
