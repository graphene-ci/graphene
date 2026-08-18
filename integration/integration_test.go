// Package integration proves the full contour on one developer machine:
// user code (testpipeline) → graphene server (Temporal proxy, agent
// registry, server worker) → agent (exec runtime) → machine container →
// back. A real Temporal dev server underneath (set TEMPORAL_CLI to skip
// the download); the agent and the pipeline run as separate processes,
// exactly as they would in an installation.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/graphene-ci/graphene/internal/config"
	"github.com/graphene-ci/graphene/internal/server"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
)

const (
	agentToken = "test-agent-token"
	runToken   = "test-run-token"
	adminToken = "test-admin-token"
	machineId  = "vm-e2e"
	runId      = "run-e2e"
)

func TestFullContour(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test needs a dev server and process builds")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Temporal underneath — visible only to the server.
	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath:  os.Getenv("TEMPORAL_CLI"),
		ClientOptions: &client.Options{},
		LogLevel:      "error",
		SearchAttributes: temporal.NewSearchAttributes(
			entdefine.SearchAttrKind.ValueSet("seed"),
			entdefine.SearchAttrPhase.ValueSet("seed"),
		),
	})
	if err != nil {
		t.Fatalf("start dev server: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	// Binaries: the agent and the user pipeline.
	bins := t.TempDir()
	// The agent builds from its sibling checkout — its own module owns
	// its dependency graph.
	agentBin := build(t, bins, "graphene-agent", filepath.Join("..", "..", "agent"), "./cmd/graphene-agent")
	pipelineBin := build(t, bins, "testpipeline", "", "github.com/graphene-ci/graphene/integration/testpipeline")

	// The server: one gRPC door (agents + Temporal proxy), one HTTP door.
	grpcAddr := freeAddr(t)
	httpAddr := freeAddr(t)
	cfg := config.Config{
		ListenGRPC:       grpcAddr,
		ListenHTTP:       httpAddr,
		ExternalGRPC:     grpcAddr,
		TemporalHostPort: srv.FrontendHostPort(),
		Tokens: []config.Token{
			{Token: agentToken, Role: "agent", MachineId: machineId},
			{Token: runToken, Role: "run"},
			{Token: adminToken, Role: "admin"},
		},
		BlobDir: t.TempDir(),
	}
	serverCtx, stopServer := context.WithCancel(ctx)
	defer stopServer()
	serverDone := make(chan error, 1)
	go func() {
		cfg := cfg
		cfg.AgentHeartbeatSeconds = 1
		applyDefaults(&cfg)
		serverDone <- server.Run(serverCtx, cfg, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	}()
	waitHTTP(t, "http://"+httpAddr+"/healthz")

	// The agent: outbound-only, exec runtime (the "container" is the same
	// pipeline binary in the machine role).
	agentData := t.TempDir()
	agent := command(ctx, agentBin, nil,
		"GRAPHENE_AGENT_SERVER="+grpcAddr,
		"GRAPHENE_AGENT_TOKEN="+agentToken,
		"GRAPHENE_AGENT_MACHINE_ID="+machineId,
		"GRAPHENE_AGENT_INSECURE=1",
		"GRAPHENE_AGENT_DATA_DIR="+agentData,
		"GRAPHENE_AGENT_RUNTIME=exec",
	)
	if err := agent.Start(); err != nil {
		t.Fatalf("start agent: %v", err)
	}
	defer stop(agent)

	// The run worker: the same user binary in the run role, dialing the
	// server's proxy — never Temporal itself.
	markerDir := t.TempDir()
	runWorker := command(ctx, pipelineBin, nil,
		"GRAPHENE_ROLE=run",
		"GRAPHENE_RUN_ID="+runId,
		"GRAPHENE_ADDRESS="+grpcAddr,
		"GRAPHENE_TOKEN="+runToken,
		"GRAPHENE_IMAGE="+pipelineBin,
		"GRAPHENE_INSECURE=1",
	)
	if err := runWorker.Start(); err != nil {
		t.Fatalf("start run worker: %v", err)
	}
	defer stop(runWorker)

	// Start the run through the server API — the only door.
	params, _ := json.Marshal(map[string]string{"machineId": machineId, "markerDir": markerDir})
	body, _ := json.Marshal(map[string]any{"runId": runId, "workflow": "E2EPipeline", "params": json.RawMessage(params)})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+httpAddr+"/api/v1/runs", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+adminToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("start run: %s: %s", resp.Status, raw)
	}

	// The run completes: machine declared and ready (agent connected),
	// container ensured on first touch, both machine functions executed
	// inside the agent-hosted process, cleanup ran.
	awaitStatus(ctx, t, httpAddr, "Completed")

	assertFile(t, filepath.Join(markerDir, "on-machine"))
	assertFile(t, filepath.Join(markerDir, "action"))

	// The machine functions ran in the agent-hosted process, not in the
	// run worker: the recorded pid differs from the run worker's.
	raw, err := os.ReadFile(filepath.Join(markerDir, "on-machine")) //nolint:gosec // test reads its own tempdir
	if err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("pid=%d", runWorker.Process.Pid); string(raw) == want {
		t.Fatalf("machine function ran inside the run worker (pid %d)", runWorker.Process.Pid)
	}

	// Cleanup stopped the machine container: the agent's state dir drains.
	awaitTrue(ctx, t, "machine container stopped", func() bool {
		entries, err := os.ReadDir(filepath.Join(agentData, "state"))
		return err == nil && len(entries) == 0
	})

	stopServer()
	<-serverDone
}

func build(t *testing.T, dir, name, workdir, pkg string) string {
	t.Helper()
	out := filepath.Join(dir, name)
	cmd := exec.Command("go", "build", "-o", out, pkg) //nolint:gosec // test builds fixed packages
	cmd.Dir = workdir
	cmd.Env = os.Environ()
	if raw, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v: %s", pkg, err, raw)
	}
	return out
}

func command(ctx context.Context, bin string, args []string, env ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, bin, args...) //nolint:gosec // test runs binaries it just built
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd
}

func stop(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func waitHTTP(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:gosec,noctx // local test URL
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("server HTTP never came up")
}

func awaitStatus(ctx context.Context, t *testing.T, httpAddr, want string) {
	t.Helper()
	awaitTrue(ctx, t, "run status "+want, func() bool {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+httpAddr+"/api/v1/runs/"+runId, nil)
		req.Header.Set("Authorization", "Bearer "+adminToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		var status struct {
			Status string `json:"status"`
		}
		if json.NewDecoder(resp.Body).Decode(&status) != nil {
			return false
		}
		return status.Status == want
	})
}

func awaitTrue(ctx context.Context, t *testing.T, what string, cond func() bool) {
	t.Helper()
	for {
		if cond() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func assertFile(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file %s: %v", path, err)
	}
}

// applyDefaults mirrors config.Load's defaulting for a hand-built config.
func applyDefaults(cfg *config.Config) {
	if cfg.AgentHeartbeatSeconds == 0 {
		cfg.AgentHeartbeatSeconds = 15
	}
	cfg.AgentHeartbeat = time.Duration(cfg.AgentHeartbeatSeconds) * time.Second
	if cfg.TemporalNamespace == "" {
		cfg.TemporalNamespace = "default"
	}
}
