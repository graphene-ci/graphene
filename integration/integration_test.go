// Package integration proves the full contour on one developer machine:
// user code (testpipeline, surface v2) → graphene server (Temporal
// proxy, agent registry, server worker, HTTP door, sweeper) → agent
// (exec runtime) → per-(agent × run) container → back. A real Temporal
// dev server underneath (set TEMPORAL_CLI to skip the download); the
// agent and the pipeline run as separate processes, exactly as they
// would in an installation.
package integration

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/gopherex/xlog"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/graphene-ci/graphene/internal/config"
	"github.com/graphene-ci/graphene/internal/server"
	"github.com/graphene-ci/pipeline/pkg/wire"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
)

const (
	agentToken = "test-agent-token"
	runToken   = "test-run-token"
	adminToken = "test-admin-token"
	agentId    = "vm-e2e"
	runId      = "run-e2e"
)

func TestFullContour(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test needs a dev server and process builds")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	// Temporal underneath — visible only to the server. The tree lives
	// in search attributes: owner, keep-until.
	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath:  os.Getenv("TEMPORAL_CLI"),
		ClientOptions: &client.Options{},
		LogLevel:      "warn",
		SearchAttributes: temporal.NewSearchAttributes(
			entdefine.SearchAttrKind.ValueSet("seed"),
			entdefine.SearchAttrPhase.ValueSet("seed"),
			entdefine.SearchAttrLabels.ValueSet([]string{"seed=seed"}),
			wire.SearchAttrOwner.ValueSet("seed"),
			wire.SearchAttrKeepUntil.ValueSet(time.Now()),
		),
	})
	if err != nil {
		t.Fatalf("start dev server: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	// Binaries: the agent from its sibling checkout, the pipeline here.
	if _, err := os.Stat(filepath.Join("..", "..", "agent", "go.mod")); err != nil {
		t.Skip("agent sibling checkout not present")
	}
	bins := t.TempDir()
	agentBin := build(t, bins, "graphene-agent", filepath.Join("..", "..", "agent"), "./cmd/graphene-agent")
	pipelineBin := build(t, bins, "testpipeline", "", "github.com/graphene-ci/graphene/integration/testpipeline")

	// The server: one gRPC door (agents + Temporal proxy), one HTTP door.
	doorAddr := freeAddr(t)
	blobDir := t.TempDir()
	cfg := config.Config{
		Listen:           doorAddr,
		TemporalHostPort: srv.FrontendHostPort(),
		Tokens: []config.Token{
			{Token: agentToken, Role: "agent", Namespace: "default", AgentId: agentId},
			{Token: runToken, Role: "run", Namespace: "default"},
			{Token: adminToken, Role: "admin", Namespace: "*"},
		},
		BlobDir:               blobDir,
		AgentHeartbeatSeconds: 1,
		SweepSeconds:          2,
		ReapSeconds:           2,
	}
	applyDefaults(&cfg)
	serverCtx, stopServer := context.WithCancel(ctx)
	defer stopServer()
	serverLog := io.Writer(os.Stderr)
	if dir := os.Getenv("GRAPHENE_E2E_LOGS"); dir != "" {
		if f, err := os.Create(filepath.Join(dir, "server.log")); err == nil { //nolint:gosec // test log dir
			serverLog = f
		}
	}
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Run(serverCtx, cfg, xlog.NewConsole(xlog.WithSink(serverLog)))
	}()
	waitHTTP(t, "http://"+doorAddr+"/healthz")

	// The agent: outbound-only, exec runtime (the "container" is the same
	// pipeline binary in the machine role).
	agentData := t.TempDir()
	if dir := os.Getenv("GRAPHENE_E2E_LOGS"); dir != "" {
		agentData = filepath.Join(dir, "agent-data")
		_ = os.MkdirAll(agentData, 0o750) //nolint:gosec // test log dir named by the runner
	}
	agent := command(ctx, agentBin, nil,
		"GRAPHENE_AGENT_SERVER="+doorAddr,
		"GRAPHENE_AGENT_TOKEN="+agentToken,
		"GRAPHENE_AGENT_ID="+agentId,
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
	if dir := os.Getenv("GRAPHENE_E2E_LOGS"); dir != "" {
		markerDir = filepath.Join(dir, "markers")
		_ = os.MkdirAll(markerDir, 0o750) //nolint:gosec // test log dir named by the runner
	}
	runWorker := command(ctx, pipelineBin, nil,
		"GRAPHENE_ROLE=run",
		"GRAPHENE_RUN_ID="+runId,
		"GRAPHENE_ADDRESS="+doorAddr,
		"GRAPHENE_NAMESPACE=default",
		"GRAPHENE_TOKEN="+runToken,
		"GRAPHENE_IMAGE="+pipelineBin,
		"GRAPHENE_INSECURE=1",
	)
	if err := runWorker.Start(); err != nil {
		t.Fatalf("start run worker: %v", err)
	}
	defer stop(runWorker)

	// Start the run through the server API — the only door.
	paramsJSON, _ := json.Marshal(map[string]any{
		"agentId":   agentId,
		"markerDir": markerDir,
		"keep":      3 * time.Second,
	})
	params := base64.StdEncoding.EncodeToString(paramsJSON)
	// The run starts through the CONNECT port — the browser-facing
	// management plane speaks plain JSON POSTs.
	body, _ := json.Marshal(map[string]any{
		"runId":    runId,
		"pipeline": "e2e",
		"params":   params,
	})
	resp := doJSON(ctx, t, http.MethodPost,
		"http://"+doorAddr+"/graphene.management.v1.RunsAPI/StartRun", adminToken, body)
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Fatalf("start run: %s: %s", resp.Status, raw)
	}
	_ = resp.Body.Close()

	//nolint:errcheck,gosec // debug hook, best effort
	if dir := os.Getenv("GRAPHENE_E2E_LOGS"); dir != "" {
		go func() {
			time.Sleep(45 * time.Second)
			f, _ := os.Create(filepath.Join(dir, "history.log"))
			defer f.Close()
			for _, kind := range []enums.TaskQueueType{enums.TASK_QUEUE_TYPE_WORKFLOW, enums.TASK_QUEUE_TYPE_ACTIVITY} {
				tq, tqErr := srv.Client().DescribeTaskQueue(ctx, "agent/"+agentId+"/run/run/"+runId, kind)
				fmt.Fprintf(f, "TASKQUEUE %v err=%v %v\n", kind, tqErr, tq.String())
			}
			it := srv.Client().GetWorkflowHistory(ctx, "run/"+runId, "", false, 0)
			for it.HasNext() {
				ev, err := it.Next()
				if err != nil {
					fmt.Fprintln(f, "ERR", err)
					return
				}
				fmt.Fprintf(f, "%d %s %s\n", ev.GetEventId(), ev.GetEventType(), ev.String()[:min(300, len(ev.String()))])
			}
		}()
	}

	// The run completes: agent declared and connected, container ensured
	// on first touch, activities executed inside the agent-hosted
	// process, capability published and required, selection fanned out,
	// artifact uploaded and attached, stand transfer done, cleanup ran.
	awaitStatus(ctx, t, doorAddr, "Completed")

	// Labels: the agent record is selectable by its user label through
	// the visibility pushdown, and carries the system marker of the run
	// that created it.
	listBody, _ := json.Marshal(map[string]any{
		"selector": map[string]any{"kind": "agent", "labels": map[string]string{"role": "e2e"}},
	})
	listResp := doJSON(ctx, t, http.MethodPost,
		"http://"+doorAddr+"/graphene.management.v1.ResourcesAPI/List", adminToken, listBody)
	var listOut struct {
		Resources []struct {
			Ref    string            `json:"ref"`
			Labels map[string]string `json:"labels"`
		} `json:"resources"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listOut); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	_ = listResp.Body.Close()
	if len(listOut.Resources) != 1 || listOut.Resources[0].Ref != "agent/"+agentId {
		t.Fatalf("label selection: want [agent/%s], got %+v", agentId, listOut.Resources)
	}
	if got := listOut.Resources[0].Labels[wire.LabelRun]; got != runId {
		t.Fatalf("system label %s: want %q, got %q (labels %v)",
			wire.LabelRun, runId, got, listOut.Resources[0].Labels)
	}

	for _, marker := range []string{"on-machine", "action", "fan-out"} {
		if _, err := os.Stat(filepath.Join(markerDir, marker)); err != nil { //nolint:gosec // test dir
			t.Fatalf("expected marker %s: %v", marker, err)
		}
	}
	// The machine code ran in the agent-hosted process, not the worker.
	raw, err := os.ReadFile(filepath.Join(markerDir, "on-machine")) //nolint:gosec // test tempdir
	if err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("pid=%d", runWorker.Process.Pid); string(raw) == want {
		t.Fatalf("machine code ran inside the run worker (pid %d)", runWorker.Process.Pid)
	}

	// The artifact's bytes reached the blob store; the stand TTL then
	// collects the record and its finalizer deletes the bytes.
	awaitTrue(ctx, t, "stand TTL sweep of the artifact", func() bool {
		left, _ := filepath.Glob(filepath.Join(blobDir, "default", "blobs", "*"))
		return len(left) == 0
	})

	// Cleanup stopped the machine container: the agent's state drains.
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
	out := os.Stderr
	if dir := os.Getenv("GRAPHENE_E2E_LOGS"); dir != "" {
		//nolint:gosec // test log dir
		f, err := os.OpenFile(filepath.Join(dir, filepath.Base(bin)+".log"),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err == nil {
			out = f
		}
	}
	cmd.Stdout = out
	cmd.Stderr = out
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

func doJSON(ctx context.Context, t *testing.T, method, url, token string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
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

func awaitStatus(ctx context.Context, t *testing.T, doorAddr, want string) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"runId": runId})
	awaitTrue(ctx, t, "run status "+want, func() bool {
		resp := doJSON(ctx, t, http.MethodPost,
			"http://"+doorAddr+"/graphene.management.v1.RunsAPI/GetRun", adminToken, body)
		defer func() { _ = resp.Body.Close() }()
		var status struct {
			Status string `json:"status"`
		}
		if json.NewDecoder(resp.Body).Decode(&status) != nil {
			return false
		}
		if status.Status == "Failed" || status.Status == "Terminated" {
			t.Fatalf("run reached %s", status.Status)
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

// applyDefaults mirrors config.Load's defaulting for a hand-built config.
func applyDefaults(cfg *config.Config) {
	if cfg.External == "" {
		cfg.External = cfg.Listen
	}
	if cfg.AgentHeartbeatSeconds == 0 {
		cfg.AgentHeartbeatSeconds = 15
	}
	cfg.AgentHeartbeat = time.Duration(cfg.AgentHeartbeatSeconds) * time.Second
	if cfg.TemporalNamespace == "" {
		cfg.TemporalNamespace = "default"
	}
	if cfg.SweepSeconds == 0 {
		cfg.SweepSeconds = 30
	}
	if cfg.ReapSeconds == 0 {
		cfg.ReapSeconds = 10
	}
}
