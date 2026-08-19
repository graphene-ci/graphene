package integration

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/gopherex/xlog"
	"io"
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
	"github.com/graphene-ci/pipeline/pkg/wire"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
)

// TestManagedRun proves the managed contour: the user pushes an image,
// the SERVER launches the run worker container, and reaps it when the
// run is over and nothing runs on its queue. Needs a docker daemon —
// skipped without one.
func TestManagedRun(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	if err := exec.Command("docker", "version").Run(); err != nil {
		t.Skipf("no docker daemon: %v", err)
	}
	if _, err := os.Stat(filepath.Join("..", "..", "agent", "go.mod")); err != nil {
		t.Skip("agent sibling checkout not present")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath:  os.Getenv("TEMPORAL_CLI"),
		ClientOptions: &client.Options{},
		LogLevel:      "error",
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

	// A static pipeline binary becomes a FROM-scratch worker image.
	bins := t.TempDir()
	buildCtx := t.TempDir()
	cmd := exec.Command("go", "build", "-o", filepath.Join(buildCtx, "pipeline"), "github.com/graphene-ci/graphene/integration/testpipeline") //nolint:gosec // fixed package
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if raw, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v: %s", err, raw)
	}
	if err := os.WriteFile(filepath.Join(buildCtx, "Dockerfile"),
		[]byte("FROM scratch\nCOPY pipeline /pipeline\nENTRYPOINT [\"/pipeline\"]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	const image = "graphene-e2e-run:latest"
	if raw, err := exec.Command("docker", "build", "-t", image, buildCtx).CombinedOutput(); err != nil { //nolint:gosec // fixed args
		t.Fatalf("docker build: %v: %s", err, raw)
	}
	agentBin := build(t, bins, "graphene-agent", filepath.Join("..", "..", "agent"), "./cmd/graphene-agent")

	doorAddr := freeAddr(t)
	const managedRunId = "run-managed"
	const managedAgent = "vm-managed"
	// Managed containers are named per namespace by the runner.
	const managedContainer = "graphene-run-default-" + managedRunId
	cfg := config.Config{
		Listen:           doorAddr,
		TemporalHostPort: srv.FrontendHostPort(),
		Tokens: []config.Token{
			{Token: agentToken, Role: "agent", Namespace: "default", AgentId: managedAgent},
			{Token: runToken, Role: "run", Namespace: "default"},
			{Token: adminToken, Role: "admin", Namespace: "*"},
		},
		BlobDir:               t.TempDir(),
		AgentHeartbeatSeconds: 1,
		SweepSeconds:          2,
		ReapSeconds:           2,
	}
	applyDefaults(&cfg)
	serverCtx, stopServer := context.WithCancel(ctx)
	defer stopServer()
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Run(serverCtx, cfg, xlog.NewConsole(xlog.WithSink(os.Stderr)))
	}()
	waitHTTP(t, "http://"+doorAddr+"/healthz")

	agent := command(ctx, agentBin, nil,
		"GRAPHENE_AGENT_SERVER="+doorAddr,
		"GRAPHENE_AGENT_TOKEN="+agentToken,
		"GRAPHENE_AGENT_ID="+managedAgent,
		"GRAPHENE_AGENT_INSECURE=1",
		"GRAPHENE_AGENT_DATA_DIR="+t.TempDir(),
		"GRAPHENE_AGENT_RUNTIME=exec",
	)
	if err := agent.Start(); err != nil {
		t.Fatalf("start agent: %v", err)
	}
	defer stop(agent)

	// The agent's exec runtime runs the LOCAL binary path as the machine
	// container; the run worker is the docker image the server launches.
	markerDir := t.TempDir()
	if err := os.Chmod(markerDir, 0o777); err != nil { //nolint:gosec // scratch container writes here
		t.Fatal(err)
	}
	paramsJSON, _ := json.Marshal(map[string]any{
		"agentId":   managedAgent,
		"markerDir": markerDir,
		"keep":      time.Second,
	})
	body, _ := json.Marshal(map[string]any{
		"runId":    managedRunId,
		"pipeline": "e2e",
		"params":   base64.StdEncoding.EncodeToString(paramsJSON),
		"image":    image,
	})
	resp := doJSON(ctx, t, http.MethodPost,
		"http://"+doorAddr+"/graphene.management.v1.RunsAPI/StartRun", adminToken, body)
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Fatalf("start managed run: %s: %s", resp.Status, raw)
	}
	_ = resp.Body.Close()

	// The container the server launched must exist...
	awaitTrue(ctx, t, "managed container running", func() bool {
		return exec.Command("docker", "inspect", managedContainer).Run() == nil
	})

	// The image knows nothing of the machine role binary path — machine
	// activities in this scenario run through the agent's exec runtime,
	// whose image ref is the CONTAINER's /pipeline path... which does not
	// exist on the host. The managed scenario therefore only drives the
	// run workflow up to its first machine touch and proves the LAUNCH
	// and the REAP; the full machine story is TestFullContour's job.
	// Terminate the run and watch the reaper collect the container.
	if err := srv.Client().TerminateWorkflow(ctx, "run/"+managedRunId, "", "managed e2e: launch and reap proven"); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	awaitTrue(ctx, t, "managed container reaped", func() bool {
		return exec.Command("docker", "inspect", managedContainer).Run() != nil
	})

	stopServer()
	<-serverDone
}
