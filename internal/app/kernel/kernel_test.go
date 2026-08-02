package kernel_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/app/config"
	appkernel "github.com/graphene-ci/graphene/internal/app/kernel"
	"github.com/graphene-ci/graphene/internal/core/auth"
	"github.com/graphene-ci/graphene/internal/core/builtin"
	"github.com/graphene-ci/graphene/internal/infrastructure/auth/resource"
	"github.com/graphene-ci/graphene/internal/infrastructure/link"
	tlsutil "github.com/graphene-ci/graphene/internal/infrastructure/tls"
)

const (
	bootstrapToken = "bootstrap-secret"
	workerToken    = "k1-secret"
)

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()

	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// freePort reserves a port by binding and releasing it: the kernel under
// test needs an address its peer can be configured with up front.
func freePort(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}

	addr := lis.Addr().String()

	if err := lis.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}

	return addr
}

func writeFile(t *testing.T, dir, name, body string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}

	return path
}

// startKernel assembles and runs a kernel from a YAML body, returning once
// it is running.
func startKernel(ctx context.Context, t *testing.T, body string) {
	t.Helper()

	path := writeFile(t, t.TempDir(), "graphene.yaml", body)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	kern, err := appkernel.New(ctx, cfg, testLogger(t))
	if err != nil {
		t.Fatalf("assemble kernel: %v", err)
	}

	t.Cleanup(func() { _ = kern.Close() })

	go func() {
		if err := kern.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("kernel run: %v", err)
		}
	}()
}

// dialControl connects to the serving kernel the way a real client does:
// pinned CA, bearer token.
func dialControl(t *testing.T, addr, caDir, token string) graphenepbv1.ResourceServiceClient {
	t.Helper()

	pem, err := waitCA(caDir)
	if err != nil {
		t.Fatalf("read ca: %v", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		t.Fatal("ca not usable")
	}

	conn, err := link.Connect(addr, link.TCP(addr), token, &tls.Config{
		RootCAs:    pool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	return graphenepbv1.NewResourceServiceClient(conn)
}

// waitCA waits for the serving kernel to mint its certificate authority.
func waitCA(dir string) ([]byte, error) {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		pem, err := tlsutil.CACertPEM(dir)
		if err == nil {
			return pem, nil
		}

		time.Sleep(20 * time.Millisecond)
	}

	return nil, fmt.Errorf("ca never appeared in %s", dir)
}

// TestTwoKernels drives the whole assembly: one kernel holds the truth and
// serves it over TLS; the operator provisions a worker identity through the
// API; a second kernel links in with that identity and renews its lease,
// and the first kernel reports it online.
func TestTwoKernels(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	addr := freePort(t)
	controlDir := t.TempDir()

	startKernel(ctx, t, fmt.Sprintf(`
data_dir: %s
identity: { name: control }
log: { level: warn }
store: {}
blobs: {}
listen: { tcp: %q, disable_uds: true }
tls: { mode: auto }
auth: { bootstrap: { token: { inline: %s } } }
`, controlDir, addr, bootstrapToken))

	client := dialControl(t, addr, filepath.Join(controlDir, "tls"), bootstrapToken)
	waitServing(ctx, t, client)

	// The operator registers the worker kernel and provisions its identity.
	putResource(ctx, t, client, builtin.KindKernel, []string{"k1"},
		schemapb.MustStructFromGo(map[string]any{"os": "linux", "arch": "amd64"}))

	putResource(ctx, t, client, builtin.KindRole, []string{"kernel-default"},
		auth.GrantsToSpec([]auth.Grant{{
			Verbs: []auth.Verb{auth.VerbGet, auth.VerbPut},
			Kind:  builtin.KindKernelLease,
			Where: []auth.Constraint{{Path: "spec.kernel", Equal: "${principal.name}"}},
		}}))

	putResource(ctx, t, client, builtin.KindIdentity, []string{"k1"},
		schemapb.MustStructFromGo(map[string]any{
			"principal_kind": string(auth.PrincipalKernel),
			"roles":          []any{"kernel-default"},
			"token_sha256":   []any{resource.Digest(workerToken)},
		}))

	// The second kernel links in with that identity and renews its lease.
	workerDir := t.TempDir()
	caFile := writeFile(t, workerDir, "ca.crt", string(mustCA(t, filepath.Join(controlDir, "tls"))))

	startKernel(ctx, t, fmt.Sprintf(`
data_dir: %s
identity: { name: k1 }
log: { level: warn }
link:
  mode: dialout
  address: %q
  ca_file: %s
  token: { inline: %s }
lease: { ttl: 5s, renew_interval: 200ms }
`, workerDir, addr, caFile, workerToken))

	waitOnline(ctx, t, client)
}

func mustCA(t *testing.T, dir string) []byte {
	t.Helper()

	pem, err := waitCA(dir)
	if err != nil {
		t.Fatalf("ca: %v", err)
	}

	return pem
}

func waitServing(ctx context.Context, t *testing.T, client graphenepbv1.ResourceServiceClient) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := client.ListDefinitions(ctx, &graphenepbv1.ListDefinitionsRequest{}); err == nil {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal("control kernel never became reachable")
}

func putResource(ctx context.Context, t *testing.T, client graphenepbv1.ResourceServiceClient,
	kind string, path []string, spec *schemapb.StructValue,
) {
	t.Helper()

	if _, err := client.Put(ctx, &graphenepbv1.PutRequest{
		Resource: &graphenepbv1.Resource{
			Key:  &graphenepbv1.Key{Kind: kind, Path: path},
			Spec: spec,
		},
	}); err != nil {
		t.Fatalf("put %s/%v: %v", kind, path, err)
	}
}

// waitOnline waits for the lease controller to mark the worker kernel
// alive — the end-to-end proof that link, auth and controllers all work.
func waitOnline(ctx context.Context, t *testing.T, client graphenepbv1.ResourceServiceClient) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		got, err := client.Get(ctx, &graphenepbv1.GetRequest{
			Key: &graphenepbv1.Key{Kind: builtin.KindKernel, Path: []string{"k1"}},
		})
		if err == nil {
			if online, _ := got.GetResource().GetStatus().ToGo()["online"].(bool); online {
				return
			}
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("worker kernel never came online")
}
