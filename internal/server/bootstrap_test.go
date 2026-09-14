package server

import (
	"context"
	"github.com/graphene-ci/graphene/internal/config"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBootstrapTransport(t *testing.T) {
	for _, tc := range []struct {
		name          string
		tls           bool
		url, insecure string
	}{
		{"TLS", true, "https://public.example:443/agent/binary", "false"},
		{"plaintext", false, "http://public.example:443/agent/binary", "true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := config.Config{External: "public.example:443", ExternalTLS: tc.tls, ExternalInternal: "internal:7233", Tokens: []config.Token{{Role: "agent", AgentId: "agent1", Namespace: "ns", Token: "test-token"}}}
			script, err := userDataBuilder(c, nil)("ns", "agent1")
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{tc.url, "GRAPHENE_AGENT_INSECURE=" + tc.insecure, "GRAPHENE_AGENT_SERVER=public.example:443"} {
				if !strings.Contains(script, want) {
					t.Errorf("missing %q", want)
				}
			}
			if strings.Contains(script, "internal:7233") {
				t.Fatal("internal address leaked to external bootstrap")
			}
			cmd := exec.Command("sh", "-n")
			cmd.Stdin = strings.NewReader(script)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("shell syntax: %v %s", err, out)
			}
		})
	}
}

func TestBootstrapRetriesBinaryDownload(t *testing.T) {
	for _, downloader := range []string{"curl", "wget"} {
		for _, success := range []bool{true, false} {
			name := downloader + "/exhausted"
			if success {
				name = downloader + "/recovers"
			}
			t.Run(name, func(t *testing.T) {
				if _, err := exec.LookPath(downloader); err != nil {
					t.Skip(err)
				}
				root := t.TempDir()
				bin := filepath.Join(root, "bin")
				systemd := filepath.Join(root, "systemd")
				if err := os.MkdirAll(bin, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(systemd, 0700); err != nil {
					t.Fatal(err)
				}
				binary := filepath.Join(bin, "graphene-agent")
				var attempts atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					n := attempts.Add(1)
					if r.Header.Get("Authorization") != "Bearer test-token" {
						t.Error("missing scoped token")
					}
					if _, err := os.Stat(binary); !os.IsNotExist(err) {
						t.Error("binary exposed before successful download")
					}
					if !success || n < 3 {
						http.Error(w, "temporary outage", http.StatusServiceUnavailable)
						return
					}
					_, _ = w.Write([]byte("#!/bin/sh\nexit 0\n"))
				}))
				defer server.Close()
				c := config.Config{External: strings.TrimPrefix(server.URL, "http://"), Tokens: []config.Token{{Role: "agent", AgentId: "agent1", Namespace: "ns", Token: "test-token"}}}
				script, err := userDataBuilder(c, nil)("ns", "agent1")
				if err != nil {
					t.Fatal(err)
				}
				script = strings.NewReplacer("/etc/graphene-agent", filepath.Join(root, "env"), "/usr/local/bin", bin, "/etc/systemd/system", systemd).Replace(script)
				for _, tool := range []string{downloader, "sed", "mkdir", "cat", "chmod", "mktemp", "rm", "mv"} {
					path, err := exec.LookPath(tool)
					if err != nil {
						t.Fatal(err)
					}
					if err = os.Symlink(path, filepath.Join(bin, tool)); err != nil {
						t.Fatal(err)
					}
				}
				// Avoid changing users/services or sleeping in a unit test. All filesystem
				// paths are redirected; the real downloader talks to an HTTP test server.
				for _, tool := range []string{"id", "runc", "sleep", "systemctl"} {
					if err = os.WriteFile(filepath.Join(bin, tool), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil { //nolint:gosec // Executable test stub inside t.TempDir.
						t.Fatal(err)
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, "/bin/sh")
				cmd.Env = append(os.Environ(), "PATH="+bin)
				cmd.Stdin = strings.NewReader(script)
				output, err := cmd.CombinedOutput()
				if success {
					if err != nil {
						t.Fatalf("bootstrap: %v: %s", err, output)
					}
					if attempts.Load() != 3 {
						t.Fatalf("attempts: %d", attempts.Load())
					}
					content, err := os.ReadFile(binary) //nolint:gosec // Generated binary inside t.TempDir.
					if err != nil || string(content) != "#!/bin/sh\nexit 0\n" {
						t.Fatalf("binary: %q %v", content, err)
					}
					info, err := os.Stat(binary)
					if err != nil || info.Mode().Perm() != 0755 {
						t.Fatalf("mode: %v %v", info, err)
					}
				} else {
					if err == nil {
						t.Fatal("bootstrap succeeded after failed downloads")
					}
					if attempts.Load() != 10 {
						t.Fatalf("attempts: %d", attempts.Load())
					}
					if _, err = os.Stat(binary); !os.IsNotExist(err) {
						t.Fatal("failed download installed binary")
					}
				}
				files, err := filepath.Glob(filepath.Join(bin, ".graphene-agent.*"))
				if err != nil || len(files) != 0 {
					t.Fatalf("temporary download leaked: %v %v", files, err)
				}
			})
		}
	}
}
