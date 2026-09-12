package server

import (
	"github.com/graphene-ci/graphene/internal/config"
	"os/exec"
	"strings"
	"testing"
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
