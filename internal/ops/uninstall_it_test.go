package ops

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/graphene-ci/pipeline/pkg/id"
)

// footprint is what the install script leaves on a machine, for agent db-1.
const footprint = `set -e
useradd -m -s /bin/sh graphene-run
mkdir -p /etc/graphene-agent /var/lib/graphene-agent/containers
printf 'GRAPHENE_AGENT_ID=db-1\n' > /etc/graphene-agent/env
printf '#!/bin/sh\nexec sleep infinity\n' > /usr/local/bin/graphene-agent
chmod 755 /usr/local/bin/graphene-agent
printf '[Service]\nExecStart=/usr/local/bin/graphene-agent\nRestart=always\n[Install]\nWantedBy=multi-user.target\n' > /etc/systemd/system/graphene-agent.service
systemctl daemon-reload
systemctl enable --now graphene-agent
`

// The uninstall on a real systemd machine: everything the install put
// there goes, the service stops; a machine bound to another agent is
// left alone. Opt-in: GRAPHENE_DOCKER_IT=1 and a docker daemon.
func TestUninstallScriptOnARealMachine(t *testing.T) {
	if os.Getenv("GRAPHENE_DOCKER_IT") == "" {
		t.Skip("set GRAPHENE_DOCKER_IT=1 to run against a docker daemon")
	}
	cases := []struct {
		name    string
		agent   string
		removed bool
	}{
		{name: "its own agent", agent: "db-1", removed: true},
		{name: "another agent's machine", agent: "db-2", removed: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			out, err := exec.CommandContext(ctx, "docker", "run", "-d", "--rm", "--privileged", "--cgroupns=host",
				"-v", "/sys/fs/cgroup:/sys/fs/cgroup:rw", "--tmpfs", "/run", "--tmpfs", "/run/lock",
				"--security-opt", "label=disable", "jrei/systemd-ubuntu:24.04").Output()
			if err != nil {
				t.Fatalf("docker run: %v", err)
			}
			container := strings.TrimSpace(string(out))
			t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", container).Run() })
			sh := func(script string) (string, error) {
				out, err := exec.CommandContext(ctx, "docker", "exec", "-i", container, "sh", "-c", script).CombinedOutput()
				return string(out), err
			}
			for {
				if state, _ := sh("systemctl is-system-running"); strings.TrimSpace(state) == "running" {
					break
				}
				time.Sleep(time.Second)
			}
			if out, err := sh(footprint); err != nil {
				t.Fatalf("footprint: %v\n%s", err, out)
			}
			cmd := exec.CommandContext(ctx, "docker", "exec", "-i", container, "sh", "-c", sshRunner)
			cmd.Stdin = strings.NewReader(UninstallScript(agentIdOf(c.agent)))
			res, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("uninstall: %v\n%s", err, res)
			}
			marker := otherAgentMarker
			if c.removed {
				marker = uninstallMarker
			}
			if !strings.Contains(string(res), marker) {
				t.Fatalf("output %q, want %s", res, marker)
			}
			left, _ := sh(`for p in /usr/local/bin/graphene-agent /etc/graphene-agent /var/lib/graphene-agent /etc/systemd/system/graphene-agent.service; do [ -e "$p" ] && echo "$p"; done; id graphene-run >/dev/null 2>&1 && echo graphene-run; systemctl is-active graphene-agent; ls /tmp`)
			if c.removed && strings.TrimSpace(left) != "inactive" {
				t.Fatalf("left on the machine:\n%s", left)
			}
			if !c.removed && !strings.Contains(left, "/usr/local/bin/graphene-agent") {
				t.Fatalf("another agent's machine was touched:\n%s", left)
			}
		})
	}
}

func agentIdOf(s string) id.AgentId { return id.AgentId(s) }
