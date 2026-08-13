package agent

import (
	"context"
	"os/exec"
	"runtime"
	"strings"

	"github.com/graphene-ci/graphene/pkg/agent"
)

// probes are the things worth asking a fresh machine about. Each is a
// command whose output, trimmed, becomes the fact — a fact the machine
// does not have is simply absent rather than empty.
//
// This list is short on purpose. A fact exists so that something can be
// chosen by it; adding one nobody selects on is adding a field to fill in.
func probes() []struct {
	fact string
	argv []string
} {
	return []struct {
		fact string
		argv []string
	}{
		{"kernel", []string{"uname", "-r"}},
		{"docker", []string{"docker", "version", "--format", "{{.Server.Version}}"}},
	}
}

// Facts reports what this machine turned out to have.
//
// We always assume the agent came up on a bare system: nothing is
// pre-installed and nothing is implied. A wrapper like docker.Install ends
// by asking for these again, and that is how a capability appears — the
// wrapper does not write the fact itself, it makes the fact true and asks
// the machine to look again.
func Facts(ctx context.Context) agent.FactsOutput {
	facts := map[string]string{
		"os":   runtime.GOOS,
		"arch": runtime.GOARCH,
	}

	for _, probe := range probes() {
		if value, ok := ask(ctx, probe.argv); ok {
			facts[probe.fact] = value
		}
	}

	return agent.FactsOutput{Facts: facts}
}

// ask runs one probe. A probe that fails means the machine does not have
// the thing, which is an answer and not a problem.
func ask(ctx context.Context, argv []string) (string, bool) {
	//nolint:gosec // список зашит выше, снаружи сюда ничего не приходит
	out, err := exec.CommandContext(ctx, argv[0], argv[1:]...).Output()
	if err != nil {
		return "", false
	}

	value := strings.TrimSpace(string(out))
	if value == "" {
		return "", false
	}

	return value, true
}
