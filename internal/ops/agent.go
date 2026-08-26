// Package ops implements the side-effect boundaries of the system
// resource flows (pipeline/pkg/flow/*): machine ops against the agent
// registry and ssh, artifact ops against the blob store. Every method is
// idempotent — the flows retry them freely.
package ops

import (
	"context"
	"fmt"
	"go.temporal.io/sdk/temporal"
	"net"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/graphene-ci/graphene/internal/agents"
	"github.com/graphene-ci/graphene/internal/secrets"
	agentflow "github.com/graphene-ci/pipeline/pkg/flow/agent"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
)

// AgentOps implements the agent flow's Ops for ONE namespace: agent
// presence from the registry, ssh install for machines that already
// exist.
type AgentOps struct {
	namespace string
	registry  *agents.Registry
	secrets   secrets.Store
	userData  func(id.AgentId) (string, error)
}

// NewAgentOps assembles the ops bound to a namespace.
func NewAgentOps(namespace string, registry *agents.Registry, store secrets.Store, userData func(id.AgentId) (string, error)) *AgentOps {
	return &AgentOps{namespace: namespace, registry: registry, secrets: store, userData: userData}
}

// UserData renders the agent install script for a machine: one script
// for both paths — a fresh VM's user-data and the ssh install.
func (o *AgentOps) UserData(agentId id.AgentId) (string, error) {
	return o.userData(agentId)
}

// AgentStatus reports whether the machine's agent is connected.
func (o *AgentOps) AgentStatus(_ context.Context, agentId id.AgentId) (agentflow.AgentStatus, error) {
	return o.registry.Status(o.namespace, agentId), nil
}

// InstallSSH runs the agent install script on an existing machine over
// ssh — the same bytes a fresh VM gets through user-data. Idempotent: the
// script itself converges.
func (o *AgentOps) InstallSSH(ctx context.Context, agentId id.AgentId, install pipeline.SSHInstall) error {
	script, err := o.userData(agentId)
	if err != nil {
		return err
	}
	keyPEM, err := o.secrets.Get(install.KeyRef.Name)
	if err != nil {
		return err
	}
	signer, err := ssh.ParsePrivateKey([]byte(keyPEM))
	if err != nil {
		return fmt.Errorf("parse ssh key: %w", err)
	}
	hostKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(install.HostKey))
	if err != nil {
		return fmt.Errorf("parse host key: %w", err)
	}
	address := install.Address
	if _, _, splitErr := net.SplitHostPort(address); splitErr != nil {
		address = net.JoinHostPort(address, "22")
	}
	sshCfg := &ssh.ClientConfig{
		User:            install.User,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.FixedHostKey(hostKey),
		// Negotiate the ALGORITHM of the pinned key — otherwise the
		// server may present a different key type and the pin
		// mismatches spuriously.
		HostKeyAlgorithms: []string{hostKey.Type()},
		Timeout:           30 * time.Second,
	}
	raw, err := (&net.Dialer{Timeout: sshCfg.Timeout}).DialContext(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("dial %s: %w", address, err)
	}
	sshConn, chans, reqs, err := ssh.NewClientConn(raw, address, sshCfg)
	if err != nil {
		_ = raw.Close()
		return fmt.Errorf("ssh handshake %s: %w", address, err)
	}
	conn := ssh.NewClient(sshConn, chans, reqs)
	defer func() { _ = conn.Close() }()
	session, err := conn.NewSession()
	if err != nil {
		return err
	}
	defer func() { _ = session.Close() }()
	// The install token is inside the script; it never appears in argv.
	// The script needs root: a non-root login escalates with sudo -n
	// (the stdin lands in a temp file first — sudo would eat the pipe).
	session.Stdin = strings.NewReader(script)
	const run = `s=$(mktemp); cat > "$s"; ` +
		`if [ "$(id -u)" -ne 0 ]; then exec sudo -n sh "$s"; else exec sh "$s"; fi`
	if out, err := session.CombinedOutput("sh -c '" + run + "'"); err != nil {
		// The machine already carries a DIFFERENT agent's identity:
		// retrying would never change that, and overwriting it would
		// silently re-badge a live agent. Fail loudly, once.
		if strings.Contains(string(out), "GRAPHENE_ALREADY_BOUND") {
			return temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("machine %s already runs another agent: %s", install.Address,
					truncate(strings.TrimSpace(string(out)), 256)),
				"MachineAlreadyBound", nil)
		}
		return fmt.Errorf("install script: %w: %s", err, truncate(string(out), 2048))
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
