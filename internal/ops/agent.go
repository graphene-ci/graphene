// Package ops implements the side-effect boundaries of the system
// resource flows (pipeline/pkg/flow/*): machine ops against the agent
// registry and ssh, artifact ops against the blob store. Every method is
// idempotent — the flows retry them freely.
package ops

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"golang.org/x/crypto/ssh"

	"github.com/graphene-ci/graphene/internal/agents"
	"github.com/graphene-ci/graphene/internal/secrets"
	agentflow "github.com/graphene-ci/pipeline/pkg/flow/agent"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
)

// AgentOps implements the agent flow's Ops for ONE namespace: agent
// presence from the registry, ssh install for machines that already
// exist, and decommission over the live session or ssh.
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
	out, err := o.runSSH(ctx, install, script)
	if err == nil {
		return nil
	}
	// The machine already carries a DIFFERENT agent's identity: retrying
	// would never change that, and overwriting it would silently re-badge
	// a live agent. Fail loudly, once.
	if strings.Contains(out, "GRAPHENE_ALREADY_BOUND") {
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("machine %s already runs another agent (%s). "+
				"Attach it (pipeline.AttachAgent / labels) if any machine will do; "+
				"declaring a NEW name here would re-badge a live machine — to really rename it, "+
				"remove /etc/graphene-agent on the host first",
				install.Address, truncate(strings.TrimSpace(out), 128)),
			"MachineAlreadyBound", nil)
	}
	return fmt.Errorf("install script: %w: %s", err, truncate(out, 2048))
}

// decommissionWait bounds how long an agent that accepted Decommission
// may take to leave its session.
const decommissionWait = 2 * time.Minute

// Decommission takes the agent off its machine. A connected agent runs the
// uninstall itself (detached from its own process) and is done when its
// session ends; an offline one is uninstalled over the record's ssh. With
// neither, the attempt fails and the flow's policy decides: retry until
// done, or give up and mark the agent as possibly remaining.
func (o *AgentOps) Decommission(ctx context.Context, agentId id.AgentId, install *pipeline.SSHInstall) error {
	script := UninstallScript(agentId)
	if o.registry.Status(o.namespace, agentId).Connected {
		if err := o.registry.Decommission(ctx, o.namespace, agentId, script); err != nil {
			return fmt.Errorf("decommission over the session: %w", err)
		}
		deadline := time.Now().Add(decommissionWait)
		for o.registry.Status(o.namespace, agentId).Connected {
			if time.Now().After(deadline) {
				return fmt.Errorf("agent %s accepted decommission but is still connected after %s", agentId, decommissionWait)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
		return nil
	}
	if install == nil {
		return errors.New("agent is offline and the record has no ssh to uninstall it with")
	}
	out, err := o.runSSH(ctx, *install, script)
	if err != nil {
		return fmt.Errorf("uninstall over ssh: %w: %s", err, truncate(out, 2048))
	}
	if !strings.Contains(out, uninstallMarker) && !strings.Contains(out, otherAgentMarker) {
		return fmt.Errorf("uninstall over ssh did not finish: %s", truncate(out, 2048))
	}
	return nil
}

// runSSH runs a root script on the machine over ssh and returns its
// combined output. The script travels on stdin, never in argv (it may
// carry a token), into a private temp file that is removed whatever the
// script's outcome. A non-root login escalates with sudo -n (sudo would
// eat a piped stdin, hence the file).
func (o *AgentOps) runSSH(ctx context.Context, install pipeline.SSHInstall, script string) (string, error) {
	keyPEM, err := o.secrets.Get(install.KeyRef.Name)
	if err != nil {
		return "", err
	}
	signer, err := ssh.ParsePrivateKey([]byte(keyPEM))
	if err != nil {
		return "", fmt.Errorf("parse ssh key: %w", err)
	}
	hostKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(install.HostKey))
	if err != nil {
		return "", fmt.Errorf("parse host key: %w", err)
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
		return "", fmt.Errorf("dial %s: %w", address, err)
	}
	sshConn, chans, reqs, err := ssh.NewClientConn(raw, address, sshCfg)
	if err != nil {
		_ = raw.Close()
		return "", fmt.Errorf("ssh handshake %s: %w", address, err)
	}
	conn := ssh.NewClient(sshConn, chans, reqs)
	defer func() { _ = conn.Close() }()
	session, err := conn.NewSession()
	if err != nil {
		return "", err
	}
	defer func() { _ = session.Close() }()
	session.Stdin = strings.NewReader(script)
	out, err := session.CombinedOutput("sh -c '" + sshRunner + "'")
	return string(out), err
}

// sshRunner takes the script from stdin into a private temp file, runs it
// as root, and removes the file on every exit path: the install script
// carries the agent's token and must not stay on the machine.
const sshRunner = `s=$(mktemp); trap "rm -f \"$s\"" EXIT; cat > "$s"; ` +
	`if [ "$(id -u)" -ne 0 ]; then sudo -n sh "$s"; else sh "$s"; fi`

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
