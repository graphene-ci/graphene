package ops

import (
	"strings"

	"github.com/graphene-ci/pipeline/pkg/id"
)

// uninstallMarker ends a successful uninstall's output; otherAgentMarker
// says the machine carries a different agent — ours is not there.
const (
	uninstallMarker  = "GRAPHENE_DECOMMISSIONED"
	otherAgentMarker = "GRAPHENE_OTHER_AGENT"
)

// UninstallScript is the inverse of the install script: it removes what
// the install put on the machine — the service, the binary, its config and
// data, the PTY account — and only for THIS agent: a machine carrying
// another agent's identity is left alone. One script for both paths: the
// agent runs it detached over its session, an ssh uninstall runs it
// directly. Idempotent: a second run finds nothing and succeeds. The runc
// package stays: the machine's package manager installed it, possibly for
// others.
func UninstallScript(agentId id.AgentId) string {
	return strings.ReplaceAll(`#!/bin/sh
set -u
if [ -f /etc/graphene-agent/env ]; then
  existing=$(sed -n 's/^GRAPHENE_AGENT_ID=//p' /etc/graphene-agent/env)
  if [ -n "$existing" ] && [ "$existing" != "%AGENT_ID%" ]; then
    echo "GRAPHENE_OTHER_AGENT agent=$existing" >&2
    exit 0
  fi
fi
if command -v systemctl >/dev/null 2>&1; then
  systemctl disable --now graphene-agent >/dev/null 2>&1 || true
  rm -f /etc/systemd/system/graphene-agent.service
  systemctl daemon-reload >/dev/null 2>&1 || true
fi
if command -v pkill >/dev/null 2>&1; then
  pkill -x graphene-agent >/dev/null 2>&1 || true
fi
rm -f /usr/local/bin/graphene-agent
rm -rf /etc/graphene-agent /var/lib/graphene-agent
if id graphene-run >/dev/null 2>&1; then
  userdel -r graphene-run >/dev/null 2>&1 || deluser --remove-home graphene-run >/dev/null 2>&1 || true
fi
echo GRAPHENE_DECOMMISSIONED
`, "%AGENT_ID%", string(agentId))
}
