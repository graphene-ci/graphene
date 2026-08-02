package ctl

import (
	"os"
	"strings"

	"github.com/graphene-ci/graphene/internal/app/install"
)

// Discover fills in what the operator did not type, so a kernel installed
// on this machine is reachable with a bare `graphen ctl definitions`.
//
// Explicit flags always win; the environment comes next (GRAPHEN_SOCKET,
// GRAPHEN_ADDRESS, GRAPHEN_TOKEN); last, the local installations are
// probed — the user scope first, since a workstation kernel is the one
// its owner means, and only then the system one.
func Discover(target Target) Target {
	if target.Socket == "" && target.Address == "" {
		target.Socket, target.Address = discoverEndpoint()
	}

	if target.Token == "" {
		target.Token = discoverToken()
	}

	return target
}

func discoverEndpoint() (socket, address string) {
	if socket := os.Getenv("GRAPHEN_SOCKET"); socket != "" {
		return socket, ""
	}

	if address := os.Getenv("GRAPHEN_ADDRESS"); address != "" {
		return "", address
	}

	for _, scope := range []install.Scope{install.ScopeUser, install.ScopeSystem} {
		layout, err := install.NewLayout(scope)
		if err != nil {
			continue
		}

		if _, err := os.Stat(layout.Socket); err == nil {
			return layout.Socket, ""
		}
	}

	return "", ""
}

func discoverToken() string {
	if token := os.Getenv("GRAPHEN_TOKEN"); token != "" {
		return token
	}

	for _, scope := range []install.Scope{install.ScopeUser, install.ScopeSystem} {
		layout, err := install.NewLayout(scope)
		if err != nil {
			continue
		}

		raw, err := os.ReadFile(layout.TokenFile)
		if err != nil {
			continue
		}

		if token := strings.TrimSpace(string(raw)); token != "" {
			return token
		}
	}

	return ""
}
