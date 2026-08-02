// Package install puts a kernel under systemd: it lays out the files a
// service needs and manages the unit.
//
// Two scopes, because two situations are real: a machine where the kernel
// is infrastructure (system scope, FHS paths, root), and a workstation
// where someone wants to try it without touching the system (user scope,
// XDG paths, no privileges at all).
package install

import (
	"fmt"
	"os"
	"path/filepath"
)

// Scope selects where everything goes.
type Scope string

const (
	// ScopeSystem installs for the machine: /etc, /var/lib, /usr/local/bin.
	ScopeSystem Scope = "system"
	// ScopeUser installs for the current user: XDG directories, no root.
	ScopeUser Scope = "user"
)

// Layout is where every artifact of an installation lives.
type Layout struct {
	Scope Scope
	// Binary is the executable the unit runs.
	Binary string
	// Config is the kernel configuration file.
	Config string
	// Data is the kernel's data directory (store, blobs, tls).
	Data string
	// Unit is the systemd unit file.
	Unit string
	// Socket is the API socket the unit exposes.
	Socket string
	// TokenFile holds the bootstrap credential.
	TokenFile string
}

// UnitName is the service name in both scopes.
const UnitName = "graphen-kernel.service"

// NewLayout computes the paths for a scope.
func NewLayout(scope Scope) (Layout, error) {
	if scope == ScopeSystem {
		return Layout{
			Scope:     ScopeSystem,
			Binary:    "/usr/local/bin/graphen",
			Config:    "/etc/graphen/kernel.yaml",
			Data:      "/var/lib/graphen",
			Unit:      filepath.Join("/etc/systemd/system", UnitName),
			Socket:    "/run/graphen/kernel.sock",
			TokenFile: "/etc/graphen/bootstrap.token",
		}, nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return Layout{}, fmt.Errorf("install: home directory: %w", err)
	}

	config := xdgDir("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	data := xdgDir("XDG_DATA_HOME", filepath.Join(home, ".local", "share"))
	runtime := xdgDir("XDG_RUNTIME_DIR", filepath.Join(data, "graphen", "run"))

	return Layout{
		Scope:     ScopeUser,
		Binary:    filepath.Join(home, ".local", "bin", "graphen"),
		Config:    filepath.Join(config, "graphen", "kernel.yaml"),
		Data:      filepath.Join(data, "graphen"),
		Unit:      filepath.Join(config, "systemd", "user", UnitName),
		Socket:    filepath.Join(runtime, "graphen", "kernel.sock"),
		TokenFile: filepath.Join(config, "graphen", "bootstrap.token"),
	}, nil
}

func xdgDir(env, fallback string) string {
	if value := os.Getenv(env); value != "" {
		return value
	}

	return fallback
}
