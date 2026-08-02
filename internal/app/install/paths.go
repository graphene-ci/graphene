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

const (
	// UnitName is the service name in both scopes.
	UnitName = "graphene-kernel.service"

	systemUnitDir   = "/etc/systemd/system"
	systemConfigDir = "/etc/graphene"
)

// NewLayout computes the paths for a scope.
func NewLayout(scope Scope) (Layout, error) {
	if scope == ScopeSystem {
		return Layout{
			Scope:     ScopeSystem,
			Binary:    "/usr/local/bin/graphene",
			Config:    systemConfigDir + "/kernel.yaml",
			Data:      "/var/lib/graphene",
			Unit:      systemUnitDir + "/" + UnitName,
			Socket:    "/run/graphene/kernel.sock",
			TokenFile: systemConfigDir + "/bootstrap.token",
		}, nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return Layout{}, fmt.Errorf("install: home directory: %w", err)
	}

	config := xdgDir("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	data := xdgDir("XDG_DATA_HOME", filepath.Join(home, ".local", "share"))
	runtime := xdgDir("XDG_RUNTIME_DIR", filepath.Join(data, "graphene", "run"))

	return Layout{
		Scope:     ScopeUser,
		Binary:    filepath.Join(home, ".local", "bin", "graphene"),
		Config:    filepath.Join(config, "graphene", "kernel.yaml"),
		Data:      filepath.Join(data, "graphene"),
		Unit:      filepath.Join(config, "systemd", "user", UnitName),
		Socket:    filepath.Join(runtime, "graphene", "kernel.sock"),
		TokenFile: filepath.Join(config, "graphene", "bootstrap.token"),
	}, nil
}

func xdgDir(env, fallback string) string {
	if value := os.Getenv(env); value != "" {
		return value
	}

	return fallback
}
