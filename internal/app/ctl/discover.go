package ctl

import (
	"fmt"
	"os"

	"github.com/graphene-ci/graphene/internal/app/clientconfig"
	"github.com/graphene-ci/graphene/internal/app/install"
	"github.com/graphene-ci/graphene/internal/app/secret"
)

// Resolve turns what the operator typed into a connection target.
//
// Order, borrowed from kubectl: explicit flags win; then the selected
// context of the client configuration; then, as the last resort, a kernel
// installed on this machine — the analog of in-cluster credentials, so
// `graphene kernel install` followed by `graphene ctl definitions` simply
// works.
func Resolve(flags Target, configPath, contextName string) (Target, error) {
	if flags.complete() {
		return flags, nil
	}

	cfg, _, err := clientconfig.Load(configPath)
	if err != nil {
		return Target{}, fmt.Errorf("ctl: %w", err)
	}

	if resolved, err := cfg.Resolve(contextName); err == nil {
		flags = flags.mergeContext(&resolved)
	} else if contextName != "" {
		// A context was named explicitly: silently falling back would
		// connect somewhere the operator did not ask for.
		return Target{}, fmt.Errorf("ctl: context %q: %w", contextName, err)
	}

	if flags.complete() {
		return flags, nil
	}

	return flags.mergeInstalled(), nil
}

// complete reports whether the target can be dialed as it stands.
func (t Target) complete() bool {
	return (t.Address != "" || t.Socket != "") && t.Token != ""
}

func (t Target) mergeContext(resolved *clientconfig.Resolved) Target {
	if t.Address == "" && t.Socket == "" {
		t.Address = resolved.Kernel.Address
		t.Socket = resolved.Kernel.Socket
	}

	if t.CAFile == "" {
		t.CAFile = resolved.Kernel.CAFile
	}

	if t.Token == "" {
		if token, err := resolved.Identity.Token.Resolve(); err == nil {
			t.Token = token
		}
	}

	return t
}

// mergeInstalled looks at the installations on this machine: the user
// scope first (a workstation kernel is the one its owner means), then the
// system one.
func (t Target) mergeInstalled() Target {
	for _, scope := range []install.Scope{install.ScopeUser, install.ScopeSystem} {
		layout, err := install.NewLayout(scope)
		if err != nil {
			continue
		}

		if t.Address == "" && t.Socket == "" {
			if _, err := os.Stat(layout.Socket); err == nil {
				t.Socket = layout.Socket
			}
		}

		if t.Token == "" {
			value := secret.Value{File: layout.TokenFile}
			if token, err := value.Resolve(); err == nil {
				t.Token = token
			}
		}
	}

	return t
}
