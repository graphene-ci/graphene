package install

import (
	"fmt"
	"os"
	"path/filepath"
)

// Shell names a shell whose completion we install.
type Shell string

const (
	// ShellBash — bash-completion's user and system directories.
	ShellBash Shell = "bash"
	// ShellZsh — a site-functions directory on zsh's fpath.
	ShellZsh Shell = "zsh"
	// ShellFish — fish's completion directories.
	ShellFish Shell = "fish"
)

// Shells is every shell an installation writes completions for.
//
//nolint:gochecknoglobals // a slice cannot be const; treated as one
var Shells = []Shell{ShellBash, ShellZsh, ShellFish}

// CompletionPath is where a shell looks for our completion script.
//
// Installing the script is part of installing the tool: a completion that
// exists in the binary but is never sourced is a completion nobody has.
// The paths are the conventional ones, so each shell finds them without
// the operator editing an rc file.
func CompletionPath(layout *Layout, shell Shell) (string, error) {
	if layout.Scope == ScopeSystem {
		switch shell {
		case ShellBash:
			return "/usr/share/bash-completion/completions/graphen", nil
		case ShellZsh:
			return "/usr/share/zsh/site-functions/_graphen", nil
		case ShellFish:
			return "/usr/share/fish/vendor_completions.d/graphen.fish", nil
		}

		return "", fmt.Errorf("%w: %s", ErrUnknownShell, shell)
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("install: home directory: %w", err)
	}

	data := xdgDir("XDG_DATA_HOME", filepath.Join(home, ".local", "share"))
	config := xdgDir("XDG_CONFIG_HOME", filepath.Join(home, ".config"))

	switch shell {
	case ShellBash:
		return filepath.Join(data, "bash-completion", "completions", "graphen"), nil
	case ShellZsh:
		return filepath.Join(data, "zsh", "site-functions", "_graphen"), nil
	case ShellFish:
		return filepath.Join(config, "fish", "completions", "graphen.fish"), nil
	}

	return "", fmt.Errorf("%w: %s", ErrUnknownShell, shell)
}

// WriteCompletions installs the scripts it is given, skipping shells whose
// directory does not exist and cannot be created — an absent shell is not
// a failed installation.
func WriteCompletions(layout *Layout, scripts map[Shell][]byte) []Shell {
	var written []Shell

	for shell, body := range scripts {
		path, err := CompletionPath(layout, shell)
		if err != nil {
			continue
		}

		if err := os.MkdirAll(filepath.Dir(path), completionDirMode); err != nil {
			continue
		}

		if err := os.WriteFile(path, body, fileMode); err != nil {
			continue
		}

		written = append(written, shell)
	}

	return written
}

const completionDirMode = 0o755
