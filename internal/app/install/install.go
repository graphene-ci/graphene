package install

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/graphene-ci/graphene/internal/app/clientconfig"
	"github.com/graphene-ci/graphene/internal/app/secret"
)

const (
	dirMode    = 0o750
	fileMode   = 0o644
	secretMode = 0o600
	binMode    = 0o755

	tokenBytes = 24
)

// ErrNoSystemd — systemd is not the init system here (or not reachable in
// this scope), so the unit cannot be managed.
var ErrNoSystemd = errors.New("install: systemd is not available")

// Options steer an installation.
type Options struct {
	Scope Scope
	// Tenant and Name identify the kernel in its own resources.
	Tenant string
	Name   string
	// TCP, when set, also serves the network endpoint (TLS is minted).
	TCP string
	// Force overwrites an existing configuration.
	Force bool
	// SkipEnable installs the files without starting anything: useful in
	// images and containers where the unit is enabled later.
	SkipEnable bool
}

// Result reports what an installation produced.
type Result struct {
	Layout Layout
	// Token is set only when a bootstrap credential was generated now;
	// an existing one is never read back for printing.
	Token string
	// Started reports whether the unit was enabled and started.
	Started bool
}

// Install lays out the files and, unless asked not to, enables the unit.
func Install(ctx context.Context, opts Options) (Result, error) {
	layout, err := NewLayout(opts.Scope)
	if err != nil {
		return Result{}, err
	}

	result := Result{Layout: layout}

	if err := installBinary(&layout); err != nil {
		return result, err
	}

	token, err := ensureToken(&layout, opts.Force)
	if err != nil {
		return result, err
	}

	result.Token = token

	if err := writeConfig(&layout, opts); err != nil {
		return result, err
	}

	if err := writeUnit(&layout); err != nil {
		return result, err
	}

	if err := recordContext(&layout, opts); err != nil {
		return result, err
	}

	if opts.SkipEnable {
		return result, nil
	}

	if err := Enable(ctx, &layout); err != nil {
		return result, err
	}

	// A service that was already running holds the OLD binary and the old
	// token in memory: enabling it again changes nothing, so a reinstall
	// has to restart it explicitly.
	if err := Systemctl(ctx, opts.Scope, "restart", UnitName); err != nil {
		return result, err
	}

	result.Started = true

	return result, nil
}

// recordContext teaches the client about the kernel just installed: the
// same file kubectl-style tooling reads, so `graphen ctl ...` needs no
// flags on this machine — and adding a second kernel later does not
// disturb this one.
func recordContext(layout *Layout, opts Options) error {
	cfg, path, err := clientconfig.Load("")
	if err != nil {
		return fmt.Errorf("install: client configuration: %w", err)
	}

	name := opts.Name
	if opts.Scope == ScopeSystem {
		name = opts.Name + "-system"
	}

	cfg.Upsert(name,
		clientconfig.Kernel{Socket: layout.Socket},
		clientconfig.Identity{Token: secret.Value{File: layout.TokenFile}},
		opts.Tenant,
	)

	if err := clientconfig.Save(cfg, path); err != nil {
		return fmt.Errorf("install: client configuration: %w", err)
	}

	return nil
}

// installBinary copies the running executable to its installed location —
// so `graphen kernel install` works from anywhere (a build directory, a
// download) and the unit points at a stable path.
func installBinary(layout *Layout) error {
	current, err := os.Executable()
	if err != nil {
		return fmt.Errorf("install: locate executable: %w", err)
	}

	current, err = filepath.EvalSymlinks(current)
	if err != nil {
		return fmt.Errorf("install: resolve executable: %w", err)
	}

	target, err := filepath.EvalSymlinks(layout.Binary)
	if err == nil && target == current {
		return nil // already running from the installed location
	}

	if err := os.MkdirAll(filepath.Dir(layout.Binary), binMode); err != nil {
		return fmt.Errorf("install: create bin directory: %w", err)
	}

	return copyFile(current, layout.Binary, binMode)
}

// ensureToken creates the bootstrap credential unless one exists. The
// value is returned only when freshly generated: an operator who lost it
// rotates it rather than reading it back from a file the service owns.
func ensureToken(layout *Layout, force bool) (string, error) {
	if err := os.MkdirAll(filepath.Dir(layout.TokenFile), dirMode); err != nil {
		return "", fmt.Errorf("install: create config directory: %w", err)
	}

	if _, err := os.Stat(layout.TokenFile); err == nil && !force {
		return "", nil
	}

	raw := make([]byte, tokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("install: generate token: %w", err)
	}

	token := hex.EncodeToString(raw)
	if err := os.WriteFile(layout.TokenFile, []byte(token+"\n"), secretMode); err != nil {
		return "", fmt.Errorf("install: write token: %w", err)
	}

	return token, nil
}

func writeConfig(layout *Layout, opts Options) error {
	if _, err := os.Stat(layout.Config); err == nil && !opts.Force {
		return nil // an operator's configuration is never overwritten silently
	}

	body, err := RenderConfig(layout, ConfigOptions{
		Tenant: opts.Tenant,
		Name:   opts.Name,
		TCP:    opts.TCP,
	})
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(layout.Config), dirMode); err != nil {
		return fmt.Errorf("install: create config directory: %w", err)
	}

	if err := os.WriteFile(layout.Config, body, fileMode); err != nil {
		return fmt.Errorf("install: write config: %w", err)
	}

	return nil
}

func writeUnit(layout *Layout) error {
	body, err := RenderUnit(layout)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(layout.Unit), dirMode); err != nil {
		return fmt.Errorf("install: create unit directory: %w", err)
	}

	if err := os.WriteFile(layout.Unit, body, fileMode); err != nil {
		return fmt.Errorf("install: write unit: %w", err)
	}

	return nil
}

// Uninstall stops the unit and removes what the installation created,
// keeping the data directory: dropping a kernel's truth must be a
// deliberate, separate act.
func Uninstall(ctx context.Context, scope Scope) (Layout, error) {
	layout, err := NewLayout(scope)
	if err != nil {
		return Layout{}, err
	}

	if Available(scope) {
		_ = Systemctl(ctx, scope, "disable", "--now", UnitName)
	}

	if err := os.Remove(layout.Unit); err != nil && !errors.Is(err, os.ErrNotExist) {
		return layout, fmt.Errorf("install: remove unit: %w", err)
	}

	if Available(scope) {
		_ = Systemctl(ctx, scope, "daemon-reload")
	}

	return layout, nil
}

// Enable reloads systemd and starts the unit.
func Enable(ctx context.Context, layout *Layout) error {
	if !Available(layout.Scope) {
		return ErrNoSystemd
	}

	if err := Systemctl(ctx, layout.Scope, "daemon-reload"); err != nil {
		return err
	}

	return Systemctl(ctx, layout.Scope, "enable", "--now", UnitName)
}

// Available reports whether systemd can be driven in this scope.
func Available(scope Scope) bool {
	if _, err := exec.LookPath("systemctl"); err != nil {
		return false
	}

	if _, err := os.Stat("/run/systemd/system"); err != nil {
		return false
	}

	if scope == ScopeUser && os.Getenv("XDG_RUNTIME_DIR") == "" {
		// Without a runtime directory there is no user bus to talk to.
		return false
	}

	return true
}

// Systemctl runs a systemctl verb in the right scope.
func Systemctl(ctx context.Context, scope Scope, args ...string) error {
	full := args
	if scope == ScopeUser {
		full = append([]string{"--user"}, args...)
	}

	cmd := exec.CommandContext(ctx, "systemctl", full...)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("install: systemctl %s: %w: %s",
			strings.Join(full, " "), err, strings.TrimSpace(string(out)))
	}

	return nil
}

// Status returns systemctl's own status text for the unit.
func Status(ctx context.Context, scope Scope) (string, error) {
	if !Available(scope) {
		return "", ErrNoSystemd
	}

	args := []string{"status", UnitName, "--no-pager"}
	if scope == ScopeUser {
		args = append([]string{"--user"}, args...)
	}

	out, err := exec.CommandContext(ctx, "systemctl", args...).CombinedOutput()
	// systemctl status exits non-zero for a stopped unit: the text is the
	// answer either way.
	if len(out) > 0 {
		return string(out), nil
	}

	if err != nil {
		return "", fmt.Errorf("install: systemctl status: %w", err)
	}

	return "", nil
}

func copyFile(from, target string, mode os.FileMode) error {
	src, err := os.Open(from)
	if err != nil {
		return fmt.Errorf("install: read %s: %w", from, err)
	}

	defer func() { _ = src.Close() }()

	// Replacing a running binary fails with ETXTBSY; writing beside it and
	// renaming is atomic and works while the old one runs.
	tmp := target + ".new"

	dst, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("install: write %s: %w", tmp, err)
	}

	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		_ = os.Remove(tmp)

		return fmt.Errorf("install: copy binary: %w", err)
	}

	if err := dst.Close(); err != nil {
		_ = os.Remove(tmp)

		return fmt.Errorf("install: close %s: %w", tmp, err)
	}

	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp)

		return fmt.Errorf("install: install binary: %w", err)
	}

	return nil
}
