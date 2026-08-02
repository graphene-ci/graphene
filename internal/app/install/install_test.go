package install_test

import (
	"strings"
	"testing"

	"github.com/graphene-ci/graphene/internal/app/install"
)

// The two scopes exist for two different situations; what matters is that
// nothing of the user scope points at a path only root can write.
func TestScopesStayInTheirLanes(t *testing.T) {
	t.Parallel()

	system, err := install.NewLayout(install.ScopeSystem)
	if err != nil {
		t.Fatalf("system layout: %v", err)
	}

	for _, path := range []string{system.Binary, system.Config, system.Data, system.Unit} {
		if !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "/home") {
			t.Fatalf("system path outside the machine: %s", path)
		}
	}

	user, err := install.NewLayout(install.ScopeUser)
	if err != nil {
		t.Fatalf("user layout: %v", err)
	}

	for _, path := range []string{user.Binary, user.Config, user.Data, user.Unit} {
		if strings.HasPrefix(path, "/etc") || strings.HasPrefix(path, "/usr") || strings.HasPrefix(path, "/var") {
			t.Fatalf("user path needs privileges: %s", path)
		}
	}
}

// Both scopes are sandboxed: a user manager applies these directives just
// as the system one does, so a workstation kernel is confined too. Only
// what is genuinely privileged differs.
func TestUnitMatchesItsScope(t *testing.T) {
	t.Parallel()

	shared := []string{
		"NoNewPrivileges=", "PrivateTmp=", "ProtectSystem=strict",
		"ProtectKernelTunables=", "RestrictSUIDSGID=", "SystemCallFilter=",
		"RuntimeDirectory=", "UMask=",
	}
	// StateDirectory belongs to the system manager; ProtectHome would hide
	// the directory a user-scope kernel keeps its data in.
	systemOnly := []string{"StateDirectory=", "ProtectHome="}

	system, _ := install.NewLayout(install.ScopeSystem)

	systemUnit, err := install.RenderUnit(&system)
	if err != nil {
		t.Fatalf("render system unit: %v", err)
	}

	user, _ := install.NewLayout(install.ScopeUser)

	userUnit, err := install.RenderUnit(&user)
	if err != nil {
		t.Fatalf("render user unit: %v", err)
	}

	for _, directive := range shared {
		if !strings.Contains(string(systemUnit), directive) {
			t.Errorf("system unit misses %s", directive)
		}

		if !strings.Contains(string(userUnit), directive) {
			t.Errorf("user unit misses %s", directive)
		}
	}

	for _, directive := range systemOnly {
		if !strings.Contains(string(systemUnit), directive) {
			t.Errorf("system unit misses %s", directive)
		}

		if strings.Contains(string(userUnit), directive) {
			t.Errorf("user unit carries the privileged %s", directive)
		}
	}

	// Each unit must be writable exactly where its data lives.
	if !strings.Contains(string(userUnit), "ReadWritePaths="+user.Data) {
		t.Errorf("user unit cannot write its own data directory")
	}

	if !strings.Contains(string(systemUnit), "ReadWritePaths="+system.Data) {
		t.Errorf("system unit cannot write its own data directory")
	}
}

// The generated configuration must be the one the kernel then reads: the
// socket, the data directory and the token file all agree with the layout.
func TestConfigMatchesLayout(t *testing.T) {
	t.Parallel()

	layout, _ := install.NewLayout(install.ScopeUser)

	body, err := install.RenderConfig(&layout, install.ConfigOptions{Tenant: "acme", Name: "k1"})
	if err != nil {
		t.Fatalf("render config: %v", err)
	}

	text := string(body)
	for _, want := range []string{
		"data_dir: " + layout.Data,
		"uds: " + layout.Socket,
		"file: " + layout.TokenFile,
		"tenant: acme",
		"name: k1",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("config misses %q:\n%s", want, text)
		}
	}

	// Without a TCP address there is no TLS section to configure.
	if strings.Contains(text, "tls:") {
		t.Fatalf("socket-only config configures tls:\n%s", text)
	}

	withTCP, err := install.RenderConfig(&layout, install.ConfigOptions{Tenant: "acme", Name: "k1", TCP: "0.0.0.0:9000"})
	if err != nil {
		t.Fatalf("render config: %v", err)
	}

	if !strings.Contains(string(withTCP), "tcp: 0.0.0.0:9000") || !strings.Contains(string(withTCP), "mode: auto") {
		t.Fatalf("tcp config:\n%s", withTCP)
	}
}
