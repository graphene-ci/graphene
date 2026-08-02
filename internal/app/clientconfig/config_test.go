package clientconfig_test

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/graphene-ci/graphene/internal/app/clientconfig"
	"github.com/graphene-ci/graphene/internal/app/secret"
)

func tempPath(t *testing.T) string {
	t.Helper()

	return filepath.Join(t.TempDir(), "config.yaml")
}

// A machine that never ran install has no file, and that is not an error:
// the client falls through to its other sources.
func TestMissingFileIsEmpty(t *testing.T) {
	t.Parallel()

	cfg, path, err := clientconfig.Load(tempPath(t))
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if path == "" {
		t.Fatal("load reported no path")
	}

	if len(cfg.Names()) != 0 {
		t.Fatalf("fresh config has contexts: %v", cfg.Names())
	}

	if _, err := cfg.Resolve(""); !errors.Is(err, clientconfig.ErrNoContext) {
		t.Fatalf("resolve on empty config: %v", err)
	}
}

func TestUpsertResolveRoundTrip(t *testing.T) {
	t.Parallel()

	path := tempPath(t)

	cfg := &clientconfig.Config{}
	cfg.Upsert("local",
		clientconfig.Kernel{Socket: "/run/graphene/kernel.sock"},
		clientconfig.Identity{Token: secret.Value{Inline: "t"}},
		"acme")

	// The first context installed becomes the current one.
	if cfg.CurrentContext != "local" {
		t.Fatalf("current context: %q", cfg.CurrentContext)
	}

	if err := clientconfig.Save(cfg, path); err != nil {
		t.Fatalf("save: %v", err)
	}

	reloaded, _, err := clientconfig.Load(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}

	resolved, err := reloaded.Resolve("")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if resolved.Kernel.Socket != "/run/graphene/kernel.sock" || resolved.Context.Tenant != "acme" {
		t.Fatalf("resolved: %+v", resolved)
	}

	token, err := resolved.Identity.Token.Resolve()
	if err != nil || token != "t" {
		t.Fatalf("token: %q %v", token, err)
	}
}

// A second kernel must not disturb the first: that is the whole point of
// keeping kernels, identities and contexts apart.
func TestSecondKernelIsAdditive(t *testing.T) {
	t.Parallel()

	cfg := &clientconfig.Config{}
	cfg.Upsert("local", clientconfig.Kernel{Socket: "/s"}, clientconfig.Identity{Token: secret.Value{Inline: "a"}}, "acme")
	cfg.Upsert("prod", clientconfig.Kernel{Address: "srv:9000", CAFile: "/ca"}, clientconfig.Identity{Token: secret.Value{Inline: "b"}}, "acme")

	if cfg.CurrentContext != "local" {
		t.Fatalf("adding a context stole the selection: %q", cfg.CurrentContext)
	}

	if err := cfg.Use("prod"); err != nil {
		t.Fatalf("use: %v", err)
	}

	resolved, err := cfg.Resolve("")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if resolved.Kernel.Address != "srv:9000" {
		t.Fatalf("resolved: %+v", resolved)
	}

	// The other context is still intact and reachable by name.
	if _, err := cfg.Resolve("local"); err != nil {
		t.Fatalf("resolve local: %v", err)
	}

	if err := cfg.Use("nope"); !errors.Is(err, clientconfig.ErrUnknownContext) {
		t.Fatalf("use unknown: %v", err)
	}
}

func TestRemoveSelectsAnother(t *testing.T) {
	t.Parallel()

	cfg := &clientconfig.Config{}
	cfg.Upsert("local", clientconfig.Kernel{Socket: "/s"}, clientconfig.Identity{}, "acme")
	cfg.Upsert("prod", clientconfig.Kernel{Address: "srv:9000"}, clientconfig.Identity{}, "acme")

	cfg.Remove("local")

	if _, ok := cfg.Contexts["local"]; ok {
		t.Fatal("context survived removal")
	}

	if _, ok := cfg.Kernels["local"]; ok {
		t.Fatal("the kernel only that context referenced survived")
	}

	if cfg.CurrentContext != "prod" {
		t.Fatalf("removal left no selection: %q", cfg.CurrentContext)
	}
}

func TestBrokenReferencesAreNamed(t *testing.T) {
	t.Parallel()

	cfg := &clientconfig.Config{
		CurrentContext: "broken",
		Contexts:       map[string]clientconfig.Context{"broken": {Kernel: "gone", Identity: "gone"}},
	}

	if _, err := cfg.Resolve(""); !errors.Is(err, clientconfig.ErrUnknownKernel) {
		t.Fatalf("dangling kernel: %v", err)
	}

	cfg.Kernels = map[string]clientconfig.Kernel{"gone": {Socket: "/s"}}
	if _, err := cfg.Resolve(""); !errors.Is(err, clientconfig.ErrUnknownIdentity) {
		t.Fatalf("dangling identity: %v", err)
	}
}
