// Package clientconfig is how a client knows which kernels exist and what
// to present to them — the kubeconfig idea, kept to what this system
// needs.
//
// Three registries and a pointer: kernels (where and how to connect),
// identities (what to present), contexts (a pairing). Adding a kernel does
// not disturb the others, and switching between them is one word.
//
// Resolution order, borrowed as-is: an explicit path, then $GRAPHENE_CONFIG,
// then the user's config directory. The only implicit source is the last
// resort — a kernel installed on this machine — which is the analog of
// kubectl's in-cluster credentials.
package clientconfig

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"sigs.k8s.io/yaml"

	"github.com/graphene-ci/graphene/internal/app/secret"
)

// EnvPath overrides where the client configuration is read from.
const EnvPath = "GRAPHENE_CONFIG"

const (
	dirMode  = 0o700
	fileMode = 0o600
)

var (
	// ErrNoContext — nothing selects a context.
	ErrNoContext = errors.New("clientconfig: no context selected")
	// ErrUnknownContext — the named context is not defined.
	ErrUnknownContext = errors.New("clientconfig: unknown context")
	// ErrUnknownKernel — a context points at an undefined kernel.
	ErrUnknownKernel = errors.New("clientconfig: unknown kernel")
	// ErrUnknownIdentity — a context points at an undefined identity.
	ErrUnknownIdentity = errors.New("clientconfig: unknown identity")
)

// Config is the whole client configuration file.
type Config struct {
	CurrentContext string              `json:"current_context,omitempty"`
	Contexts       map[string]Context  `json:"contexts,omitempty"`
	Kernels        map[string]Kernel   `json:"kernels,omitempty"`
	Identities     map[string]Identity `json:"identities,omitempty"`
}

// Context pairs a kernel with an identity.
type Context struct {
	Kernel   string `json:"kernel"`
	Identity string `json:"identity"`
	// Tenant is the default tenant for commands that need one.
	Tenant string `json:"tenant,omitempty"`
}

// Kernel is where a kernel is and how its certificate is trusted.
type Kernel struct {
	Address string `json:"address,omitempty"`
	Socket  string `json:"socket,omitempty"`
	CAFile  string `json:"ca_file,omitempty"`
}

// Identity is what to present — the same secret model the kernel uses, so
// a token can live in a file, in the environment, or (for scratch setups)
// inline.
type Identity struct {
	Token secret.Value `json:"token,omitempty"`
}

// Resolved is a context with its parts looked up.
type Resolved struct {
	Name     string
	Context  Context
	Kernel   Kernel
	Identity Identity
}

// Path reports where the configuration lives.
func Path(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}

	if fromEnv := os.Getenv(EnvPath); fromEnv != "" {
		return fromEnv, nil
	}

	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("clientconfig: config directory: %w", err)
	}

	return filepath.Join(dir, "graphene", "config.yaml"), nil
}

// Load reads the configuration. A missing file is an empty configuration,
// not an error: a machine that never ran install has none.
func Load(explicit string) (*Config, string, error) {
	path, err := Path(explicit)
	if err != nil {
		return nil, "", err
	}

	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return &Config{}, path, nil
	}

	if err != nil {
		return nil, path, fmt.Errorf("clientconfig: read %s: %w", path, err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return nil, path, fmt.Errorf("clientconfig: parse %s: %w", path, err)
	}

	return cfg, path, nil
}

// Save writes the configuration back.
func Save(cfg *Config, path string) error {
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("clientconfig: render: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), dirMode); err != nil {
		return fmt.Errorf("clientconfig: create directory: %w", err)
	}

	if err := os.WriteFile(path, raw, fileMode); err != nil {
		return fmt.Errorf("clientconfig: write %s: %w", path, err)
	}

	return nil
}

// Resolve looks a context up; an empty name means the current one.
func (c *Config) Resolve(name string) (Resolved, error) {
	if name == "" {
		name = c.CurrentContext
	}

	if name == "" {
		return Resolved{}, ErrNoContext
	}

	ctx, ok := c.Contexts[name]
	if !ok {
		return Resolved{}, fmt.Errorf("%w: %s", ErrUnknownContext, name)
	}

	kernel, ok := c.Kernels[ctx.Kernel]
	if !ok {
		return Resolved{}, fmt.Errorf("%w: %s", ErrUnknownKernel, ctx.Kernel)
	}

	identity, ok := c.Identities[ctx.Identity]
	if !ok {
		return Resolved{}, fmt.Errorf("%w: %s", ErrUnknownIdentity, ctx.Identity)
	}

	return Resolved{Name: name, Context: ctx, Kernel: kernel, Identity: identity}, nil
}

// Upsert records a kernel, an identity and the context pairing them,
// selecting it when nothing was selected before.
func (c *Config) Upsert(name string, kernel Kernel, identity Identity, tenant string) {
	if c.Kernels == nil {
		c.Kernels = map[string]Kernel{}
	}

	if c.Identities == nil {
		c.Identities = map[string]Identity{}
	}

	if c.Contexts == nil {
		c.Contexts = map[string]Context{}
	}

	c.Kernels[name] = kernel
	c.Identities[name] = identity
	c.Contexts[name] = Context{Kernel: name, Identity: name, Tenant: tenant}

	if c.CurrentContext == "" {
		c.CurrentContext = name
	}
}

// Use selects a context.
func (c *Config) Use(name string) error {
	if _, ok := c.Contexts[name]; !ok {
		return fmt.Errorf("%w: %s", ErrUnknownContext, name)
	}

	c.CurrentContext = name

	return nil
}

// Remove drops a context and the kernel and identity only it referenced.
func (c *Config) Remove(name string) {
	ctx, ok := c.Contexts[name]
	if !ok {
		return
	}

	delete(c.Contexts, name)

	if !referenced(c.Contexts, ctx.Kernel, ctx.Identity) {
		delete(c.Kernels, ctx.Kernel)
		delete(c.Identities, ctx.Identity)
	}

	if c.CurrentContext == name {
		c.CurrentContext = ""

		if names := c.Names(); len(names) > 0 {
			c.CurrentContext = names[0]
		}
	}
}

func referenced(contexts map[string]Context, kernel, identity string) bool {
	for _, ctx := range contexts {
		if ctx.Kernel == kernel || ctx.Identity == identity {
			return true
		}
	}

	return false
}

// Names lists the defined contexts in a stable order.
func (c *Config) Names() []string {
	out := make([]string, 0, len(c.Contexts))
	for name := range c.Contexts {
		out = append(out, name)
	}

	sort.Strings(out)

	return out
}
