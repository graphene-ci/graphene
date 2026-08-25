// Package runtimes is the installation's catalogue of toolchains: what
// languages this Graphene can build a pipeline from. A runtime is data
// — an image, a build command, the artifact it produces — so adding a
// language is configuration, not a code change.
package runtimes

import (
	"fmt"
	"sort"
	"strings"
)

// Runtime describes one toolchain.
type Runtime struct {
	// Name is what a workspace asks for ("go").
	Name string `mapstructure:"name"`
	// Version is the pinned toolchain version, shown to humans.
	Version string `mapstructure:"version"`
	// Image is the toolchain container the build runs in; it also does
	// the source checkout (it carries git).
	Image string `mapstructure:"image"`
	// Build is the shell command that turns the workspace root into ONE
	// executable artifact at Artifact. The workspace root IS the build
	// root: no entrypoint discovery anywhere.
	Build string `mapstructure:"build"`
	// Artifact is the path the build writes inside the container.
	Artifact string `mapstructure:"artifact"`
	// Describe runs the artifact in manifest mode; its stdout is the
	// pipeline manifest.
	Describe string `mapstructure:"describe"`
	// Base is the image the artifact is laid over to make the worker
	// image.
	Base string `mapstructure:"base"`
}

// Catalogue is the installation's set of runtimes.
type Catalogue struct {
	byName map[string]Runtime
	// Default is the runtime a workspace gets when it names none.
	Default string
}

// The built-in Go toolchain. mirror.gcr.io serves regions docker.io
// does not; the image carries git, so a checkout needs no second one.
var builtinGo = Runtime{
	Name:     "go",
	Version:  "1.26",
	Image:    "mirror.gcr.io/library/golang:1.26",
	Build:    "go build -trimpath -ldflags '-s -w -buildid=' -o /tmp/app .",
	Artifact: "/tmp/app",
	Describe: "/tmp/app",
	Base:     "gcr.io/distroless/static:nonroot",
}

// New builds the catalogue: the built-ins plus whatever the
// installation configured (the same name overrides field by field).
func New(configured []Runtime) *Catalogue {
	c := &Catalogue{byName: map[string]Runtime{}, Default: builtinGo.Name}
	c.byName[builtinGo.Name] = builtinGo
	for _, r := range configured {
		if r.Name == "" {
			continue
		}
		base := c.byName[r.Name] // a partial override keeps the rest
		if r.Image != "" {
			base.Image = r.Image
		}
		if r.Version != "" {
			base.Version = r.Version
		}
		if r.Build != "" {
			base.Build = r.Build
		}
		if r.Artifact != "" {
			base.Artifact = r.Artifact
		}
		if r.Describe != "" {
			base.Describe = r.Describe
		}
		if r.Base != "" {
			base.Base = r.Base
		}
		base.Name = r.Name
		c.byName[r.Name] = base
	}
	return c
}

// Resolve returns the runtime a workspace asks for; an empty name
// takes the default. A name spelled "go@1.26" pins the version the
// installation carries.
func (c *Catalogue) Resolve(name string) (Runtime, error) {
	if name == "" {
		name = c.Default
	}
	base, version, _ := strings.Cut(name, "@")
	r, ok := c.byName[base]
	if !ok {
		return Runtime{}, fmt.Errorf("runtime %q is not available here; this installation carries %s", name, strings.Join(c.Names(), ", "))
	}
	if version != "" && version != r.Version {
		return Runtime{}, fmt.Errorf("runtime %s carries version %s, not %s", base, r.Version, version)
	}
	if r.Image == "" || r.Build == "" || r.Artifact == "" {
		return Runtime{}, fmt.Errorf("runtime %q is misconfigured: image, build and artifact are required", name)
	}
	return r, nil
}

// Names lists the catalogue, sorted.
func (c *Catalogue) Names() []string {
	out := make([]string, 0, len(c.byName))
	for name, r := range c.byName {
		out = append(out, name+"@"+r.Version)
	}
	sort.Strings(out)
	return out
}

// All returns every runtime, sorted by name.
func (c *Catalogue) All() []Runtime {
	out := make([]Runtime, 0, len(c.byName))
	for _, r := range c.byName {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
