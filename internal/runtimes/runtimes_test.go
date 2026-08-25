package runtimes

import "testing"

// The catalogue is the installation's answer to "which languages can I
// write a pipeline in" — a runtime it does not carry must fail with a
// list, not silently fall back.
func TestResolve(t *testing.T) {
	c := New(nil)
	if _, err := c.Resolve(""); err != nil {
		t.Fatalf("the default runtime must resolve: %v", err)
	}
	if _, err := c.Resolve("go@1.26"); err != nil {
		t.Fatalf("pinning the carried version must resolve: %v", err)
	}
	if _, err := c.Resolve("go@1.21"); err == nil {
		t.Fatal("a version the installation does not carry must be refused")
	}
	if _, err := c.Resolve("python"); err == nil {
		t.Fatal("an unknown runtime must be refused")
	}
}

// A configured runtime is data: adding a language is configuration.
func TestConfiguredRuntime(t *testing.T) {
	c := New([]Runtime{{
		Name: "node", Version: "22", Image: "mirror.gcr.io/library/node:22",
		Build:    "npx esbuild --bundle --platform=node index.ts --outfile=/tmp/app",
		Artifact: "/tmp/app", Describe: "node /tmp/app", Base: "mirror.gcr.io/library/node:22-slim",
	}})
	r, err := c.Resolve("node")
	if err != nil {
		t.Fatalf("a configured runtime must resolve: %v", err)
	}
	if r.Image != "mirror.gcr.io/library/node:22" {
		t.Fatalf("wrong image: %s", r.Image)
	}
	// A partial override keeps the rest of a built-in.
	c2 := New([]Runtime{{Name: "go", Image: "internal.registry/golang:1.26"}})
	g, err := c2.Resolve("go")
	if err != nil {
		t.Fatal(err)
	}
	if g.Image != "internal.registry/golang:1.26" || g.Build != builtinGo.Build {
		t.Fatalf("partial override lost the built-in: %+v", g)
	}
}
