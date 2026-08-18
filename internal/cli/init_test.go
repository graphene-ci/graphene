package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitScaffoldsProject(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "perf")
	var out, errOut bytes.Buffer
	if err := Init([]string{"-dir", dir, "-module", "example.com/perf", "perf-nightly"}, &out, &errOut); err != nil {
		t.Fatalf("init: %v (%s)", err, errOut.String())
	}
	for _, name := range []string{"main.go", "go.mod", "graphene.yaml", "Dockerfile", "Makefile", ".gitignore"} {
		raw, err := os.ReadFile(filepath.Join(dir, name)) //nolint:gosec // test reads its own tempdir
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if strings.Contains(string(raw), "{{") {
			t.Fatalf("%s: unrendered template markers", name)
		}
	}
	mainSrc, err := os.ReadFile(filepath.Join(dir, "main.go")) //nolint:gosec // test tempdir
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mainSrc), `pipeline.Main("perf-nightly"`) {
		t.Fatal("main.go does not carry the pipeline id")
	}
	gomod, err := os.ReadFile(filepath.Join(dir, "go.mod")) //nolint:gosec // test tempdir
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(gomod), "module example.com/perf") {
		t.Fatal("go.mod does not carry the module path")
	}

	// Init never overwrites what a person may have edited.
	if err := Init([]string{"-dir", dir, "perf-nightly"}, &out, &errOut); err == nil {
		t.Fatal("second init overwrote an existing project")
	}
}
