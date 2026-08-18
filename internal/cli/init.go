// Package cli is the user-facing graphene command: project scaffolding
// now, the full control surface next.
package cli

import (
	"embed"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

//go:embed templates/*.tmpl
var templates embed.FS

// initData feeds the project templates.
type initData struct {
	// Name is the pipeline id: one main == one pipeline.
	Name string
	// Module is the Go module path of the project.
	Module string
	// Image is the worker image ref of the pipeline.
	Image string
	// ServerGRPC / ServerHTTP are the installation's single door.
	ServerGRPC string
	ServerHTTP string
	GoVersion  string
}

// initFiles maps template names to the files they become.
var initFiles = []struct{ tmpl, out string }{
	{"main.go.tmpl", "main.go"},
	{"go.mod.tmpl", "go.mod"},
	{"graphene.yaml.tmpl", "graphene.yaml"},
	{"Dockerfile.tmpl", "Dockerfile"},
	{"Makefile.tmpl", "Makefile"},
	{"gitignore.tmpl", ".gitignore"},
}

// Init scaffolds a pipeline project: the layout a person needs to write,
// build, and ship a pipeline — main.go on the pipeline library, a worker
// image Dockerfile, a Makefile, and graphene.yaml with the run settings.
func Init(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("graphene init", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var d initData
	dir := fs.String("dir", "", "directory to create the project in (default: ./<name>)")
	fs.StringVar(&d.Module, "module", "", "Go module path (default: <name>)")
	fs.StringVar(&d.Image, "image", "", "worker image ref (default: <name>:latest)")
	fs.StringVar(&d.ServerGRPC, "server-grpc", "127.0.0.1:7233", "installation gRPC door")
	fs.StringVar(&d.ServerHTTP, "server-http", "http://127.0.0.1:7280", "installation HTTP door")
	fs.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "usage: graphene init <name> [flags]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return fmt.Errorf("init takes exactly one argument: the pipeline name")
	}
	d.Name = fs.Arg(0)
	if strings.ContainsAny(d.Name, " \t/\\") {
		return fmt.Errorf("pipeline name %q: no spaces or slashes", d.Name)
	}
	if d.Module == "" {
		d.Module = d.Name
	}
	if d.Image == "" {
		d.Image = d.Name + ":latest"
	}
	d.GoVersion = goVersion
	target := *dir
	if target == "" {
		target = d.Name
	}

	if err := os.MkdirAll(target, 0o750); err != nil {
		return err
	}
	tpl, err := template.ParseFS(templates, "templates/*.tmpl")
	if err != nil {
		return err
	}
	for _, f := range initFiles {
		out := filepath.Join(target, f.out)
		// Never overwrite: init scaffolds, a person owns the files after.
		if _, err := os.Stat(out); err == nil {
			return fmt.Errorf("%s already exists — init refuses to overwrite", out)
		}
		file, err := os.OpenFile(out, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644) //nolint:gosec // scaffolded sources are world-readable
		if err != nil {
			return err
		}
		if err := tpl.ExecuteTemplate(file, f.tmpl, d); err != nil {
			_ = file.Close()
			return fmt.Errorf("render %s: %w", f.out, err)
		}
		if err := file.Close(); err != nil {
			return err
		}
		_, _ = fmt.Fprintln(stdout, "created", out)
	}
	_, _ = fmt.Fprintf(stdout, "\nnext:\n  cd %s\n  make tidy\n  make build\n", target)
	return nil
}

// goVersion pins the toolchain line of scaffolded projects.
const goVersion = "1.25"
