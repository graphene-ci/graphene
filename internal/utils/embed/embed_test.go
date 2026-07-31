package embed_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/graphene-ci/graphene/internal/utils/embed"
)

func TestCopy(t *testing.T) {
	t.Parallel()

	source := fstest.MapFS{
		"README.md": {
			Data: []byte("# {{ .Name }}\n"),
		},
		"nested/config.txt": {
			Data: []byte("enabled={{ .Enabled }}\n"),
		},
		"empty": {
			Mode: fs.ModeDir,
		},
	}
	destination := t.TempDir()
	data := struct {
		Name    string
		Enabled bool
	}{
		Name:    "example",
		Enabled: true,
	}

	if err := embed.Copy(source, destination, data); err != nil {
		t.Fatalf("Copy() error = %v", err)
	}

	assertFileContents(t, filepath.Join(destination, "README.md"), "# example\n")
	assertFileContents(t, filepath.Join(destination, "nested", "config.txt"), "enabled=true\n")

	info, err := os.Stat(filepath.Join(destination, "empty"))
	if err != nil {
		t.Fatalf("stat empty directory: %v", err)
	}

	if !info.IsDir() {
		t.Fatal("empty is not a directory")
	}
}

func TestCopyFromSubdirectory(t *testing.T) {
	t.Parallel()

	source := fstest.MapFS{
		"templates/file.txt": {
			Data: []byte("{{ . }}"),
		},
		"ignored.txt": {
			Data: []byte("ignored"),
		},
	}

	sub, err := fs.Sub(source, "templates")
	if err != nil {
		t.Fatalf("fs.Sub() error = %v", err)
	}

	destination := t.TempDir()

	if err := embed.Copy(sub, destination, "rendered"); err != nil {
		t.Fatalf("Copy() error = %v", err)
	}

	assertFileContents(t, filepath.Join(destination, "file.txt"), "rendered")

	if _, err := os.Stat(filepath.Join(destination, "ignored.txt")); !os.IsNotExist(err) {
		t.Fatalf("ignored file stat error = %v, want os.ErrNotExist", err)
	}
}

func TestCopyWithPermissions(t *testing.T) {
	t.Parallel()

	source := fstest.MapFS{
		"nested/file.txt": {
			Data: []byte("contents"),
		},
	}
	destination := t.TempDir()

	err := embed.Copy(
		source,
		destination,
		nil,
		embed.WithFilePermissions(0o600),
		embed.WithDirectoryPermissions(0o700),
	)
	if err != nil {
		t.Fatalf("Copy() error = %v", err)
	}

	assertPermissions(t, filepath.Join(destination, "nested"), 0o700)
	assertPermissions(t, filepath.Join(destination, "nested", "file.txt"), 0o600)
}

func TestCopyDoesNotOverwriteFileWhenRenderingFails(t *testing.T) {
	t.Parallel()

	destination := t.TempDir()

	target := filepath.Join(destination, "file.txt")
	if err := os.WriteFile(target, []byte("original"), 0o644); err != nil {
		t.Fatalf("write original file: %v", err)
	}

	source := fstest.MapFS{
		"file.txt": {
			Data: []byte("{{ .Missing }}"),
		},
	}

	err := embed.Copy(source, destination, struct{}{})
	if err == nil {
		t.Fatal("Copy() error = nil, want template execution error")
	}

	if !strings.Contains(err.Error(), `render embedded template "file.txt"`) {
		t.Fatalf("Copy() error = %q, want rendered file context", err)
	}

	assertFileContents(t, target, "original")
}

func TestCopyRejectsInvalidArguments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		source      fs.FS
		destination string
		want        string
	}{
		{
			name:        "nil source",
			destination: t.TempDir(),
			want:        "source is nil",
		},
		{
			name:   "empty destination",
			source: fstest.MapFS{},
			want:   "destination is empty",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := embed.Copy(test.source, test.destination, nil)
			if err == nil {
				t.Fatal("Copy() error = nil, want error")
			}

			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Copy() error = %q, want substring %q", err, test.want)
			}
		})
	}
}

func TestCopyRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		option embed.CopyOption
		want   string
	}{
		{
			name: "nil option",
			want: "option 0 is nil",
		},
		{
			name:   "file type bits",
			option: embed.WithFilePermissions(fs.ModeDir | 0o644),
			want:   "invalid file permissions",
		},
		{
			name:   "directory type bits",
			option: embed.WithDirectoryPermissions(fs.ModeDir | 0o755),
			want:   "invalid directory permissions",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := embed.Copy(
				fstest.MapFS{},
				t.TempDir(),
				nil,
				test.option,
			)
			if err == nil {
				t.Fatal("Copy() error = nil, want error")
			}

			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Copy() error = %q, want substring %q", err, test.want)
			}
		})
	}
}

func assertFileContents(t *testing.T, path, want string) {
	t.Helper()

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %q: %v", path, err)
	}

	if string(got) != want {
		t.Fatalf("contents of %q = %q, want %q", path, got, want)
	}
}

func assertPermissions(t *testing.T, path string, want fs.FileMode) {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %q: %v", path, err)
	}

	if got := info.Mode().Perm(); got != want {
		t.Fatalf("permissions of %q = %v, want %v", path, got, want)
	}
}
