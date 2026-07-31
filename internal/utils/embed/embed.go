package embed

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"text/template"
)

const (
	// DefaultFilePermissions lets the process umask determine the permissions
	// of newly created files.
	DefaultFilePermissions fs.FileMode = 0o666

	// DefaultDirectoryPermissions lets the process umask determine the
	// permissions of newly created directories.
	DefaultDirectoryPermissions fs.FileMode = 0o777
)

type copyOptions struct {
	filePermissions      fs.FileMode
	directoryPermissions fs.FileMode
}

// CopyOption configures Copy.
type CopyOption func(*copyOptions)

// WithFilePermissions sets the permissions used for newly created files.
// The process umask still applies.
func WithFilePermissions(permissions fs.FileMode) CopyOption {
	return func(options *copyOptions) {
		options.filePermissions = permissions
	}
}

// WithDirectoryPermissions sets the permissions used for newly created
// directories. The process umask still applies.
func WithDirectoryPermissions(permissions fs.FileMode) CopyOption {
	return func(options *copyOptions) {
		options.directoryPermissions = permissions
	}
}

// Copy renders every file in source as a Go text template and writes the
// resulting tree to destination.
//
// source is walked from its root. Callers can use fs.Sub when only a
// subdirectory of an embed.FS should be copied.
func Copy(source fs.FS, destination string, data any, options ...CopyOption) error {
	if source == nil {
		return fmt.Errorf("copy embedded templates: source is nil")
	}

	if destination == "" {
		return fmt.Errorf("copy embedded templates: destination is empty")
	}

	config := copyOptions{
		filePermissions:      DefaultFilePermissions,
		directoryPermissions: DefaultDirectoryPermissions,
	}
	for index, option := range options {
		if option == nil {
			return fmt.Errorf("copy embedded templates: option %d is nil", index)
		}

		option(&config)
	}

	if err := validatePermissions("file", config.filePermissions); err != nil {
		return err
	}
	if err := validatePermissions("directory", config.directoryPermissions); err != nil {
		return err
	}

	return fs.WalkDir(source, ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("walk embedded templates at %q: %w", path, walkErr)
		}

		target := filepath.Join(destination, filepath.FromSlash(path))
		if entry.IsDir() {
			if err := os.MkdirAll(target, config.directoryPermissions); err != nil {
				return fmt.Errorf("create destination directory %q: %w", target, err)
			}

			return nil
		}

		contents, err := fs.ReadFile(source, path)
		if err != nil {
			return fmt.Errorf("read embedded template %q: %w", path, err)
		}

		tmpl, err := template.New(path).Option("missingkey=error").Parse(string(contents))
		if err != nil {
			return fmt.Errorf("parse embedded template %q: %w", path, err)
		}

		var rendered bytes.Buffer
		if err := tmpl.Execute(&rendered, data); err != nil {
			return fmt.Errorf("render embedded template %q: %w", path, err)
		}

		if err := os.WriteFile(target, rendered.Bytes(), config.filePermissions); err != nil {
			return fmt.Errorf("write rendered template %q: %w", target, err)
		}

		return nil
	})
}

func validatePermissions(kind string, permissions fs.FileMode) error {
	if permissions&^fs.ModePerm != 0 {
		return fmt.Errorf(
			"copy embedded templates: invalid %s permissions %v",
			kind,
			permissions,
		)
	}

	return nil
}
