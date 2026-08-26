package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// FS is the filesystem store of the dev contour.
type FS struct {
	dir string
}

// NewFS roots the store.
func NewFS(dir string) *FS { return &FS{dir: dir} }

// Put writes the blob under namespace/location.
func (f *FS) Put(_ context.Context, namespace, location string, r io.Reader) (int64, error) {
	path, err := f.path(namespace, location)
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return 0, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640) //nolint:gosec // confined by path()
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(file, r)
	if cerr := file.Close(); err == nil {
		err = cerr
	}
	return n, err
}

// Get opens the blob.
func (f *FS) Get(_ context.Context, namespace, location string) (io.ReadCloser, error) {
	path, err := f.path(namespace, location)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path) //nolint:gosec // confined by path()
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return file, err
}

// Exists reports presence.
func (f *FS) Exists(_ context.Context, namespace, location string) (bool, error) {
	path, err := f.path(namespace, location)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

// Delete removes the bytes; absence is fine.
func (f *FS) Delete(_ context.Context, namespace, location string) error {
	path, err := f.path(namespace, location)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// List walks the namespace's tree under the prefix.
func (f *FS) List(_ context.Context, namespace, prefix string) ([]string, error) {
	root, err := f.path(namespace, "")
	if err != nil {
		return nil, err
	}
	var out []string
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// A namespace that has never stored anything has no
			// directory; that is an empty listing, not a failure.
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return rerr
		}
		loc := filepath.ToSlash(rel)
		if strings.HasPrefix(loc, prefix) {
			out = append(out, loc)
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return out, err
}

func (f *FS) path(namespace, location string) (string, error) {
	loc := filepath.Clean(strings.TrimPrefix(location, "/"))
	if loc == "." || !filepath.IsLocal(loc) || namespace == "" || strings.ContainsAny(namespace, "/\\") {
		return "", fmt.Errorf("bad blob address %q/%q", namespace, location)
	}
	return filepath.Join(f.dir, namespace, filepath.FromSlash(loc)), nil
}
