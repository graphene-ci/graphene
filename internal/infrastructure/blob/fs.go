package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
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

func (f *FS) path(namespace, location string) (string, error) {
	loc := filepath.Clean(strings.TrimPrefix(location, "/"))
	if loc == "." || !filepath.IsLocal(loc) || namespace == "" || strings.ContainsAny(namespace, "/\\") {
		return "", fmt.Errorf("bad blob address %q/%q", namespace, location)
	}
	return filepath.Join(f.dir, namespace, filepath.FromSlash(loc)), nil
}
