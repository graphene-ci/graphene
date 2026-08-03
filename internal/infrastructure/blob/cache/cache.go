// Package cache materializes blobs on local disk, once.
//
// A kernel may be asked to run the same bytes a thousand times, and the
// link they arrive over may be an ssh pipe on a bad connection. Blob ids
// are opaque and immutable, so the file that was fetched under an id is
// the file for that id, forever — which is what makes the cache a plain
// directory with no bookkeeping.
package cache

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/graphene-ci/graphene/internal/core/blob"
)

const (
	dirMode = 0o700
	// fileMode is read+execute: everything the cache holds is there to be
	// run, and nothing should be able to rewrite it in place afterwards.
	fileMode = 0o500
)

// Cache is a directory of fetched blobs, keyed by id.
type Cache struct {
	dir    string
	source blob.Reader
}

func New(dir string, source blob.Reader) *Cache {
	return &Cache{dir: dir, source: source}
}

// Fetch returns the local path of a blob, downloading it the first time.
//
// Integrity is checked against the checksum the source declares, and a
// blob that fails is not stored: the point of running someone's bytes is
// that they are the bytes that were meant, and a truncated download that
// executed would be worse than one that failed.
func (c *Cache) Fetch(ctx context.Context, id string) (string, error) {
	path := filepath.Join(c.dir, id)
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}

	if err := os.MkdirAll(c.dir, dirMode); err != nil {
		return "", fmt.Errorf("cache: create %s: %w", c.dir, err)
	}

	reader, info, err := c.source.Open(ctx, id)
	if err != nil {
		return "", fmt.Errorf("cache: open blob %s: %w", id, err)
	}

	defer func() { _ = reader.Close() }()

	// Downloaded beside the destination and renamed into place: a reader
	// must never find a half-written file under a name that promises
	// whole content.
	temp, err := os.CreateTemp(c.dir, "."+id+".*")
	if err != nil {
		return "", fmt.Errorf("cache: temporary file: %w", err)
	}

	defer func() {
		_ = temp.Close()
		_ = os.Remove(temp.Name()) // a no-op once renamed
	}()

	sum := sha256.New()
	if _, err := io.Copy(io.MultiWriter(temp, sum), reader); err != nil {
		return "", fmt.Errorf("cache: download blob %s: %w", id, err)
	}

	if len(info.SHA256) > 0 && !equalBytes(sum.Sum(nil), info.SHA256) {
		return "", fmt.Errorf("cache: blob %s: %w", id, blob.ErrChecksumMismatch)
	}

	if err := temp.Chmod(fileMode); err != nil {
		return "", fmt.Errorf("cache: permissions: %w", err)
	}

	if err := temp.Close(); err != nil {
		return "", fmt.Errorf("cache: close: %w", err)
	}

	if err := os.Rename(temp.Name(), path); err != nil && !errors.Is(err, os.ErrExist) {
		return "", fmt.Errorf("cache: install blob %s: %w", id, err)
	}

	return path, nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
