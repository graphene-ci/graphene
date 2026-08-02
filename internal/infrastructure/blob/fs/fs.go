// Package fs is the filesystem adapter of the blob port: one file per
// blob under a fan-out directory, in-flight uploads in a temp dir, commit
// by fsync+rename — the standard CAS-on-disk shape.
//
// Layout:
//
//	<root>/tmp/<random>          — in-flight uploads
//	<root>/blobs/<id[:2]>/<id>   — committed content
//	<root>/blobs/<id[:2]>/<id>.meta — size + sha256 (checksum is metadata,
//	  not the address; ids are opaque and random)
package fs

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/graphene-ci/graphene/internal/core/blob"
)

const (
	dirMode  = 0o750
	fileMode = 0o600

	idBytes    = 16
	fanoutLen  = 2
	metaSuffix = ".meta"
	metaLen    = 8 + sha256.Size
)

var errClosed = errors.New("blob fs: writer already finished")

// Store implements blob.Store on a local directory.
type Store struct {
	root string
}

// Open prepares the directory layout.
func Open(root string) (*Store, error) {
	for _, dir := range []string{filepath.Join(root, "tmp"), filepath.Join(root, "blobs")} {
		if err := os.MkdirAll(dir, dirMode); err != nil {
			return nil, fmt.Errorf("blob fs: mkdir %s: %w", dir, err)
		}
	}

	return &Store{root: root}, nil
}

// Close implements the port; the adapter holds no long-lived handles.
func (s *Store) Close() error { return nil }

// Create implements blob.Store.
func (s *Store) Create(_ context.Context) (blob.Writer, error) {
	id, err := newID()
	if err != nil {
		return nil, err
	}

	tmpPath := filepath.Join(s.root, "tmp", id)

	file, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fileMode)
	if err != nil {
		return nil, fmt.Errorf("blob fs: create temp: %w", err)
	}

	return &writer{store: s, id: id, tmpPath: tmpPath, file: file, digest: sha256.New()}, nil
}

// Open implements blob.Store.
func (s *Store) Open(_ context.Context, id string, offset uint64) (io.ReadCloser, blob.Info, error) {
	info, err := s.stat(id)
	if err != nil {
		return nil, blob.Info{}, err
	}

	file, err := os.Open(s.blobPath(id))
	if err != nil {
		return nil, blob.Info{}, fmt.Errorf("blob fs: open: %w", err)
	}

	if offset > 0 {
		if _, err := file.Seek(int64(offset), io.SeekStart); err != nil { //nolint:gosec // sizes fit int64 by construction
			_ = file.Close()

			return nil, blob.Info{}, fmt.Errorf("blob fs: seek: %w", err)
		}
	}

	return file, info, nil
}

// Stat implements blob.Store.
func (s *Store) Stat(_ context.Context, id string) (blob.Info, error) {
	return s.stat(id)
}

// Delete implements blob.Store.
func (s *Store) Delete(_ context.Context, id string) error {
	if err := os.Remove(s.blobPath(id)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return blob.ErrNotFound
		}

		return fmt.Errorf("blob fs: delete: %w", err)
	}

	_ = os.Remove(s.metaPath(id))

	return nil
}

func (s *Store) stat(id string) (blob.Info, error) {
	raw, err := os.ReadFile(s.metaPath(id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return blob.Info{}, blob.ErrNotFound
		}

		return blob.Info{}, fmt.Errorf("blob fs: read meta: %w", err)
	}

	if len(raw) != metaLen {
		return blob.Info{}, fmt.Errorf("blob fs: corrupt meta for %s", id) //nolint:err113 // corruption carries the id
	}

	return blob.Info{
		ID:     id,
		Size:   binary.BigEndian.Uint64(raw[:8]),
		SHA256: raw[8:],
	}, nil
}

func (s *Store) blobPath(id string) string {
	return filepath.Join(s.root, "blobs", id[:fanoutLen], id)
}

func (s *Store) metaPath(id string) string {
	return s.blobPath(id) + metaSuffix
}

func newID() (string, error) {
	raw := make([]byte, idBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("blob fs: id: %w", err)
	}

	return hex.EncodeToString(raw), nil
}

// --- writer -------------------------------------------------------------

type writer struct {
	store   *Store
	id      string
	tmpPath string
	file    *os.File
	digest  hash.Hash
	size    uint64
	done    bool
}

func (w *writer) Write(p []byte) (int, error) {
	if w.done {
		return 0, errClosed
	}

	n, err := w.file.Write(p)
	w.size += uint64(n) //nolint:gosec // n is non-negative
	_, _ = w.digest.Write(p[:n])

	if err != nil {
		return n, fmt.Errorf("blob fs: write: %w", err)
	}

	return n, nil
}

func (w *writer) Commit(expectedSHA256 []byte, expectedSize uint64) (blob.Info, error) {
	if w.done {
		return blob.Info{}, errClosed
	}

	w.done = true
	sum := w.digest.Sum(nil)

	if err := w.verify(expectedSHA256, expectedSize, sum); err != nil {
		w.discard()

		return blob.Info{}, err
	}

	if err := w.seal(sum); err != nil {
		w.discard()

		return blob.Info{}, err
	}

	return blob.Info{ID: w.id, Size: w.size, SHA256: sum}, nil
}

func (w *writer) verify(expectedSHA256 []byte, expectedSize uint64, sum []byte) error {
	if len(expectedSHA256) > 0 && !bytes.Equal(expectedSHA256, sum) {
		return blob.ErrChecksumMismatch
	}

	if expectedSize > 0 && expectedSize != w.size {
		return blob.ErrChecksumMismatch
	}

	return nil
}

func (w *writer) seal(sum []byte) error {
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("blob fs: sync: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("blob fs: close: %w", err)
	}

	final := w.store.blobPath(w.id)
	if err := os.MkdirAll(filepath.Dir(final), dirMode); err != nil {
		return fmt.Errorf("blob fs: mkdir fanout: %w", err)
	}

	meta := make([]byte, metaLen)
	binary.BigEndian.PutUint64(meta, w.size)
	copy(meta[8:], sum)

	if err := os.WriteFile(w.store.metaPath(w.id), meta, fileMode); err != nil {
		return fmt.Errorf("blob fs: write meta: %w", err)
	}

	if err := os.Rename(w.tmpPath, final); err != nil {
		return fmt.Errorf("blob fs: commit rename: %w", err)
	}

	return nil
}

func (w *writer) Abort() error {
	if w.done {
		return nil
	}

	w.done = true
	w.discard()

	return nil
}

func (w *writer) discard() {
	_ = w.file.Close()
	_ = os.Remove(w.tmpPath)
	_ = os.Remove(w.store.metaPath(w.id))
}
