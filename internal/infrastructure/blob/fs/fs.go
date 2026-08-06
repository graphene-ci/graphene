// Package fs keeps blobs in a directory: one file per blob, uploads in
// progress off to one side, and a commit that is a rename.
//
//	<root>/tmp/<id>              being written
//	<root>/blobs/<ab>/<id>       the bytes
//	<root>/blobs/<ab>/<id>.meta  the size and checksum
//
// The fan-out by the first two characters is for the filesystem's sake:
// directories with a million entries are slow to walk on every filesystem
// anyone still runs.
package fs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/graphene-ci/graphene/internal/blob"
)

const (
	dirMode  = 0o750
	fileMode = 0o600

	fanout     = 2
	metaSuffix = ".meta"
	metaBytes  = 8 + sha256.Size
)

var (
	errFinished = errors.New("blob fs: this upload is already finished")
	errCorrupt  = errors.New("blob fs: metadata is not the size it should be")
)

// Store keeps blobs under a directory.
type Store struct{ root string }

// Open prepares the layout. It does not scan what is there: a blob store
// with a million blobs would take a minute to start, and there is nothing
// to learn from the scan that a later Stat does not answer.
func Open(root string) (*Store, error) {
	for _, dir := range []string{filepath.Join(root, "tmp"), filepath.Join(root, "blobs")} {
		if err := os.MkdirAll(dir, dirMode); err != nil {
			return nil, fmt.Errorf("blob fs: %s: %w", dir, err)
		}
	}

	return &Store{root: root}, nil
}

// Close implements the port; nothing is held open between calls.
func (s *Store) Close() error { return nil }

// Create starts one upload.
func (s *Store) Create(_ context.Context) (blob.Writer, error) {
	id, err := blob.Issue()
	if err != nil {
		return nil, fmt.Errorf("blob fs: %w", err)
	}

	temporary := filepath.Join(s.root, "tmp", id.String())

	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fileMode)
	if err != nil {
		return nil, fmt.Errorf("blob fs: begin upload: %w", err)
	}

	return &writer{store: s, id: id, temporary: temporary, file: file, digest: sha256.New()}, nil
}

// Open reads a blob from offset.
func (s *Store) Open(_ context.Context, id blob.Id, offset uint64) (io.ReadCloser, blob.Info, error) {
	if err := usable(id); err != nil {
		return nil, blob.Info{}, err
	}

	info, err := s.stat(id)
	if err != nil {
		return nil, blob.Info{}, err
	}

	file, err := os.Open(s.blobPath(id))
	if err != nil {
		return nil, blob.Info{}, fmt.Errorf("blob fs: open %s: %w", id, err)
	}

	if offset > 0 {
		if _, err := file.Seek(int64(offset), io.SeekStart); err != nil { //nolint:gosec // a file offset fits
			_ = file.Close()

			return nil, blob.Info{}, fmt.Errorf("blob fs: seek %s: %w", id, err)
		}
	}

	return file, info, nil
}

// Stat reports what is stored without reading it.
func (s *Store) Stat(_ context.Context, id blob.Id) (blob.Info, error) {
	if err := usable(id); err != nil {
		return blob.Info{}, err
	}

	return s.stat(id)
}

// Delete removes a blob.
//
// The metadata goes FIRST. A blob is what its metadata says exists, so
// removing that is what makes it gone; a crash after it leaves bytes
// nobody can name, which is waste rather than damage. The other order
// would leave a blob that stats and cannot be read.
func (s *Store) Delete(_ context.Context, id blob.Id) error {
	if err := usable(id); err != nil {
		return err
	}

	if err := os.Remove(s.metaPath(id)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return blob.ErrNotFound
		}

		return fmt.Errorf("blob fs: delete %s: %w", id, err)
	}

	_ = os.Remove(s.blobPath(id))

	return nil
}

func (s *Store) stat(id blob.Id) (blob.Info, error) {
	raw, err := os.ReadFile(s.metaPath(id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return blob.Info{}, blob.ErrNotFound
		}

		return blob.Info{}, fmt.Errorf("blob fs: read metadata of %s: %w", id, err)
	}

	if len(raw) != metaBytes {
		return blob.Info{}, fmt.Errorf("%w: %s", errCorrupt, id)
	}

	return blob.Info{
		Id:     id,
		Size:   binary.BigEndian.Uint64(raw[:8]),
		SHA256: raw[8:],
	}, nil
}

func (s *Store) blobPath(id blob.Id) string {
	text := id.String()

	return filepath.Join(s.root, "blobs", text[:fanout], text)
}

func (s *Store) metaPath(id blob.Id) string {
	return s.blobPath(id) + metaSuffix
}

// usable re-checks an id before it becomes a path.
//
// The type says where a value came from; it cannot say it was not forged,
// because Id("../../etc/passwd") is a conversion anybody may write. Here
// is where that stops mattering, and an id that would not pass its own
// rules is simply a blob nobody has.
func usable(id blob.Id) error {
	if _, err := blob.NewId(id.String()); err != nil {
		return blob.ErrNotFound
	}

	return nil
}

// writer accumulates one blob and names it at the end.
type writer struct {
	store     *Store
	id        blob.Id
	temporary string
	file      *os.File
	digest    hash.Hash
	size      uint64
	finished  bool
}

func (w *writer) Write(p []byte) (int, error) {
	if w.finished {
		return 0, errFinished
	}

	written, err := w.file.Write(p)
	w.size += uint64(written) //nolint:gosec // a count of bytes written is not negative

	_, _ = w.digest.Write(p[:written])

	if err != nil {
		return written, fmt.Errorf("blob fs: write %s: %w", w.id, err)
	}

	return written, nil
}

func (w *writer) Commit(sha256Sum []byte, size uint64) (blob.Info, error) {
	if w.finished {
		return blob.Info{}, errFinished
	}

	w.finished = true
	sum := w.digest.Sum(nil)

	if err := w.agrees(sha256Sum, size, sum); err != nil {
		w.discard()

		return blob.Info{}, err
	}

	if err := w.seal(sum); err != nil {
		w.discard()

		return blob.Info{}, err
	}

	return blob.Info{Id: w.id, Size: w.size, SHA256: sum}, nil
}

// agrees checks what arrived against what was declared. Either
// declaration may be absent — a sender that has not read the bytes cannot
// know them — but a declaration that is present and wrong is a refusal,
// and the two disagreements are told apart because they mean different
// things to whoever has to look into it.
func (w *writer) agrees(declaredSum []byte, declaredSize uint64, sum []byte) error {
	if len(declaredSum) > 0 && !bytes.Equal(declaredSum, sum) {
		return blob.ErrChecksumMismatch
	}

	if declaredSize > 0 && declaredSize != w.size {
		return fmt.Errorf("%w: %d bytes declared, %d arrived", blob.ErrSizeMismatch, declaredSize, w.size)
	}

	return nil
}

// seal makes the blob real.
//
// The bytes are put in place BEFORE the metadata, because the metadata is
// what makes a blob exist: crash in between and there is an unnamed file
// nobody will ever ask for. The other order would publish a blob that
// stats and cannot be read, which is the failure that wakes somebody up.
func (w *writer) seal(sum []byte) error {
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("blob fs: sync %s: %w", w.id, err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("blob fs: close %s: %w", w.id, err)
	}

	final := w.store.blobPath(w.id)
	if err := os.MkdirAll(filepath.Dir(final), dirMode); err != nil {
		return fmt.Errorf("blob fs: %s: %w", filepath.Dir(final), err)
	}

	if err := os.Rename(w.temporary, final); err != nil {
		return fmt.Errorf("blob fs: place %s: %w", w.id, err)
	}

	meta := make([]byte, metaBytes)
	binary.BigEndian.PutUint64(meta, w.size)
	copy(meta[8:], sum)

	if err := os.WriteFile(w.store.metaPath(w.id), meta, fileMode); err != nil {
		return fmt.Errorf("blob fs: name %s: %w", w.id, err)
	}

	return nil
}

func (w *writer) Abort() error {
	if w.finished {
		return nil
	}

	w.finished = true
	w.discard()

	return nil
}

func (w *writer) discard() {
	_ = w.file.Close()
	_ = os.Remove(w.temporary)
	_ = os.Remove(w.store.metaPath(w.id))
	_ = os.Remove(w.store.blobPath(w.id))
}
