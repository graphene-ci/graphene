// Package blob is the port for the byte side of the system: content
// storage addressed by opaque server-issued ids (R20).
//
// This is deliberately NOT the resource API: resource values travel the
// MVCC log and every watch stream — bytes do not belong there (the reason
// k8s keeps bytes out of etcd). The transport is the irreducible core;
// blob LIFECYCLE (metadata, GC by reachability from resources' blob_refs,
// retention) is resource/controller land built on top.
//
// Digests are integrity checksums here, never addresses.
package blob

import (
	"context"
	"errors"
	"io"
)

var (
	// ErrNotFound — no blob under the id.
	ErrNotFound = errors.New("blob: not found")
	// ErrChecksumMismatch — the declared checksum does not match the bytes.
	ErrChecksumMismatch = errors.New("blob: checksum mismatch")
)

// Info describes a stored blob.
type Info struct {
	// ID is the opaque server-issued handle.
	ID string
	// Size in bytes.
	Size uint64
	// SHA256 of the content — integrity metadata, not an address.
	SHA256 []byte
}

// Writer accumulates one blob; exactly one of Commit/Abort must be called.
type Writer interface {
	io.Writer

	// Commit seals the blob and returns its identity. When expectedSHA256
	// is non-empty the server verifies it against the computed checksum
	// (mismatch = ErrChecksumMismatch, nothing is stored).
	Commit(expectedSHA256 []byte, expectedSize uint64) (Info, error)

	// Abort discards everything written.
	Abort() error
}

// Store is the port. All methods are safe for concurrent use.
type Store interface {
	// Create starts a new blob upload.
	Create(ctx context.Context) (Writer, error)

	// Open returns the content reader starting at offset, plus the info.
	// ErrNotFound for unknown ids.
	Open(ctx context.Context, id string, offset uint64) (io.ReadCloser, Info, error)

	// Stat reports the blob's info; ErrNotFound for unknown ids.
	Stat(ctx context.Context, id string) (Info, error)

	// Delete removes the blob (the GC controller's tool).
	Delete(ctx context.Context, id string) error

	Close() error
}
