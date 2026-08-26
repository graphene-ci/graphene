// Package blob is the byte store behind artifacts: content-addressed
// locations, namespaced isolation, one interface — a filesystem for the
// dev contour, S3 for production. Workers never see the store: bytes go
// through the server's door, so store credentials stay on the server.
package blob

import (
	"context"
	"errors"
	"io"
)

// ErrNotFound reports an absent blob.
var ErrNotFound = errors.New("blob not found")

// Store moves and keeps bytes. Locations are store-relative keys
// ("blobs/<hex>"); the namespace isolates tenants.
type Store interface {
	// Put writes the blob; overwriting the same location is a no-op by
	// construction (content-addressed).
	Put(ctx context.Context, namespace, location string, r io.Reader) (int64, error)
	// Get opens the blob for reading; ErrNotFound when absent.
	Get(ctx context.Context, namespace, location string) (io.ReadCloser, error)
	// Exists reports presence without reading.
	Exists(ctx context.Context, namespace, location string) (bool, error)
	// Delete removes the bytes; absence is not an error.
	Delete(ctx context.Context, namespace, location string) error
	// List names every blob under a prefix. Deleting a record has to
	// take its bytes with it, and a record's bytes are only knowable
	// as "everything under my prefix" — old file versions and old
	// indexes included, which no live reference names any more.
	List(ctx context.Context, namespace, prefix string) ([]string, error)

	// TODO(presigned): hand the caller a short-lived URL so big bytes
	// bypass the door without the store credentials leaving the server.
}
