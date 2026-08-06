// Package blob is the byte side of the system, and the port onto it.
//
// A resource is a small value that travels the revision log and every
// watch stream. A blob is somebody's binary. Putting one in the log would
// mean every watcher downloading it to learn that it changed, which is
// why the two are kept apart here the way k8s keeps bytes out of etcd.
//
// What the kernel knows about a blob is a reference in some resource's
// spec. What lives here is the bytes and nothing else: no metadata anyone
// can set, no lifecycle, no garbage collection. Those are questions about
// resources, and they are answered where resources are.
package blob

import (
	"context"
	"errors"
	"io"
)

var (
	// ErrNotFound — nothing is stored under that id.
	ErrNotFound = errors.New("blob: not found")

	// ErrChecksumMismatch — the bytes are not what they were declared to
	// be. Nothing is stored: a blob that arrived wrong and was kept would
	// be a blob somebody later runs.
	ErrChecksumMismatch = errors.New("blob: checksum mismatch")

	// ErrSizeMismatch — fewer or more bytes arrived than were declared.
	ErrSizeMismatch = errors.New("blob: size mismatch")
)

// Info is everything known about a blob without reading it.
type Info struct {
	Id     Id
	Size   uint64
	SHA256 []byte
}

// Store keeps bytes under ids it issues. All methods are safe for
// concurrent use.
//
// There is no Put taking a []byte. A blob is as big as somebody's binary,
// and an interface that took one would put every one of them in memory
// twice — once in the caller and once in the store — at the only moment
// when the machine is already busy.
type Store interface {
	// Create starts one upload. Exactly one of Commit or Abort follows.
	Create(ctx context.Context) (Writer, error)

	// Open reads a blob from offset. ErrNotFound for an unknown id.
	Open(ctx context.Context, id Id, offset uint64) (io.ReadCloser, Info, error)

	// Stat reports what is stored without reading it.
	Stat(ctx context.Context, id Id) (Info, error)

	// Delete removes a blob. Whoever calls it is asserting that nothing
	// refers to it: this store cannot know, because a reference lives in
	// a resource's spec and nothing here reads those.
	Delete(ctx context.Context, id Id) error

	// Close releases the store.
	Close() error
}

// Writer accumulates one blob.
//
// The id does not exist until Commit, on purpose. An id handed out before
// the bytes were whole would be an id somebody could store in a resource,
// pointing at a blob that never finished arriving.
type Writer interface {
	io.Writer

	// Commit seals the blob and names it.
	//
	// A non-empty checksum or a non-zero size is a DECLARATION, and the
	// store refuses the blob when what arrived disagrees. That is the
	// difference between a truncated transfer that fails and one that is
	// stored for somebody to run later.
	Commit(sha256 []byte, size uint64) (Info, error)

	// Abort discards everything written. Calling it after Commit is not
	// an error and does nothing.
	Abort() error
}

// Reader is the read half alone: what a kernel needs when the bytes are
// somebody else's. A worker holds no blob store and still has to run what
// it is told to run.
type Reader interface {
	Open(ctx context.Context, id Id, offset uint64) (io.ReadCloser, Info, error)
	Stat(ctx context.Context, id Id) (Info, error)
}
