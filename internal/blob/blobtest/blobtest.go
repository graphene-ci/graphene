// Package blobtest is what decides whether something is a blob.Store.
//
// The port is an interface, and an interface only says which methods
// exist. What matters about a blob store is behavior: that an id is
// issued and not chosen, that bytes which arrived wrong are refused
// rather than kept, that a blob nobody named cannot be read. None of that
// is in a method signature.
//
// Written against the PORT and never against an implementation, so
// whatever passes it is a store — the one on a filesystem here, and the
// one that reaches a kernel over a link next.
package blobtest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/graphene-ci/graphene/internal/blob"
)

// Factory opens stores for the suite to work on.
type Factory struct {
	// Open returns an empty store. Every subtest gets a fresh one.
	Open func(t *testing.T) blob.Store
}

// Run puts a store through the whole port.
func Run(t *testing.T, factory Factory) {
	t.Helper()

	if factory.Open == nil {
		t.Fatal("blobtest: Factory.Open is required")
	}

	t.Run("round trip", func(t *testing.T) { testRoundTrip(t, factory) })
	t.Run("declarations", func(t *testing.T) { testDeclarations(t, factory) })
	t.Run("absence", func(t *testing.T) { testAbsence(t, factory) })
	t.Run("ids", func(t *testing.T) { testIds(t, factory) })
	t.Run("removal", func(t *testing.T) { testRemoval(t, factory) })
}

// Bytes go in, the same bytes come out, and the store says what it holds
// without being asked to read it.
func testRoundTrip(t *testing.T, factory Factory) {
	t.Helper()

	store := open(t, factory)
	content := []byte("the bytes somebody meant")

	info := write(t, store, content, nil, 0)
	if info.Size != uint64(len(content)) {
		t.Fatalf("size: %d, want %d", info.Size, len(content))
	}

	sum := sha256.Sum256(content)
	if !bytes.Equal(info.SHA256, sum[:]) {
		t.Fatal("the checksum is not of the bytes that were written")
	}

	// Read whole, and read from the middle: a resumed download asks for
	// the second half and must not be given the first.
	if got := read(t, store, info.Id, 0); !bytes.Equal(got, content) {
		t.Fatalf("read back %q", got)
	}

	const partway = 4
	if got := read(t, store, info.Id, partway); !bytes.Equal(got, content[partway:]) {
		t.Fatalf("read from an offset: %q", got)
	}

	stated, err := store.Stat(context.Background(), info.Id)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	if stated.Size != info.Size || !bytes.Equal(stated.SHA256, info.SHA256) {
		t.Fatalf("stat disagrees with the commit: %+v vs %+v", stated, info)
	}
}

// A declaration that does not match what arrived is a refusal, and
// nothing is stored — a blob kept after arriving wrong is a blob somebody
// runs later.
func testDeclarations(t *testing.T, factory Factory) {
	t.Helper()

	store := open(t, factory)
	content := []byte("declared")
	sum := sha256.Sum256(content)

	// Declaring correctly is allowed and changes nothing.
	info := write(t, store, content, sum[:], uint64(len(content)))
	if got := read(t, store, info.Id, 0); !bytes.Equal(got, content) {
		t.Fatalf("declared upload read back as %q", got)
	}

	wrong := sha256.Sum256([]byte("something else"))
	if _, err := attempt(store, content, wrong[:], 0); !errors.Is(err, blob.ErrChecksumMismatch) {
		t.Fatalf("wrong checksum: got %v, want ErrChecksumMismatch", err)
	}

	// A size disagreement is its OWN answer. It is a different fault with
	// a different cause — a truncated transfer, not corrupted bytes — and
	// whoever reads the error has to be told which.
	if _, err := attempt(store, content, nil, uint64(len(content)+1)); !errors.Is(err, blob.ErrSizeMismatch) {
		t.Fatalf("wrong size: got %v, want ErrSizeMismatch", err)
	}
}

// What was refused, and what was never written, are both nothing.
func testAbsence(t *testing.T, factory Factory) {
	t.Helper()

	store := open(t, factory)

	unused, err := blob.Issue()
	if err != nil {
		t.Fatalf("issue: %v", err)
	}

	if _, err := store.Stat(context.Background(), unused); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("stat of an unused id: got %v, want ErrNotFound", err)
	}

	if _, _, err := store.Open(context.Background(), unused, 0); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("open of an unused id: got %v, want ErrNotFound", err)
	}

	// An abandoned upload leaves nothing behind, and the id it would have
	// had is an id nobody has.
	writer, err := store.Create(context.Background())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if _, err := writer.Write([]byte("abandoned")); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := writer.Abort(); err != nil {
		t.Fatalf("abort: %v", err)
	}

	// Aborting twice is not an error: a caller unwinding does not have to
	// remember whether it already did.
	if err := writer.Abort(); err != nil {
		t.Fatalf("abort twice: %v", err)
	}
}

// An id is issued, never chosen. One that was written by hand — a path, a
// traversal, a guess — is a blob nobody has, and never a file somewhere
// it should not be.
func testIds(t *testing.T, factory Factory) {
	t.Helper()

	store := open(t, factory)

	for _, forged := range []string{
		"../../etc/passwd",
		"..",
		"",
		"NOTHEX",
		"deadbeef", // right alphabet, wrong length
	} {
		if _, err := store.Stat(context.Background(), blob.Id(forged)); !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("stat of %q: got %v, want ErrNotFound", forged, err)
		}

		if _, _, err := store.Open(context.Background(), blob.Id(forged), 0); !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("open of %q: got %v, want ErrNotFound", forged, err)
		}
	}
}

// Removing a blob makes it gone, and removing it twice says so.
func testRemoval(t *testing.T, factory Factory) {
	t.Helper()

	store := open(t, factory)
	info := write(t, store, []byte("temporary"), nil, 0)

	if err := store.Delete(context.Background(), info.Id); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if _, err := store.Stat(context.Background(), info.Id); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("stat after delete: got %v, want ErrNotFound", err)
	}

	if err := store.Delete(context.Background(), info.Id); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("delete twice: got %v, want ErrNotFound", err)
	}
}

func open(t *testing.T, factory Factory) blob.Store {
	t.Helper()

	store := factory.Open(t)
	t.Cleanup(func() { _ = store.Close() })

	return store
}

// write uploads and fails the test if it could not.
func write(t *testing.T, store blob.Store, content, sum []byte, size uint64) blob.Info {
	t.Helper()

	info, err := attempt(store, content, sum, size)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}

	return info
}

// attempt uploads and hands back whatever happened.
func attempt(store blob.Store, content, sum []byte, size uint64) (blob.Info, error) {
	writer, err := store.Create(context.Background())
	if err != nil {
		return blob.Info{}, fmt.Errorf("create: %w", err)
	}

	if _, err := writer.Write(content); err != nil {
		_ = writer.Abort()

		return blob.Info{}, fmt.Errorf("write: %w", err)
	}

	info, err := writer.Commit(sum, size)
	if err != nil {
		return blob.Info{}, fmt.Errorf("commit: %w", err)
	}

	return info, nil
}

func read(t *testing.T, store blob.Store, id blob.Id, offset uint64) []byte {
	t.Helper()

	reader, _, err := store.Open(context.Background(), id, offset)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	defer func() { _ = reader.Close() }()

	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	return content
}
