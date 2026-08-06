package blob

import (
	"context"
	"io"
)

// Permission is what a guarded store asks before it does anything.
//
// It is an interface here rather than the auth package's own type
// because blobs know nothing about identities, roles or grants — only
// that somebody decides. auth answers it; a test answers it with yes.
type Permission interface {
	// MayRead reports whether the caller may read bytes at all.
	MayRead(ctx context.Context) error
	// MayWrite reports whether the caller may add bytes.
	MayWrite(ctx context.Context) error
	// MayDelete reports whether the caller may remove them.
	MayDelete(ctx context.Context) error
}

// Guarded is a store with a question asked first — the same shape auth
// puts in front of the kernel, and for the same reason: the store it
// wraps has no idea it is guarded, so there is one place where the
// unguarded one is handed out.
//
// The permission is coarse ON PURPOSE. An id is opaque and carries no
// path, so there is nothing to confine a grant to; a caller either may
// read bytes or may not. What keeps that from being too much is that an
// id is not guessable and is only learned by reading a resource that
// names it — so reading blobs is only useful to somebody already allowed
// to read where the ids are written down.
type Guarded struct {
	store Store
	may   Permission
}

// Guard puts a permission in front of a store.
func Guard(store Store, may Permission) Guarded {
	return Guarded{store: store, may: may}
}

func (g Guarded) Create(ctx context.Context) (Writer, error) {
	if err := g.may.MayWrite(ctx); err != nil {
		return nil, err
	}

	return g.store.Create(ctx)
}

func (g Guarded) Open(ctx context.Context, id Id, offset uint64) (io.ReadCloser, Info, error) {
	if err := g.may.MayRead(ctx); err != nil {
		return nil, Info{}, err
	}

	return g.store.Open(ctx, id, offset)
}

func (g Guarded) Stat(ctx context.Context, id Id) (Info, error) {
	if err := g.may.MayRead(ctx); err != nil {
		return Info{}, err
	}

	return g.store.Stat(ctx, id)
}

func (g Guarded) Delete(ctx context.Context, id Id) error {
	if err := g.may.MayDelete(ctx); err != nil {
		return err
	}

	return g.store.Delete(ctx, id)
}

// Close releases the store beneath. A guard owns no resources of its own.
func (g Guarded) Close() error { return g.store.Close() }
