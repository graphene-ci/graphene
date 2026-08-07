// Package kernel is the control kernel: the one door onto the store.
//
// It is thin on purpose. Everything it does is already written somewhere
// else — resource.Admit decides what a write becomes, def.New decides
// what a kind is, store.Store decides how either reaches bytes — and what
// is left here is the joining: which of them to call, in what order, and
// what to refuse before calling any of them.
//
// There is no registry. There used to be, in the old code, and its jobs
// went one by one to the pieces below: the typed store took its encoding,
// the head record took its index, and def took its vocabulary. What was
// left was a name, so the name went too.
package kernel

import (
	"context"

	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/store/codec"
	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Kernel keeps three kinds of record over one byte store.
//
//	heads      Kind/<kind>            the definition that is current
//	published  Definition/<kind>/v<n> the definition as it was
//	resources  <kind>/<path…>         the instances
//
// Three typed stores and not three databases: one key space, three
// decoders. The split is by VALUE TYPE and it is what the compiler holds
// on to — a head and a published definition are the same shape and differ
// only in where they belong, so wiring the wrong codec in would otherwise
// write every version over the head.
//
// It is a value. Nothing here is mutable and nothing here is shared: the
// state is all in the byte store underneath, which is where the mutex
// lives. A Kernel copied is a Kernel that talks to the same store.
type Kernel struct {
	// bytes is the store itself, kept for the one thing the typed views
	// cannot do: begin a transaction.
	bytes kv.Store

	heads     store.Store[def.Head]
	published store.Store[def.Published]
	resources store.Store[resource.Resource]

	// by is who this kernel's writes are made by. Empty is the kernel
	// itself, which is what a store being bootstrapped and a kernel
	// writing down that it exists both are.
	by resource.Author
}

// As is this kernel, writing on somebody's behalf.
//
// The same shape the guard has, and for the same reason: who is calling
// is known once, at the edge, and everything below is the same code
// either way. A parameter on every write would be a parameter every
// implementation of every port had to carry, including the ones across a
// link — where the far side works the author out from the credential and
// would have to ignore what it was sent.
func (k Kernel) As(by resource.Author) Kernel {
	k.by = by

	return k
}

// New puts a kernel on a byte store.
//
// The three codecs are wired here and nowhere else, which is the whole
// mitigation for the one mistake the types cannot catch: codec.Head and
// codec.Definition write the same message and differ only in Id.
func New(bytes kv.Store) Kernel {
	return Kernel{
		bytes:     bytes,
		heads:     store.New(bytes, codec.Head{}),
		published: store.New(bytes, codec.Definition{}),
		resources: store.New(bytes, codec.Resource{}),
	}
}

// change runs work as ONE change: everything it reads and everything it
// writes is the same moment.
//
// This is what makes a check a guarantee rather than a hope. A rule about
// more than one record — "refuse to point at what is not there", "refuse
// to remove what is pointed at" — is correct only when nothing can land
// between the reading and the writing, and outside a transaction
// something always can.
//
// The kernel handed to the work is THIS kernel bound to the transaction:
// every method it has means the same thing inside as outside, and none of
// them had to be written twice. What it cannot do is watch, and the store
// says so in those words.
func (k Kernel) change(ctx context.Context, work func(inside Kernel) error) error {
	return k.bytes.Do(ctx, func(tx kv.Tx) error {
		return work(k.in(tx))
	})
}

// in is this kernel, reading and writing through one transaction.
func (k Kernel) in(tx kv.Tx) Kernel {
	k.heads = store.New(tx, codec.Head{})
	k.published = store.New(tx, codec.Definition{})
	k.resources = store.New(tx, codec.Resource{})

	return k
}

// Revision is the store-wide revision as of now.
//
// It is the first of the three lines that take a snapshot, and it is
// first for a reason: taken before the scan, a write that races the scan
// is seen twice rather than not at all.
//
//	at, _ := kernel.Revision(ctx)          // the cursor FIRST
//	for v := range kernel.List(ctx, p) {}  // then the snapshot
//	s, _ := kernel.Watch(ctx, p, at)       // then the changes
//
// One counter serves resources and definitions alike, because there is
// one store underneath them.
func (k Kernel) Revision(ctx context.Context) (revision.Revision, error) {
	return k.resources.Revision(ctx)
}
