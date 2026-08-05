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
	heads     store.Store[def.Head]
	published store.Store[def.Published]
	resources store.Store[resource.Resource]
}

// New puts a kernel on a byte store.
//
// The three codecs are wired here and nowhere else, which is the whole
// mitigation for the one mistake the types cannot catch: codec.Head and
// codec.Definition write the same message and differ only in Id.
func New(bytes kv.Store) Kernel {
	return Kernel{
		heads:     store.New(bytes, codec.Head{}),
		published: store.New(bytes, codec.Definition{}),
		resources: store.New(bytes, codec.Resource{}),
	}
}

// Revision is the store-wide revision as of now.
//
// It is the first of the three lines that take a snapshot, and it is
// first for a reason: taken before the scan, a write that races the scan
// is seen twice rather than not at all.
//
//	at, _ := kernel.Revision(ctx)          // the cursor FIRST
//	for v := range kernel.Scan(ctx, p) {}  // then the snapshot
//	s, _ := kernel.Watch(ctx, p, at)       // then the changes
//
// One counter serves resources and definitions alike, because there is
// one store underneath them.
func (k Kernel) Revision(ctx context.Context) (revision.Revision, error) {
	return k.resources.Revision(ctx)
}
