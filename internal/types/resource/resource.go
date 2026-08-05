package resource

import (
	"slices"

	"github.com/gopherex/schemapb/go/schemapb"
	"github.com/graphene-ci/graphene/internal/types/kind"

	"github.com/graphene-ci/graphene/internal/types/def"
)

// Resource is one admitted instance of a kind: an author's intent,
// together with everything the kernel worked out about it.
//
// Every field below the intent is the kernel's and only the kernel's.
// There is no exported way to build one but Admit and Restore, and Admit
// needs a definition — so a resource that exists is a resource some kind
// described and something checked.
//
// What is NOT here: the store's revisions, which live in types.Stored
// beside the value rather than inside it, and the references the spec
// carries, which are read out of the spec when they are wanted rather
// than copied here to go stale.
type Resource struct {
	intent Intent
	status *schemapb.StructValue
	// finalizers are on the resource and not on the intent, because they
	// are not an author's intent: a claim is placed by whoever will do
	// the cleaning, which is usually somebody else entirely. Claim and
	// Release are the only things that move them.
	finalizers []Finalizer
	generation Generation
	version    def.Version
	deleting   bool
}

// Intent is the part of this that an author asked for.
func (r Resource) Intent() Intent { return r.intent }

// Id is which resource this is.
func (r Resource) Id() Id { return r.intent.id }

// Kind is what it is.
func (r Resource) Kind() kind.Kind { return r.intent.id.Kind() }

// Spec is the asked-for state. Read, do not write: the message is handed
// out rather than cloned.
func (r Resource) Spec() *schemapb.StructValue { return r.intent.spec }

// Status is what the controller reports back, or nil before it has
// reported anything. Read, do not write.
//
// Nil and empty are different answers: nil is "nobody has looked at this
// yet", empty is "somebody looked and found nothing to say".
func (r Resource) Status() *schemapb.StructValue { return r.status }

// Finalizers are the claims that must be released before this may be
// removed.
func (r Resource) Finalizers() []Finalizer { return slices.Clone(r.finalizers) }

// Generation counts intent: it moves when the spec moves and at no other
// time. A controller compares it against what it last acted on.
func (r Resource) Generation() Generation { return r.generation }

// DefinitionVersion is the version of the kind's definition this was
// admitted against.
//
// It is pinned and not looked up, because the definition may have moved
// on since. A resource admitted under v1 is a v1 resource until something
// rewrites it, and anything that reads it has to know which shape it is
// reading.
func (r Resource) DefinitionVersion() def.Version { return r.version }

// IsDeleting reports a resource that has been asked to go away and is
// waiting on its finalizers. It still exists and can still be read; what
// it can no longer do is change its spec.
func (r Resource) IsDeleting() bool { return r.deleting }

// IsZero reports a resource that was never admitted.
func (r Resource) IsZero() bool { return r.intent.IsZero() }

func (r Resource) String() string { return r.intent.id.String() }
