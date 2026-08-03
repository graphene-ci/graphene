package builtin

import (
	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/auth"
)

// RoleKernel is what a kernel needs in order to be a kernel: renew its
// lease, describe itself, and run the processes placed on it.
//
// It ships with the binary for the same reason the kind definitions do.
// These grants are not a policy anyone chose — they are the consequence
// of what a kernel does, and every one of them was learned by watching a
// kernel fail without it. Leaving them to be typed at join time means
// every installation reinvents them, and gets them subtly wrong: too
// narrow and the kernel silently stops reporting, too wide and a machine
// at the edge of the network can read the whole system.
//
// Joining a worker is then one resource: an Identity carrying this role.
const RoleKernel = "system-kernel"

// Roles returns the roles that ship in the binary. They are ensured at
// every start, so an installation cannot drift away from them — the same
// bargain as the built-in kinds: the binary is the author.
func Roles() []*graphenepbv1.Resource {
	return []*graphenepbv1.Resource{{
		Key:  &graphenepbv1.Key{Kind: KindRole, Path: []string{RoleKernel}},
		Spec: auth.GrantsToSpec(kernelGrants()),
	}}
}

func kernelGrants() []auth.Grant {
	self := []string{principalName}

	return []auth.Grant{
		{
			// Its own lease, and the constraint is on the FIELD rather
			// than the path: expiry is judged by whoever holds the store,
			// and a kernel that could write another's lease could keep a
			// dead machine looking alive.
			Verbs: []auth.Verb{auth.VerbGet, auth.VerbPut},
			Kind:  KindKernelLease,
			Where: []auth.Constraint{{Path: "spec.kernel", Equal: principalName}},
		},
		{
			// Its own record, spec only. What a kernel IS, it says; what
			// it is judged to be — online — stays with the controller
			// that judges it.
			Verbs:      []auth.Verb{auth.VerbGet, auth.VerbPut},
			Kind:       KindKernel,
			PathPrefix: self,
			Parts:      []auth.Part{auth.PartSpec},
		},
		{
			// The processes placed on it: watch to learn of them, status
			// to report them. Not the spec — an agent that could rewrite
			// what it was told to run would be arguing with its orders.
			Verbs:      []auth.Verb{auth.VerbGet, auth.VerbList, auth.VerbWatch, auth.VerbPut},
			Kind:       KindProcess,
			PathPrefix: self,
			Parts:      []auth.Part{auth.PartStatus},
		},
		{
			// Read the bytes it is asked to run. Object-less: blobs are
			// named by opaque ids, so there is no path to confine to, and
			// an id is not guessable.
			Verbs: []auth.Verb{auth.VerbGet},
			Kind:  KindBlob,
		},
	}
}

// principalName is the grant variable a kernel's own name interpolates
// into; spelled out here so the role reads as what it is.
const principalName = "${principal.name}"
