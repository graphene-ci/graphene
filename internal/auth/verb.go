// Package auth decides what a caller may do.
//
// It is a wrapper around the kernel, the way a cache is a wrapper around
// a store, and for the same reason: the thing it guards has no idea it is
// guarded, and there is exactly one place — the composition root — where
// the unguarded one is handed out.
//
// That shape also settles the question that would otherwise be circular.
// Authorising a request means reading an Identity, and reading is a
// request. A guard reads identities through the kernel BELOW it, so the
// regress is closed by construction rather than by remembering not to
// take it.
//
// What is NOT here is who the caller is. Establishing that is a
// transport's job — a certificate, a token — and by the time anything
// here runs, the answer is already in hand.
package auth

import (
	"fmt"
	"strings"
)

// Verb is one thing a caller may be permitted to do.
//
// The verbs are the kernel's methods, one for one, and that is the whole
// point of there being a method per party: Report exists because a
// controller writes status and not spec, Claim exists because whoever
// cleans up is not whoever asked. Permission is then the method, and no
// grant has to describe which PART of a resource a write touched.
//
// The old code did describe parts, and had to diff the incoming resource
// against the stored one to find out which had changed. That diff is what
// this replaces.
type Verb uint8

// Every verb there is. Adding one to the kernel and not to this list
// leaves a method nothing can grant, which fails closed.
const (
	// NoVerb is what an unparsed verb has. Never granted.
	NoVerb Verb = iota

	// Get, List and Watch are the three ways of reading, and they are
	// three because they differ in what they expose: Get needs to know
	// the name already, List hands out the names, and Watch keeps
	// handing them out for as long as it is open.
	Get
	List
	Watch

	// Put writes intent — the spec, and nothing else.
	Put
	// Report writes what a controller found.
	Report
	// Claim and Release place and remove a hold on a deletion.
	Claim
	Release
	// Delete asks a resource to go away, which it may do at once or after
	// the holds on it are released.
	Delete

	// Define and Undefine are about a KIND rather than about any instance
	// of it, which is why a grant of either carries no path.
	Define
	Undefine
)

// verbNames is the one place a verb's spelling lives. A grant is stored
// as text, so these strings are a wire format: changing one silently
// revokes every grant that used it.
var verbNames = map[Verb]string{
	Get:      "get",
	List:     "list",
	Watch:    "watch",
	Put:      "put",
	Report:   "report",
	Claim:    "claim",
	Release:  "release",
	Delete:   "delete",
	Define:   "define",
	Undefine: "undefine",
}

// ParseVerb reads a verb from a grant.
//
// An unknown verb is refused rather than ignored. A grant nobody can
// interpret is a grant that would silently permit nothing, and a role
// full of them would look like permission and behave like none.
func ParseVerb(raw string) (Verb, error) {
	for verb, name := range verbNames {
		if name == strings.TrimSpace(raw) {
			return verb, nil
		}
	}

	return NoVerb, fmt.Errorf("%w: %q", ErrUnknownVerb, raw)
}

// IsZero reports a verb nobody named.
func (v Verb) IsZero() bool { return v == NoVerb }

// AddressesOne reports whether this verb acts on a named resource, and so
// whether a path in a grant of it means anything.
//
// Define and Undefine are about the kind itself. A grant of either
// carries no path, because there is no instance for one to confine.
func (v Verb) AddressesOne() bool { return v != Define && v != Undefine }

func (v Verb) String() string {
	if name, found := verbNames[v]; found {
		return name
	}

	return "unknown"
}
