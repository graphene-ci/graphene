// Package ref holds reference types: values that stand for something
// stored elsewhere. A reference is what travels through specs, workflow
// history, and logs — the referent (secret value, blob bytes, owning
// record) is resolved at the point of use and never travels back.
package ref

import (
	"fmt"
	"strings"

	"github.com/graphene-ci/graphene/pkg/id"
)

// SecretRef names a secret. The value is resolved by the consumer (agent
// or worker) at the moment of use; it never appears in specs, logs, or
// Temporal history.
type SecretRef struct {
	Name id.SecretId `json:"name"`
}

// BlobRef points at bytes in external storage. Digest pins the content;
// Location says where to fetch it.
type BlobRef struct {
	Digest   string `json:"digest"`
	Location string `json:"location"`
	Size     int64  `json:"size,omitempty"`
}

// OwnerRef points at the owning record as "kind/id" (an entity workflow
// address). Ownership can be given away, never taken; the owner must have
// a reason to die.
type OwnerRef string

// RunOwner builds the owner reference for a run.
func RunOwner(r id.RunId) OwnerRef { return OwnerRef("run/" + string(r)) }

// Validate reports whether the owner reference is well-formed.
func (o OwnerRef) Validate() error {
	s := string(o)
	i := strings.IndexByte(s, '/')
	if i <= 0 || i == len(s)-1 {
		return fmt.Errorf("owner ref %q: want kind/id", s)
	}
	return nil
}
