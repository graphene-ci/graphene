package resource

import (
	"fmt"
	"slices"
)

// Claim places a claim on a resource's deletion.
//
// A separate act from writing the spec, for the same reason Report is:
// it is a different party writing a different part, and the permission
// to do one should not be the permission to do the other. A controller
// that may hold a resource open for cleanup has no business rewriting
// what it is.
//
// Claiming twice changes nothing and is not an error — a controller
// unwinding and starting again does not have to remember whether it
// already did.
//
// A resource already on its way out cannot be claimed. Letting anyone add
// a claim after the mark would be letting anyone hold a deletion open
// forever, and the party that meant to clean up had its chance before the
// delete began.
func Claim(current Resource, finalizer Finalizer) (Resource, error) {
	if err := claimable(current, finalizer); err != nil {
		return Resource{}, err
	}

	if current.deleting {
		return Resource{}, fmt.Errorf("%w: %s", ErrClaimWhileDeleting, current.Id())
	}

	if slices.Contains(current.finalizers, finalizer) {
		return current, nil
	}

	// current is a copy but its slice is not: appending in place could
	// reach the caller's resource, or another copy of it.
	current.finalizers = append(slices.Clone(current.finalizers), finalizer)

	return current, nil
}

// Release lets go of a claim.
//
// Releasing one that is not there changes nothing and is not an error,
// which is the same forgiveness Claim gives and for the same reason.
//
// Releasing the LAST claim on a resource that is marked for deletion is
// what finally removes it — but that is the caller's step, not this one:
// this hands back a resource with no claims left, and whoever asked can
// see that as easily as it could be hidden here.
func Release(current Resource, finalizer Finalizer) (Resource, error) {
	if err := claimable(current, finalizer); err != nil {
		return Resource{}, err
	}

	at := slices.Index(current.finalizers, finalizer)
	if at < 0 {
		return current, nil
	}

	current.finalizers = slices.Delete(slices.Clone(current.finalizers), at, at+1)

	return current, nil
}

// claimable refuses what neither Claim nor Release can act on.
func claimable(current Resource, finalizer Finalizer) error {
	if current.IsZero() {
		return ErrNoResource
	}

	if finalizer.IsZero() {
		return ErrNoFinalizer
	}

	return nil
}
