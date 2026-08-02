package auth

import (
	"context"
	"slices"
)

// CheckEscalation enforces the rule that keeps resource-backed roles from
// being a hole: you may only WRITE grants you already HOLD.
//
// Without it, any identity allowed to put Role resources could hand itself
// kind "*" and own the system. The same rule exists in k8s RBAC for the
// same reason.
//
// A written grant is covered when the writer holds a grant that is at
// least as permissive in every dimension: verbs (superset), kind (equal
// or "*"), path scope (an ancestor prefix), parts (superset or full
// write), and constraints (the holder's Where must be implied by the
// written one — a writer restricted to spec.placement==self cannot mint an
// unconstrained grant).
// tenant is where the written grants will live: a Role document carries no
// tenant of its own (the system assigns it from the Role's path), so the
// guard must compare the holder's confinement against that destination —
// not against the empty field of the document.
func CheckEscalation(ctx context.Context, written []Grant, tenant string) error {
	creds, ok := FromContext(ctx)
	if !ok {
		return ErrDenied
	}

	for i := range written {
		scoped := written[i].clone()
		scoped.Tenant = tenant

		if !heldCovers(creds, &scoped) {
			return ErrDenied
		}
	}

	return nil
}

func heldCovers(creds Credentials, written *Grant) bool {
	for i := range creds.Grants {
		if covers(&creds.Grants[i], written, creds.Principal) {
			return true
		}
	}

	return false
}

func covers(held, written *Grant, principal Principal) bool {
	if held.Kind != "*" && held.Kind != written.Kind {
		return false
	}

	// A tenant-confined holder can only write grants confined to the same
	// tenant; an unconfined holder (bootstrap) can write anything.
	if held.Tenant != "" && held.Tenant != written.Tenant {
		return false
	}

	for _, verb := range written.Verbs {
		if !slices.Contains(held.Verbs, verb) {
			return false
		}
	}

	return coversParts(held, written) && coversScope(held, written, principal) && coversWhere(held, written, principal)
}

// coversParts: Parts only constrain writes, so a read-only grant carrying
// a stale Parts list must not block re-issuing itself. Full write covers
// any part list; a restricted holder covers only the parts it has.
func coversParts(held, written *Grant) bool {
	if len(held.Parts) == 0 || !writesAnything(written) {
		return true
	}

	if len(written.Parts) == 0 {
		return false // full write against a restricted holder
	}

	for _, part := range written.Parts {
		if !slices.Contains(held.Parts, part) {
			return false
		}
	}

	return true
}

// coversScope: the holder's prefix must be an ancestor of the written one
// (a longer held prefix means the writer is confined deeper and cannot
// widen). Segments match literally — the SAME variable expression is safe
// delegation, it narrows the new holder exactly as it narrows this one —
// or through the literal the holder resolves to.
func coversScope(held, written *Grant, principal Principal) bool {
	if len(held.PathPrefix) > len(written.PathPrefix) {
		return false
	}

	for i, seg := range held.PathPrefix {
		if seg != written.PathPrefix[i] && interpolate(seg, principal) != written.PathPrefix[i] {
			return false
		}
	}

	return true
}

// coversWhere: every constraint the holder is bound by must also bind the
// written grant, so constraints can only be added.
func coversWhere(held, written *Grant, principal Principal) bool {
	for _, term := range held.Where {
		if !boundBy(written.Where, term, principal) {
			return false
		}
	}

	return true
}

// writesAnything reports whether the grant permits any write verb — the
// only case where Parts mean something.
func writesAnything(grant *Grant) bool {
	return slices.Contains(grant.Verbs, VerbPut) || slices.Contains(grant.Verbs, VerbDelete)
}

func boundBy(written []Constraint, held Constraint, principal Principal) bool {
	literal := Constraint{Path: held.Path, Equal: interpolate(held.Equal, principal)}

	return slices.Contains(written, literal) || slices.Contains(written, held)
}
