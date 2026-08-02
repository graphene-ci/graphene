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
func CheckEscalation(ctx context.Context, written []Grant) error {
	creds, ok := FromContext(ctx)
	if !ok {
		return ErrDenied
	}

	for i := range written {
		if !heldCovers(creds, &written[i]) {
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

	for _, verb := range written.Verbs {
		if !slices.Contains(held.Verbs, verb) {
			return false
		}
	}

	// Full write covers any part list; a restricted holder covers only the
	// parts it has.
	if len(held.Parts) > 0 {
		if len(written.Parts) == 0 {
			return false // written grant is a full write, holder is not
		}

		for _, part := range written.Parts {
			if !slices.Contains(held.Parts, part) {
				return false
			}
		}
	}

	// The holder's scope must be an ancestor of (or equal to) the written
	// one: writing a broader scope than held is escalation.
	if len(held.PathPrefix) > len(written.PathPrefix) {
		return false
	}

	for i, seg := range held.PathPrefix {
		if interpolate(seg, principal) != written.PathPrefix[i] {
			return false
		}
	}

	// Every constraint the holder is bound by must also bind the written
	// grant, in interpolated form (the written grant is a literal).
	for _, term := range held.Where {
		want := Constraint{Path: term.Path, Equal: interpolate(term.Equal, principal)}
		if !slices.Contains(written.Where, want) {
			return false
		}
	}

	return true
}
