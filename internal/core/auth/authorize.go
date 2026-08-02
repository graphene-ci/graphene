package auth

import (
	"context"
	"slices"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
)

// CheckRead authorizes a Get of the fetched resource.
func CheckRead(ctx context.Context, kind string, path []string, res *graphenepbv1.Resource) error {
	return checkObject(ctx, VerbGet, kind, path, res)
}

// CheckDelete authorizes a Delete of the current resource.
func CheckDelete(ctx context.Context, kind string, path []string, res *graphenepbv1.Resource) error {
	return checkObject(ctx, VerbDelete, kind, path, res)
}

// CheckWrite authorizes a Put that changes the given parts. The resource
// the constraints are evaluated against is the CURRENT record for updates
// (a writer must not be able to move an object out of its own scope) and
// the incoming one for creates.
func CheckWrite(ctx context.Context, kind string, path []string, parts []Part, res *graphenepbv1.Resource) error {
	creds, ok := FromContext(ctx)
	if !ok {
		return ErrDenied
	}

	for _, part := range parts {
		if !partAllowed(creds, kind, path, part, res) {
			return ErrDenied
		}
	}

	// A Put touching nothing still needs the verb at all.
	if len(parts) == 0 && !anyGrantForObject(creds, VerbPut, kind, path, res) {
		return ErrDenied
	}

	return nil
}

// Check authorizes an object-less operation on a kind (the byte-plane
// services: blob upload/download). Grants carrying Where constraints can
// never authorize object-less calls — there is no object to constrain.
func Check(ctx context.Context, verb Verb, kind string) error {
	creds, ok := FromContext(ctx)
	if !ok {
		return ErrDenied
	}

	for i := range creds.Grants {
		grant := &creds.Grants[i]
		if grantCovers(grant, creds.Principal, verb, kind, nil) && len(grant.Where) == 0 {
			return nil
		}
	}

	return ErrDenied
}

// CheckDefine authorizes definition writes.
func CheckDefine(ctx context.Context) error {
	creds, ok := FromContext(ctx)
	if !ok {
		return ErrDenied
	}

	for i := range creds.Grants {
		if grantCovers(&creds.Grants[i], creds.Principal, VerbDefine, "Kind", nil) {
			return nil
		}
	}

	return ErrDenied
}

// Predicate filters resources object-by-object.
type Predicate func(*graphenepbv1.Resource) bool

// Filter authorizes a List/Watch over kind+prefix and returns the
// mandatory server-side predicate: the union of matching grants — an
// object passes when ANY matching grant's scope and Where hold on it.
// ErrDenied when no grant covers the verb+kind at all.
func Filter(ctx context.Context, verb Verb, kind string, prefix []string) (Predicate, error) {
	creds, ok := FromContext(ctx)
	if !ok {
		return nil, ErrDenied
	}

	var matched []Grant

	for i := range creds.Grants {
		grant := &creds.Grants[i]
		if hasVerb(grant, verb) && kindMatches(grant, kind) && scopesOverlap(grant, creds.Principal, prefix) {
			matched = append(matched, *grant)
		}
	}

	if len(matched) == 0 {
		return nil, ErrDenied
	}

	principal := creds.Principal
	predicate := func(res *graphenepbv1.Resource) bool {
		for i := range matched {
			grant := &matched[i]
			if objectInScope(grant, principal, res.GetKey().GetPath()) && whereHolds(grant, principal, res) {
				return true
			}
		}

		return false
	}

	return predicate, nil
}

// --- internals ----------------------------------------------------------

func checkObject(ctx context.Context, verb Verb, kind string, path []string, res *graphenepbv1.Resource) error {
	creds, ok := FromContext(ctx)
	if !ok {
		return ErrDenied
	}

	if !anyGrantForObject(creds, verb, kind, path, res) {
		return ErrDenied
	}

	return nil
}

func anyGrantForObject(creds Credentials, verb Verb, kind string, path []string, res *graphenepbv1.Resource) bool {
	for i := range creds.Grants {
		grant := &creds.Grants[i]
		if grantCovers(grant, creds.Principal, verb, kind, path) && whereHolds(grant, creds.Principal, res) {
			return true
		}
	}

	return false
}

func partAllowed(creds Credentials, kind string, path []string, part Part, res *graphenepbv1.Resource) bool {
	for i := range creds.Grants {
		grant := &creds.Grants[i]
		if !grantCovers(grant, creds.Principal, VerbPut, kind, path) {
			continue
		}

		if !whereHolds(grant, creds.Principal, res) {
			continue
		}

		if len(grant.Parts) == 0 || slices.Contains(grant.Parts, part) {
			return true
		}
	}

	return false
}

// grantCovers: verb + kind + the object path lies inside the grant scope.
func grantCovers(grant *Grant, p Principal, verb Verb, kind string, path []string) bool {
	return hasVerb(grant, verb) && kindMatches(grant, kind) && objectInScope(grant, p, path)
}

func hasVerb(grant *Grant, verb Verb) bool {
	return slices.Contains(grant.Verbs, verb)
}

func kindMatches(grant *Grant, kind string) bool {
	return grant.Kind == "*" || grant.Kind == kind
}

// objectInScope: the object lies inside the grant's tenant confinement and
// its path starts with the (interpolated) grant prefix.
func objectInScope(grant *Grant, p Principal, path []string) bool {
	if !tenantAllows(grant, path) {
		return false
	}

	if len(grant.PathPrefix) > len(path) {
		return false
	}

	for i, seg := range grant.PathPrefix {
		if interpolate(seg, p) != path[i] {
			return false
		}
	}

	return true
}

// tenantAllows enforces the grant's tenant confinement: a tenant-scoped
// grant reaches only resources whose first path segment is that tenant.
// Object-less operations (no path — the byte plane) are unaffected.
func tenantAllows(grant *Grant, path []string) bool {
	if grant.Tenant == "" || len(path) == 0 {
		return true
	}

	return path[0] == grant.Tenant
}

// scopesOverlap: a List/Watch request prefix and a grant scope can share
// objects — either may be the narrower one; the predicate settles per
// object.
func scopesOverlap(grant *Grant, p Principal, prefix []string) bool {
	if !tenantAllows(grant, prefix) {
		return false
	}

	shorter := min(len(grant.PathPrefix), len(prefix))

	for i := range shorter {
		if interpolate(grant.PathPrefix[i], p) != prefix[i] {
			return false
		}
	}

	return true
}

func whereHolds(grant *Grant, p Principal, res *graphenepbv1.Resource) bool {
	for _, constraint := range grant.Where {
		if !FieldEquals(res, constraint.Path, interpolate(constraint.Equal, p)) {
			return false
		}
	}

	return true
}
