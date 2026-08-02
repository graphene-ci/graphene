// Package auth is the authorization port: principals, grants and the
// checks the resource service enforces.
//
// The model is deliberately monotone (k8s RBAC's one good decision):
// default deny, grants only allow, no negative rules — a union of grants
// is always safe to reason about.
//
// The hole k8s had to patch with a special node authorizer does not exist
// here: a Grant can carry Where constraints — the same field matching the
// public selector uses — so "a kernel sees only executions placed on it"
// is an ordinary grant, enforced server-side as a mandatory filter.
package auth

import (
	"context"
	"errors"
	"strings"
)

// ErrDenied — the request is not covered by any grant.
var ErrDenied = errors.New("auth: permission denied")

// PrincipalKind separates the three trust domains.
type PrincipalKind string

const (
	// PrincipalUser — a human or CI launch talking to the control kernel.
	PrincipalUser PrincipalKind = "user"
	// PrincipalKernel — a worker kernel on its link.
	PrincipalKernel PrincipalKind = "kernel"
	// PrincipalProcess — a spawned user-code process on the worker's uds.
	PrincipalProcess PrincipalKind = "process"
	// PrincipalSystem — in-process system components (controllers) of the
	// control kernel itself.
	PrincipalSystem PrincipalKind = "system"
)

// Principal is an authenticated identity.
type Principal struct {
	Kind PrincipalKind
	// Tenant the identity belongs to. Names are unique only WITHIN a
	// tenant, so anything comparing names must carry this alongside:
	// without it "k1" of one tenant and "k1" of another are the same
	// principal to every ${principal.name} constraint.
	Tenant string
	// Name: user name, kernel id, or execution path — the value the
	// ${principal.name} grant variable interpolates to.
	Name string
}

// Verb is an operation on resources or definitions.
type Verb string

const (
	VerbGet    Verb = "get"
	VerbList   Verb = "list"
	VerbWatch  Verb = "watch"
	VerbPut    Verb = "put"
	VerbDelete Verb = "delete"
	VerbDefine Verb = "define"
)

// Part is a writable section of a resource; Put grants enumerate them.
type Part string

const (
	PartSpec       Part = "spec"
	PartStatus     Part = "status"
	PartFinalizers Part = "finalizers"
)

// Constraint requires a field of the resource to equal a value.
// Values support the ${principal.name} variable.
type Constraint struct {
	// Path is dotted, rooted at the envelope: "spec.placement".
	Path  string
	Equal string
}

// Grant allows verbs on a kind under a path scope.
type Grant struct {
	Verbs []Verb
	// Kind of resources; "*" matches every kind including definitions.
	Kind string
	// PathPrefix scopes the grant to a subtree; empty = the whole kind.
	// Segments support the ${principal.name} variable.
	PathPrefix []string
	// Where must hold on the accessed resource (server-enforced filter
	// for List/Watch, per-object check for Get/Put/Delete). Empty = no
	// field constraint.
	Where []Constraint
	// Parts writable by Put. Empty = all parts (full write).
	Parts []Part
	// Tenant confines the grant to resources whose first path segment is
	// this tenant. It is NOT part of the Role document: the system sets it
	// from the tenant the Role lives in, so a role can never hand out
	// authority in another tenant — no matter what its author wrote.
	// Empty means unconfined (the bootstrap credential only).
	Tenant string
}

// Credentials is an authenticated principal with its grants; the transport
// layer authenticates a token and stores Credentials in the context.
type Credentials struct {
	Principal Principal
	Grants    []Grant
}

type ctxKey struct{}

// WithCredentials returns a context carrying the credentials.
func WithCredentials(ctx context.Context, creds Credentials) context.Context {
	return context.WithValue(ctx, ctxKey{}, creds)
}

// FromContext extracts credentials; ok=false means unauthenticated.
func FromContext(ctx context.Context) (Credentials, bool) {
	creds, ok := ctx.Value(ctxKey{}).(Credentials)

	return creds, ok
}

// TokenSource resolves a bearer token into credentials.
// Implementations live in infrastructure (static config today, resource-
// backed roles tomorrow); the enforcement below never changes with them.
type TokenSource interface {
	Lookup(token string) (Credentials, bool)
}

// Grant variables, interpolated into PathPrefix segments and Where values.
const (
	principalNameVar   = "${principal.name}"
	principalTenantVar = "${principal.tenant}"
)

func interpolate(s string, p Principal) string {
	s = strings.ReplaceAll(s, principalNameVar, p.Name)

	return strings.ReplaceAll(s, principalTenantVar, p.Tenant)
}

// AllVerbs is every operation the API defines.
//
//nolint:gochecknoglobals // a slice cannot be const; treated as one
var AllVerbs = []Verb{VerbGet, VerbList, VerbWatch, VerbPut, VerbDelete, VerbDefine}

// FullAccess is unrestricted authority — the bootstrap credential and the
// in-process controllers. It exists once so "what does full access mean"
// has a single answer.
func FullAccess(kind PrincipalKind, name string) Credentials {
	return Credentials{
		Principal: Principal{Kind: kind, Name: name},
		Grants:    []Grant{{Verbs: AllVerbs, Kind: "*"}},
	}
}

// ScopeToTenant returns the grants confined to the given tenant. The token
// source applies this when resolving a Role: the resulting grants can only
// ever touch that tenant's resources.
func ScopeToTenant(grants []Grant, tenant string) []Grant {
	out := make([]Grant, 0, len(grants))

	for i := range grants {
		scoped := grants[i].clone()
		scoped.Tenant = tenant
		out = append(out, scoped)
	}

	return out
}

// clone deep-copies a grant: the index and every credential handed out
// must not share mutable slices.
func (g *Grant) clone() Grant {
	return Grant{
		Verbs:      append([]Verb(nil), g.Verbs...),
		Kind:       g.Kind,
		PathPrefix: append([]string(nil), g.PathPrefix...),
		Where:      append([]Constraint(nil), g.Where...),
		Parts:      append([]Part(nil), g.Parts...),
		Tenant:     g.Tenant,
	}
}
