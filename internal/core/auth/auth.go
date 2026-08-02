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
)

// Principal is an authenticated identity.
type Principal struct {
	Kind PrincipalKind
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

const principalVar = "${principal.name}"

func interpolate(s string, p Principal) string {
	return strings.ReplaceAll(s, principalVar, p.Name)
}
