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
	// Name is the identity's whole path joined — one string, globally
	// unique, the value ${principal.name} interpolates to. The kernel
	// never splits it: a path segment carries no meaning here, so
	// comparing names is comparing one string (k8s does the same with
	// "system:serviceaccount:<ns>:<name>").
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

// Vouching authenticates a principal that holds no credentials at all.
//
// A spawned process has none by design: minting it a token would mean
// writing a digest into an Identity (handing the kernel that spawns
// processes authority over the strongest kind in the system), and putting
// a token in the Process spec would mean a secret readable by anyone with
// get on it. So instead the kernel that spawned the process VOUCHES for
// it, signing the request with its own credentials.
//
// This adds no trust: a kernel already holds the bytes and runs them, so
// anything on it is already within its reach. What it does is make the
// existing trust checkable — the vouch names a process, and the answer
// comes from the store, where a Process record exists only because
// someone authorized to hand out that identity wrote it. Revocation is
// deleting the record; there is no secret anywhere to leak or rotate.
type Vouching interface {
	// ActingFor returns the credentials of the named process on the named
	// kernel; false when that kernel has no such process.
	ActingFor(kernel, process string) (Credentials, bool)
}

// principalNameVar is the one grant variable, interpolated into PathPrefix
// segments and Where values.
const principalNameVar = "${principal.name}"

func interpolate(s string, p Principal) string {
	return strings.ReplaceAll(s, principalNameVar, p.Name)
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

// Clone deep-copies grants: the index and every credential handed out must
// not share mutable slices with the resource they were decoded from.
func Clone(grants []Grant) []Grant {
	out := make([]Grant, 0, len(grants))
	for i := range grants {
		out = append(out, grants[i].clone())
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
	}
}
