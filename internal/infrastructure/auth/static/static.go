// Package static is the config-file token source: a fixed table of bearer
// tokens with their principals and grants. The enforcement lives in
// core/auth and does not change when this source is replaced by
// resource-backed roles.
package static

import (
	"github.com/graphene-ci/graphene/internal/core/auth"
)

// Entry binds one bearer token to credentials.
type Entry struct {
	Token       string
	Credentials auth.Credentials
}

// Source implements auth.TokenSource over a fixed entry list.
type Source struct {
	byToken map[string]auth.Credentials
}

// New builds a source from entries; later duplicates win deliberately
// (config order is explicit).
func New(entries ...Entry) *Source {
	byToken := make(map[string]auth.Credentials, len(entries))
	for _, entry := range entries {
		byToken[entry.Token] = entry.Credentials
	}

	return &Source{byToken: byToken}
}

// Lookup implements auth.TokenSource.
func (s *Source) Lookup(token string) (auth.Credentials, bool) {
	creds, ok := s.byToken[token]

	return creds, ok
}

// Admin returns full-access credentials scoped to the given path prefix
// (empty prefix = everything).
func Admin(name string, prefix ...string) auth.Credentials {
	return auth.Credentials{
		Principal: auth.Principal{Kind: auth.PrincipalUser, Name: name},
		Grants: []auth.Grant{{
			Verbs:      []auth.Verb{auth.VerbGet, auth.VerbList, auth.VerbWatch, auth.VerbPut, auth.VerbDelete, auth.VerbDefine},
			Kind:       "*",
			PathPrefix: prefix,
		}},
	}
}

// Kernel returns the worker-kernel role for the given kernel id: it sees
// and completes only executions placed on it, maintains its lease, and
// reads programs and definitions.
func Kernel(kernelID string) auth.Credentials {
	placed := []auth.Constraint{{Path: "spec.placement", Equal: "${principal.name}"}}

	return auth.Credentials{
		Principal: auth.Principal{Kind: auth.PrincipalKernel, Name: kernelID},
		Grants: []auth.Grant{
			{
				Verbs: []auth.Verb{auth.VerbGet, auth.VerbList, auth.VerbWatch},
				Kind:  "Execution",
				Where: placed,
			},
			{
				Verbs: []auth.Verb{auth.VerbPut},
				Kind:  "Execution",
				Where: placed,
				Parts: []auth.Part{auth.PartStatus},
			},
			{
				Verbs: []auth.Verb{auth.VerbGet, auth.VerbList, auth.VerbWatch},
				Kind:  "Program",
			},
			{
				// The byte plane: fetch programs/artifact content, upload
				// outputs. Object-less — deliberately Where-free.
				Verbs: []auth.Verb{auth.VerbGet, auth.VerbPut},
				Kind:  "Blob",
			},
			{
				Verbs: []auth.Verb{auth.VerbGet, auth.VerbList, auth.VerbWatch},
				Kind:  "Kind",
			},
			{
				Verbs:      []auth.Verb{auth.VerbGet, auth.VerbPut},
				Kind:       "KernelLease",
				PathPrefix: []string{},
				Where: []auth.Constraint{{
					Path:  "spec.kernel",
					Equal: "${principal.name}",
				}},
			},
		},
	}
}
