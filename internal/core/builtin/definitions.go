// Package builtin holds the definitions of system kinds — CRDs that ship
// in the binary. They pass through the SAME Define machinery as user
// kinds: the only privileges built-ins have are being ensured at startup
// and being system-owned in authorization.
//
// Their paths are flat, one name each. These are objects of the
// installation itself — identities, roles, kernels — the way Node and
// ClusterRole are cluster-scoped in k8s. Grouping, ownership and isolation
// are things an operator expresses on their OWN kinds, by giving them
// whatever path shape they need and confining grants with PathPrefix; the
// kernel attaches no meaning to any segment.
package builtin

import (
	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
)

// Kind names of the system kinds.
const (
	KindKernel      = "Kernel"
	KindKernelLease = "KernelLease"
	KindRole        = "Role"
	KindIdentity    = "Identity"

	// schemaNS namespaces the schemapb identities of built-in kinds.
	schemaNS = "graphene"

	// segKernel names the path segment holding a kernel's name.
	segKernel = "kernel"
)

// Definitions returns the compiled-in kind definitions, version field
// unset (assigned by Define).
func Definitions() []*graphenepbv1.ResourceDefinition {
	return []*graphenepbv1.ResourceDefinition{
		kernelDefinition(),
		kernelLeaseDefinition(),
		roleDefinition(),
		identityDefinition(),
	}
}

// Kernel — a kernel that has announced itself. The SPEC is written by the
// kernel it describes (core/presence): os and arch are facts about that
// machine, and nobody else should be typing them in. The STATUS is written
// by the lease controller only — what a kernel is, it says; what it is
// judged to be, it is told.
func kernelDefinition() *graphenepbv1.ResourceDefinition {
	spec := schemapb.NewSchema(&schemapb.SchemaIdentity{Namespace: schemaNS, Name: "kernel-spec", Version: "v1"}).
		Fields(
			schemapb.Str("os").Required(),
			schemapb.Str("arch").Required(),
		).
		MustBuild()

	status := schemapb.NewSchema(&schemapb.SchemaIdentity{Namespace: schemaNS, Name: "kernel-status", Version: "v1"}).
		Fields(
			schemapb.Bool("online"),
		).
		MustBuild()

	return &graphenepbv1.ResourceDefinition{
		Kind:         KindKernel,
		PathSegments: []string{segKernel},
		SpecSchema:   spec,
		StatusSchema: status,
	}
}

// KernelLease — the liveness heartbeat: the worker kernel renews its
// lease by ordinary Puts; expiry is judged by the lease controller with
// the CONTROL kernel's clock (no timestamps in the store, no trust in
// worker clocks — a renewal is a revision bump, nothing more).
func kernelLeaseDefinition() *graphenepbv1.ResourceDefinition {
	spec := schemapb.NewSchema(&schemapb.SchemaIdentity{Namespace: schemaNS, Name: "kernel-lease-spec", Version: "v1"}).
		Fields(
			// The kernel this lease belongs to — the field the worker
			// grant's Where binds to (spec.kernel == ${principal.name}).
			schemapb.Str("kernel").Required(),
			schemapb.Int64("ttl_seconds").Required().Gte(1),
		).
		MustBuild()

	return &graphenepbv1.ResourceDefinition{
		Kind:         KindKernelLease,
		PathSegments: []string{segKernel},
		SpecSchema:   spec,
	}
}

// Role — a named set of grants, the serialized form of auth.Grant. Roles
// live as resources so authorization can be administered through the API
// (and watched live) instead of a config file requiring restarts.
func roleDefinition() *graphenepbv1.ResourceDefinition {
	spec := schemapb.NewSchema(&schemapb.SchemaIdentity{Namespace: schemaNS, Name: "role-spec", Version: "v1"}).
		Fields(grantsField("grants").Required()).
		MustBuild()

	return &graphenepbv1.ResourceDefinition{
		Kind:         KindRole,
		PathSegments: []string{"name"},
		SpecSchema:   spec,
	}
}

// Identity — who a token authenticates as, and which roles it carries.
//
// Tokens are stored as sha256 hex digests, never in clear: a token is a
// high-entropy random string, so a plain digest is enough (this is not a
// password — there is nothing to brute force). The list holds more than
// one digest during rotation.
func identityDefinition() *graphenepbv1.ResourceDefinition {
	spec := schemapb.NewSchema(&schemapb.SchemaIdentity{Namespace: schemaNS, Name: "identity-spec", Version: "v1"}).
		Fields(
			schemapb.Str("principal_kind").In("user", "kernel", "process").Required(),
			schemapb.List("roles", schemapb.Str("role")).Required(),
			schemapb.List("token_sha256", schemapb.Str("digest")).Required(),
		).
		MustBuild()

	return &graphenepbv1.ResourceDefinition{
		Kind:         KindIdentity,
		PathSegments: []string{"name"},
		SpecSchema:   spec,
	}
}

// grantsField is the serialized form of []auth.Grant, kept apart from the
// one kind using it today because grants are a shape, not a Role: anything
// that hands authority to someone else serializes them the same way.
//
// Verbs and parts are closed vocabularies: a typo like "Put" would
// otherwise produce a grant that silently matches nothing — fail-safe but
// undiagnosable.
func grantsField(name schemapb.FieldName) *schemapb.ListB {
	return schemapb.List(name,
		schemapb.Object("grant",
			schemapb.List("verbs",
				schemapb.Str("verb").In("get", "list", "watch", "put", "delete", "define"),
			).Required(),
			schemapb.Str("kind").Required(),
			schemapb.List("path_prefix", schemapb.Str("segment")),
			schemapb.List("where",
				schemapb.Object("term",
					schemapb.Str("path").Required(),
					schemapb.Str("equal").Required(),
				),
			),
			schemapb.List("parts",
				schemapb.Str("part").In("spec", "status", "finalizers"),
			),
		),
	)
}
