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
	KindBundle      = "Bundle"
	KindBinding     = "Binding"
	KindActivation  = "Activation"

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
		bundleDefinition(),
		bindingDefinition(),
		activationDefinition(),
	}
}

// IsBuiltin reports whether the kind ships in the binary. Built-in kinds
// are driven by controllers compiled in here, so they are not bindable:
// letting anyone attach code to Identity or Role would put user code
// exactly where authority is decided.
func IsBuiltin(kind string) bool {
	switch kind {
	case KindKernel, KindKernelLease, KindRole, KindIdentity,
		KindBundle, KindBinding, KindActivation:
		return true
	default:
		return false
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

// grantsField is the serialized form of []auth.Grant. Two kinds carry
// grants — a Role to name a set of them, a Binding to hand a set to the
// processes it spawns — and it is the same shape in both, so it is
// written once.
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

// Bundle — built code, immutable. `graphene push` compiles, uploads the
// bytes as blobs and writes this record; nothing ever edits it, because a
// Binding and an Activation both point AT a bundle and must keep meaning
// the same thing forever (R12–R13).
//
// One bundle carries many entrypoints — a block is one binary — and one
// variant per platform, because a raw-exec runner cannot run someone
// else's architecture. The entrypoint list comes from asking the binary
// itself at push time (its describe mode), so a Binding naming a typo is
// refused when it is written rather than at 3am when it is needed.
func bundleDefinition() *graphenepbv1.ResourceDefinition {
	spec := schemapb.NewSchema(&schemapb.SchemaIdentity{Namespace: schemaNS, Name: "bundle-spec", Version: "v1"}).
		Fields(
			schemapb.List("entrypoints", schemapb.Str("entrypoint")).Required(),
			schemapb.List("variants",
				schemapb.Object("variant",
					schemapb.Str("os").Required(),
					schemapb.Str("arch").Required(),
					// Blob id, not a digest: the digest is an integrity
					// checksum, never an address.
					schemapb.Str("blob").Required(),
				),
			).Required(),
		).
		MustBuild()

	return &graphenepbv1.ResourceDefinition{
		Kind:         KindBundle,
		PathSegments: []string{"bundle", "version"},
		SpecSchema:   spec,
	}
}

// Binding — which code drives a kind. The path IS the kind it drives, so
// two bindings for one kind cannot exist: two controllers reconciling the
// same resource is a race with no arbiter, and refusing it structurally
// beats refusing it in a check.
//
// It is mutable on purpose. Upgrading a controller is an ordinary act and
// must not touch the kind's schema version (see ResourceDefinition).
func bindingDefinition() *graphenepbv1.ResourceDefinition {
	spec := schemapb.NewSchema(&schemapb.SchemaIdentity{Namespace: schemaNS, Name: "binding-spec", Version: "v1"}).
		Fields(
			schemapb.Str("bundle").Required(),
			schemapb.Str("bundle_version").Required(),
			// (spec, previous status) → new status.
			schemapb.Str("reconcile").Required(),
			// (last status) → gone. Absent = the kind needs no teardown, so
			// no finalizer is held on its instances.
			schemapb.Str("destroy"),
			// Re-run reconcile this often even with nothing changed — the
			// drift check. Absent = react to changes only.
			schemapb.Int64("resync_seconds").Gte(1),
			// Which kernels may run it; empty fields match anything.
			schemapb.Object("placement",
				schemapb.Str("os"),
				schemapb.Str("arch"),
			),
			// What the spawned process is allowed to touch. Intersected
			// with what the binding's author holds — the escalation guard
			// applies here exactly as it does to a Role.
			grantsField("grants"),
		).
		MustBuild()

	return &graphenepbv1.ResourceDefinition{
		Kind:         KindBinding,
		PathSegments: []string{"kind"},
		SpecSchema:   spec,
	}
}

// Activation — one invocation of one entrypoint on one kernel: the unit of
// work and the journal entry, the same record seen from two sides.
//
// The path is the KERNEL that runs it, so a kernel watches the prefix of
// its own name — no selector, no scan, and its grant is an ordinary path
// prefix. History ("every activation of this resource") is the cold path
// and is answered by a selector on target_kind/target_path instead.
//
// There is no input field. For a reconcile the input IS the target, and
// the kernel reads it by key: copying a spec in here would only create a
// second, staler copy of something the store already holds.
func activationDefinition() *graphenepbv1.ResourceDefinition {
	spec := schemapb.NewSchema(&schemapb.SchemaIdentity{Namespace: schemaNS, Name: "activation-spec", Version: "v1"}).
		Fields(
			schemapb.Str("entrypoint").Required(),
			schemapb.Str("target_kind").Required(),
			schemapb.List("target_path", schemapb.Str("segment")).Required(),
			schemapb.Str("bundle").Required(),
			schemapb.Str("bundle_version").Required(),
			// Which intent of the target this activation answers. The
			// driver acts again when the target's generation moves past it.
			schemapb.Int64("generation").Required().Gte(1),
			// Cancellation travels the same way everything else does: a
			// spec write the running kernel sees on its watch stream (R21).
			schemapb.Bool("cancelled"),
		).
		MustBuild()

	status := schemapb.NewSchema(&schemapb.SchemaIdentity{Namespace: schemaNS, Name: "activation-status", Version: "v1"}).
		Fields(
			schemapb.Str("phase").In("pending", "running", "succeeded", "failed"),
			schemapb.Str("error"),
			schemapb.Int64("attempt"),
		).
		MustBuild()

	return &graphenepbv1.ResourceDefinition{
		Kind:         KindActivation,
		PathSegments: []string{segKernel, "sequence"},
		SpecSchema:   spec,
		StatusSchema: status,
	}
}
