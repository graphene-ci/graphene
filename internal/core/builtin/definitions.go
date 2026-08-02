// Package builtin holds the definitions of system kinds — CRDs that ship
// in the binary. They pass through the SAME Define machinery as user
// kinds: the only privileges built-ins have are being ensured at startup
// and being system-owned in authorization.
package builtin

import (
	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
)

// Kind names of the system kinds defined so far. The set grows with the
// controllers that need them (Execution arrives with the execution layer).
const (
	KindKernel      = "Kernel"
	KindKernelLease = "KernelLease"

	// schemaNS namespaces the schemapb identities of built-in kinds.
	schemaNS = "graphene"
)

// Definitions returns the compiled-in kind definitions, version field
// unset (assigned by Define).
func Definitions() []*graphenepbv1.ResourceDefinition {
	return []*graphenepbv1.ResourceDefinition{
		kernelDefinition(),
		kernelLeaseDefinition(),
	}
}

// Kernel — a registered worker kernel. Created by the OPERATOR when the
// join token is issued; spec is operator-owned, status is written by the
// lease controller only.
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
		PathSegments: []string{"tenant", "kernel"},
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
		PathSegments: []string{"tenant", "kernel"},
		SpecSchema:   spec,
	}
}
