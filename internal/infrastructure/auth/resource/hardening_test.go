package resource_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/auth"
	"github.com/graphene-ci/graphene/internal/core/builtin"
	"github.com/graphene-ci/graphene/internal/core/registry"
	"github.com/graphene-ci/graphene/internal/core/service"
	authres "github.com/graphene-ci/graphene/internal/infrastructure/auth/resource"
	"github.com/graphene-ci/graphene/internal/infrastructure/store/bbolt"
)

// authorityAdmin is the shape the review's attack used: full rights over
// the authority kinds themselves, and nothing else. Administering roles is
// NOT holding what they grant — that gap is the whole point.
func authorityAdmin() []auth.Grant {
	return []auth.Grant{
		{
			Verbs: []auth.Verb{auth.VerbGet, auth.VerbList, auth.VerbWatch, auth.VerbPut, auth.VerbDelete},
			Kind:  builtin.KindRole,
		},
		{
			Verbs: []auth.Verb{auth.VerbGet, auth.VerbList, auth.VerbWatch, auth.VerbPut, auth.VerbDelete},
			Kind:  builtin.KindIdentity,
		},
	}
}

func superRole() []auth.Grant {
	return []auth.Grant{{
		Verbs: []auth.Verb{auth.VerbGet, auth.VerbList, auth.VerbWatch, auth.VerbPut, auth.VerbDelete, auth.VerbDefine},
		Kind:  "*",
	}}
}

// setupDisarmScenario builds the reviewed attack setup: a "super" role held
// by root, and an authority admin (alice) who cannot mint it.
func setupDisarmScenario(t *testing.T) (*harness, context.Context) {
	t.Helper()

	harn := newHarness(t)

	if err := harn.putRole(harn.admin, t, "super", superRole()); err != nil {
		t.Fatalf("put super role: %v", err)
	}

	if err := harn.putIdentity(harn.admin, t, "root", auth.PrincipalUser, []string{"super"}, "root-token"); err != nil {
		t.Fatalf("put root identity: %v", err)
	}

	if err := harn.putRole(harn.admin, t, "authority-admin", authorityAdmin()); err != nil {
		t.Fatalf("put authority-admin role: %v", err)
	}

	if err := harn.putIdentity(harn.admin, t, "alice", auth.PrincipalUser,
		[]string{"authority-admin"}, "alice-token"); err != nil {
		t.Fatalf("put alice identity: %v", err)
	}

	return harn, auth.WithCredentials(context.Background(), harn.waitToken(t, "alice-token"))
}

// A principal that cannot MINT the administrator's authority must not be
// able to DESTROY it either — otherwise the escalation guard is bypassed
// by disarming the system instead of escalating within it.
func TestCannotDeleteAuthorityItDoesNotHold(t *testing.T) {
	t.Parallel()

	harn, alice := setupDisarmScenario(t)

	// Minting it is denied (the original guard).
	err := harn.putRole(alice, t, "godmode", superRole())
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("minting super grants: want PermissionDenied, got %v", err)
	}

	// Deleting the role those grants come from: denied as well.
	got, err := harn.resources.Get(harn.admin, &graphenepbv1.GetRequest{
		Key: &graphenepbv1.Key{Kind: builtin.KindRole, Path: []string{"super"}},
	})
	if err != nil {
		t.Fatalf("read super role: %v", err)
	}

	_, err = harn.resources.Delete(alice, &graphenepbv1.DeleteRequest{
		Key:              got.GetResource().GetKey(),
		ExpectedRevision: got.GetResource().GetRevision(),
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("deleting the super role: want PermissionDenied, got %v", err)
	}

	// Overwriting the role with harmless grants: denied (same authority
	// would be destroyed).
	err = harn.putRole(alice, t, "super", authorityAdmin())
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("neutering the super role: want PermissionDenied, got %v", err)
	}

	// And the administrator's identity may not be rebound away either.
	err = harn.putIdentity(alice, t, "root", auth.PrincipalUser, []string{}, "alice-owns-root")
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("disarming root: want PermissionDenied, got %v", err)
	}

	// Root still works.
	creds := harn.waitToken(t, "root-token")
	if len(creds.Grants) == 0 {
		t.Fatal("root lost its grants")
	}
}

// Authority a principal DOES hold stays administrable: the guard must not
// turn into "nobody may ever delete anything".
func TestCanDeleteAuthorityItHolds(t *testing.T) {
	t.Parallel()

	harn, alice := setupDisarmScenario(t)

	// Alice re-issues her own role under another name (the parameterized
	// re-issue path, L2) and then removes it.
	if err := harn.putRole(alice, t, "copy", authorityAdmin()); err != nil {
		t.Fatalf("re-issuing a held role: %v", err)
	}

	got, err := harn.resources.Get(alice, &graphenepbv1.GetRequest{
		Key: &graphenepbv1.Key{Kind: builtin.KindRole, Path: []string{"copy"}},
	})
	if err != nil {
		t.Fatalf("read copy: %v", err)
	}

	if _, err := harn.resources.Delete(alice, &graphenepbv1.DeleteRequest{
		Key:              got.GetResource().GetKey(),
		ExpectedRevision: got.GetResource().GetRevision(),
	}); err != nil {
		t.Fatalf("deleting a held role: %v", err)
	}
}

// A parameterized role must be delegable: its holder re-issues it verbatim
// and the new holder is narrowed by the same variable.
func TestParameterizedRoleIsDelegable(t *testing.T) {
	t.Parallel()

	harn := newHarness(t)

	operator := []auth.Grant{{
		Verbs:      []auth.Verb{auth.VerbGet, auth.VerbPut},
		Kind:       builtin.KindKernelLease,
		PathPrefix: []string{"${principal.name}"},
		Where:      []auth.Constraint{{Path: "spec.kernel", Equal: "${principal.name}"}},
	}}

	if err := harn.putRole(harn.admin, t, "kernel-operator", operator); err != nil {
		t.Fatalf("put operator role: %v", err)
	}

	if err := harn.putIdentity(harn.admin, t, "ops", auth.PrincipalUser,
		[]string{"kernel-operator"}, "ops-token"); err != nil {
		t.Fatalf("put ops identity: %v", err)
	}

	// Ops also administers roles.
	if err := harn.putRole(harn.admin, t, "role-admin", []auth.Grant{{
		Verbs: []auth.Verb{auth.VerbGet, auth.VerbPut},
		Kind:  builtin.KindRole,
	}}); err != nil {
		t.Fatalf("put role-admin: %v", err)
	}

	if err := harn.putIdentity(harn.admin, t, "ops2", auth.PrincipalUser,
		[]string{"kernel-operator", "role-admin"}, "ops2-token"); err != nil {
		t.Fatalf("put ops2 identity: %v", err)
	}

	opsCtx := auth.WithCredentials(context.Background(), harn.waitToken(t, "ops2-token"))

	// Re-issuing the SAME parameterized grant must be allowed: it narrows
	// the next holder exactly as it narrows this one.
	if err := harn.putRole(opsCtx, t, "kernel-operator-copy", operator); err != nil {
		t.Fatalf("re-issuing a parameterized role denied: %v", err)
	}

	// Turning the variable into someone else's literal is escalation.
	err := harn.putRole(opsCtx, t, "kernel-operator-fixed", []auth.Grant{{
		Verbs:      []auth.Verb{auth.VerbGet, auth.VerbPut},
		Kind:       builtin.KindKernelLease,
		PathPrefix: []string{"victim"},
		Where:      []auth.Constraint{{Path: "spec.kernel", Equal: "victim"}},
	}})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("pinning the variable to a foreign literal: want PermissionDenied, got %v", err)
	}
}

// PathPrefix is the ONLY spatial confinement in the system: nothing scopes
// a role behind the escalation guard's back. So the guard alone has to
// stop a confined holder from writing a role that reaches wider — sideways
// into another subtree, or upwards into no confinement at all.
func TestConfinedHolderCannotWidenItsScope(t *testing.T) {
	t.Parallel()

	harn := newHarness(t)

	// An admin over roles, itself confined to the "a" subtree of Deployment.
	confined := []auth.Grant{
		{
			Verbs: []auth.Verb{auth.VerbGet, auth.VerbPut},
			Kind:  builtin.KindRole,
		},
		{
			Verbs:      []auth.Verb{auth.VerbGet, auth.VerbPut, auth.VerbDelete},
			Kind:       "Deployment",
			PathPrefix: []string{"a"},
		},
	}

	if err := harn.putRole(harn.admin, t, "a-admin", confined); err != nil {
		t.Fatalf("put role: %v", err)
	}

	if err := harn.putIdentity(harn.admin, t, "ann", auth.PrincipalUser,
		[]string{"a-admin"}, "ann-token"); err != nil {
		t.Fatalf("put identity: %v", err)
	}

	annCtx := auth.WithCredentials(context.Background(), harn.waitToken(t, "ann-token"))

	// Re-issuing her own confinement: allowed.
	if err := harn.putRole(annCtx, t, "a-admin-copy", confined); err != nil {
		t.Fatalf("re-issuing a held confinement denied: %v", err)
	}

	deployment := func(prefix []string) []auth.Grant {
		return []auth.Grant{{
			Verbs:      []auth.Verb{auth.VerbGet, auth.VerbPut, auth.VerbDelete},
			Kind:       "Deployment",
			PathPrefix: prefix,
		}}
	}

	// Sideways into another subtree: denied.
	if err := harn.putRole(annCtx, t, "b-admin", deployment([]string{"b"})); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("writing a grant on a foreign subtree: want PermissionDenied, got %v", err)
	}

	// Upwards to no confinement at all: denied.
	if err := harn.putRole(annCtx, t, "all-admin", deployment(nil)); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("dropping the confinement: want PermissionDenied, got %v", err)
	}

	// Deeper into her own subtree: allowed — narrowing is always safe.
	if err := harn.putRole(annCtx, t, "a-one-admin", deployment([]string{"a", "one"})); err != nil {
		t.Fatalf("narrowing a held confinement denied: %v", err)
	}
}

// A Where-constrained kernel identity reaches only its own lease. With one
// flat name per kernel there is no second "k1" anywhere to confuse it with,
// and the constraint — not the path — is what binds.
func TestFieldConstraintBindsToTheName(t *testing.T) {
	t.Parallel()

	harn := newHarness(t)

	if err := harn.putRole(harn.admin, t, "lease-writer", []auth.Grant{{
		Verbs: []auth.Verb{auth.VerbGet, auth.VerbPut},
		Kind:  builtin.KindKernelLease,
		Where: []auth.Constraint{{Path: "spec.kernel", Equal: "${principal.name}"}},
	}}); err != nil {
		t.Fatalf("put role: %v", err)
	}

	if err := harn.putIdentity(harn.admin, t, "k1", auth.PrincipalKernel,
		[]string{"lease-writer"}, "k1-token"); err != nil {
		t.Fatalf("put identity: %v", err)
	}

	creds := harn.waitToken(t, "k1-token")
	if creds.Principal.Name != "k1" {
		t.Fatalf("principal name: %+v", creds.Principal)
	}

	kernelCtx := auth.WithCredentials(context.Background(), creds)

	own := &graphenepbv1.Resource{
		Key:  &graphenepbv1.Key{Kind: builtin.KindKernelLease, Path: []string{"k1"}},
		Spec: schemapb.MustStructFromGo(map[string]any{"kernel": "k1", "ttl_seconds": int64(30)}),
	}
	if err := auth.CheckWrite(kernelCtx, builtin.KindKernelLease, own.GetKey().GetPath(),
		[]auth.Part{auth.PartSpec}, own); err != nil {
		t.Fatalf("writing its own lease: %v", err)
	}

	// Another kernel's lease is out of reach, whatever path it is written at.
	foreign := &graphenepbv1.Resource{
		Key:  &graphenepbv1.Key{Kind: builtin.KindKernelLease, Path: []string{"k2"}},
		Spec: schemapb.MustStructFromGo(map[string]any{"kernel": "k2", "ttl_seconds": int64(30)}),
	}
	if err := auth.CheckWrite(kernelCtx, builtin.KindKernelLease, foreign.GetKey().GetPath(),
		[]auth.Part{auth.PartSpec}, foreign); !errors.Is(err, auth.ErrDenied) {
		t.Fatalf("writing a foreign lease: want ErrDenied, got %v", err)
	}
}

// WaitWarm must mean what it says: after it returns, identities written
// before the restart authenticate immediately.
func TestWarmupIsUsableImmediately(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "store.db")

	st, err := bbolt.Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	reg := registry.New(st)
	adminCreds := auth.Credentials{
		Principal: auth.Principal{Kind: auth.PrincipalUser, Name: "bootstrap"},
		Grants:    superRole(),
	}
	admin := auth.WithCredentials(context.Background(), adminCreds)

	if err := builtin.Ensure(admin, reg); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	seed := &harness{resources: service.NewResources(st, reg)}
	if err := seed.putRole(admin, t, "reader", []auth.Grant{{
		Verbs: []auth.Verb{auth.VerbGet},
		Kind:  builtin.KindKernel,
	}}); err != nil {
		t.Fatalf("seed role: %v", err)
	}

	if err := seed.putIdentity(admin, t, "bob", auth.PrincipalUser, []string{"reader"}, "bob-token"); err != nil {
		t.Fatalf("seed identity: %v", err)
	}

	_ = st.Close()

	// Restart over the same store: the source must be usable the moment
	// WaitWarm returns, with the role already resolved.
	reopened, err := bbolt.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	t.Cleanup(func() { _ = reopened.Close() })

	source := authres.New(reopened, bootstrapToken, adminCreds)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() { _ = source.Run(ctx) }()

	if err := source.WaitWarm(ctx); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	creds, ok := source.Lookup("bob-token")
	if !ok {
		t.Fatal("valid token rejected right after WaitWarm")
	}

	if len(creds.Grants) == 0 {
		t.Fatal("identity indexed before its role: zero grants right after WaitWarm")
	}
}

// A Binding hands its grants to the processes it spawns, so it is
// authority — and a binding on a built-in kind would put user code where
// authority itself is decided. Both doors are shut.
func TestBindingsAreGuardedLikeRoles(t *testing.T) {
	t.Parallel()

	harn := newHarness(t)

	powerful := []auth.Grant{{Verbs: auth.AllVerbs, Kind: "*"}}

	// Minting a binding whose processes would hold more than its author:
	// refused for the same reason a Role would be.
	if err := harn.putBinding(harn.admin, t, "aws.vm", powerful); err != nil {
		t.Fatalf("admin binding: %v", err)
	}

	if err := harn.putRole(harn.admin, t, "binder", []auth.Grant{{
		Verbs: []auth.Verb{auth.VerbGet, auth.VerbPut},
		Kind:  builtin.KindBinding,
	}}); err != nil {
		t.Fatalf("put binder role: %v", err)
	}

	if err := harn.putIdentity(harn.admin, t, "bob", auth.PrincipalUser,
		[]string{"binder"}, "bob-token"); err != nil {
		t.Fatalf("put identity: %v", err)
	}

	bob := auth.WithCredentials(context.Background(), harn.waitToken(t, "bob-token"))

	if err := harn.putBinding(bob, t, "aws.rds", powerful); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("minting a binding beyond its author: want PermissionDenied, got %v", err)
	}

	// And nobody, however privileged, binds code to a built-in kind.
	for _, kind := range []string{builtin.KindIdentity, builtin.KindRole, builtin.KindKernel} {
		err := harn.putBinding(harn.admin, t, kind, nil)
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("binding %s: want PermissionDenied, got %v", kind, err)
		}
	}
}
