package resource_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/auth"
	"github.com/graphene-ci/graphene/internal/core/builtin"
	"github.com/graphene-ci/graphene/internal/core/registry"
	"github.com/graphene-ci/graphene/internal/core/service"
	authres "github.com/graphene-ci/graphene/internal/infrastructure/auth/resource"
	"github.com/graphene-ci/graphene/internal/infrastructure/store/bbolt"
)

// tenantAdmin is the shape the review's attack used: full rights over the
// authority kinds INSIDE one tenant, nothing beyond it.
func tenantAdmin() []auth.Grant {
	return []auth.Grant{
		{
			Verbs:      []auth.Verb{auth.VerbGet, auth.VerbList, auth.VerbWatch, auth.VerbPut, auth.VerbDelete},
			Kind:       builtin.KindRole,
			PathPrefix: []string{tenant},
		},
		{
			Verbs:      []auth.Verb{auth.VerbGet, auth.VerbList, auth.VerbWatch, auth.VerbPut, auth.VerbDelete},
			Kind:       builtin.KindIdentity,
			PathPrefix: []string{tenant},
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
// by root, and a tenant admin (alice) who cannot mint it.
func setupDisarmScenario(t *testing.T) (*harness, context.Context) {
	t.Helper()

	harn := newHarness(t)

	if err := harn.putRole(harn.admin, t, "super", superRole()); err != nil {
		t.Fatalf("put super role: %v", err)
	}

	if err := harn.putIdentity(harn.admin, t, "root", auth.PrincipalUser, []string{"super"}, "root-token"); err != nil {
		t.Fatalf("put root identity: %v", err)
	}

	if err := harn.putRole(harn.admin, t, "tenant-admin", tenantAdmin()); err != nil {
		t.Fatalf("put tenant-admin role: %v", err)
	}

	if err := harn.putIdentity(harn.admin, t, "alice", auth.PrincipalUser,
		[]string{"tenant-admin"}, "alice-token"); err != nil {
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
		Key: &graphenepbv1.Key{Kind: builtin.KindRole, Path: []string{tenant, "super"}},
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
	err = harn.putRole(alice, t, "super", tenantAdmin())
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
	if err := harn.putRole(alice, t, "copy", tenantAdmin()); err != nil {
		t.Fatalf("re-issuing a held role: %v", err)
	}

	got, err := harn.resources.Get(alice, &graphenepbv1.GetRequest{
		Key: &graphenepbv1.Key{Kind: builtin.KindRole, Path: []string{tenant, "copy"}},
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
		PathPrefix: []string{tenant, "${principal.name}"},
		Where:      []auth.Constraint{{Path: "spec.kernel", Equal: "${principal.name}"}},
	}}

	if err := harn.putRole(harn.admin, t, "kernel-operator", operator); err != nil {
		t.Fatalf("put operator role: %v", err)
	}

	if err := harn.putIdentity(harn.admin, t, "ops", auth.PrincipalUser,
		[]string{"kernel-operator"}, "ops-token"); err != nil {
		t.Fatalf("put ops identity: %v", err)
	}

	// Ops also administers roles inside the tenant.
	if err := harn.putRole(harn.admin, t, "role-admin", []auth.Grant{{
		Verbs:      []auth.Verb{auth.VerbGet, auth.VerbPut},
		Kind:       builtin.KindRole,
		PathPrefix: []string{tenant},
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
		PathPrefix: []string{tenant, "victim"},
		Where:      []auth.Constraint{{Path: "spec.kernel", Equal: "victim"}},
	}})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("pinning the variable to a foreign literal: want PermissionDenied, got %v", err)
	}
}

// Grants resolved from a role are confined to the tenant the role lives
// in: identities of different tenants sharing a name are different
// principals.
func TestGrantsAreTenantConfined(t *testing.T) {
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

	if creds.Principal.Tenant != tenant {
		t.Fatalf("principal tenant: got %q, want %q", creds.Principal.Tenant, tenant)
	}

	for _, grant := range creds.Grants {
		if grant.Tenant != tenant {
			t.Fatalf("grant not confined to its role's tenant: %+v", grant)
		}
	}

	// A lease of ANOTHER tenant with the same kernel name is out of reach.
	lease := &graphenepbv1.Resource{
		Key: &graphenepbv1.Key{Kind: builtin.KindKernelLease, Path: []string{"other", "k1"}},
	}

	kernelCtx := auth.WithCredentials(context.Background(), creds)
	if err := auth.CheckWrite(kernelCtx, builtin.KindKernelLease, lease.GetKey().GetPath(),
		[]auth.Part{auth.PartSpec}, lease); !errors.Is(err, auth.ErrDenied) {
		t.Fatalf("cross-tenant write: want ErrDenied, got %v", err)
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
