package resource_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

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

const (
	bootstrapToken = "bootstrap-secret"
	tenant         = "acme"
)

type harness struct {
	resources *service.Resources
	source    *authres.Source
	admin     context.Context
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	st, err := bbolt.Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	t.Cleanup(func() { _ = st.Close() })

	reg := registry.New(st)

	adminCreds := auth.Credentials{
		Principal: auth.Principal{Kind: auth.PrincipalUser, Name: "bootstrap"},
		Grants: []auth.Grant{{
			Verbs: []auth.Verb{auth.VerbGet, auth.VerbList, auth.VerbWatch, auth.VerbPut, auth.VerbDelete, auth.VerbDefine},
			Kind:  "*",
		}},
	}
	admin := auth.WithCredentials(context.Background(), adminCreds)

	if err := builtin.Ensure(admin, reg); err != nil {
		t.Fatalf("ensure builtins: %v", err)
	}

	source := authres.New(st, bootstrapToken, adminCreds)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() { _ = source.Run(ctx) }()

	if err := source.WaitWarm(ctx); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	return &harness{resources: service.NewResources(st, reg), source: source, admin: admin}
}

func (h *harness) putRole(ctx context.Context, t *testing.T, name string, grants []auth.Grant) error {
	t.Helper()

	_, err := h.resources.Put(ctx, &graphenepbv1.PutRequest{
		Resource: &graphenepbv1.Resource{
			Key:  &graphenepbv1.Key{Kind: builtin.KindRole, Path: []string{tenant, name}},
			Spec: auth.GrantsToSpec(grants),
		},
	})

	return err
}

func (h *harness) putIdentity(ctx context.Context, t *testing.T, name string,
	kind auth.PrincipalKind, roles []string, token string,
) error {
	t.Helper()

	rolesAny := make([]any, 0, len(roles))
	for _, role := range roles {
		rolesAny = append(rolesAny, role)
	}

	_, err := h.resources.Put(ctx, &graphenepbv1.PutRequest{
		Resource: &graphenepbv1.Resource{
			Key: &graphenepbv1.Key{Kind: builtin.KindIdentity, Path: []string{tenant, name}},
			Spec: schemapb.MustStructFromGo(map[string]any{
				"principal_kind": string(kind),
				"roles":          rolesAny,
				"token_sha256":   []any{authres.Digest(token)},
			}),
		},
	})

	return err
}

func (h *harness) waitToken(t *testing.T, token string) auth.Credentials {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if creds, ok := h.source.Lookup(token); ok {
			return creds
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("token never appeared in the index")

	return auth.Credentials{}
}

func TestBootstrapAndResourceIdentities(t *testing.T) {
	t.Parallel()

	harn := newHarness(t)

	// The bootstrap token authenticates before any resource exists.
	if _, ok := harn.source.Lookup(bootstrapToken); !ok {
		t.Fatal("bootstrap token not accepted")
	}

	if _, ok := harn.source.Lookup("nonsense"); ok {
		t.Fatal("unknown token accepted")
	}

	// Roles and identities administered through the API become live
	// credentials without a restart.
	grants := []auth.Grant{{
		Verbs: []auth.Verb{auth.VerbGet, auth.VerbPut},
		Kind:  builtin.KindKernelLease,
		Where: []auth.Constraint{{Path: "spec.kernel", Equal: "${principal.name}"}},
	}}
	if err := harn.putRole(harn.admin, t, "kernel-default", grants); err != nil {
		t.Fatalf("put role: %v", err)
	}

	if err := harn.putIdentity(harn.admin, t, "k1", auth.PrincipalKernel,
		[]string{"kernel-default"}, "k1-token"); err != nil {
		t.Fatalf("put identity: %v", err)
	}

	creds := harn.waitToken(t, "k1-token")
	if creds.Principal.Kind != auth.PrincipalKernel || creds.Principal.Name != "k1" {
		t.Fatalf("principal: %+v", creds.Principal)
	}

	if len(creds.Grants) != 1 || creds.Grants[0].Kind != builtin.KindKernelLease {
		t.Fatalf("grants not resolved from role: %+v", creds.Grants)
	}

	if len(creds.Grants[0].Where) != 1 || creds.Grants[0].Where[0].Equal != "${principal.name}" {
		t.Fatalf("where lost in round trip: %+v", creds.Grants[0].Where)
	}
}

func TestEscalationGuard(t *testing.T) {
	t.Parallel()

	harn := newHarness(t)

	// A limited role, and an identity bound to it.
	limited := []auth.Grant{{
		Verbs: []auth.Verb{auth.VerbGet, auth.VerbPut},
		Kind:  builtin.KindRole,
		// It may administer roles — but only ones it could grant itself.
	}}
	if err := harn.putRole(harn.admin, t, "role-admin", limited); err != nil {
		t.Fatalf("put role: %v", err)
	}

	if err := harn.putIdentity(harn.admin, t, "limited", auth.PrincipalUser,
		[]string{"role-admin"}, "limited-token"); err != nil {
		t.Fatalf("put identity: %v", err)
	}

	creds := harn.waitToken(t, "limited-token")
	limitedCtx := auth.WithCredentials(context.Background(), creds)

	// Writing a role it holds: allowed.
	if err := harn.putRole(limitedCtx, t, "copy", limited); err != nil {
		t.Fatalf("writing a held grant denied: %v", err)
	}

	// Minting wildcard power: denied.
	if err := harn.putRole(limitedCtx, t, "godmode", []auth.Grant{{
		Verbs: []auth.Verb{auth.VerbGet, auth.VerbPut, auth.VerbDelete},
		Kind:  "*",
	}}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("escalation via role allowed: %v", err)
	}

	// Binding an existing powerful role to a new identity: also denied,
	// because binding hands out those grants.
	if err := harn.putRole(harn.admin, t, "powerful", []auth.Grant{{
		Verbs: []auth.Verb{auth.VerbDelete},
		Kind:  "*",
	}}); err != nil {
		t.Fatalf("admin put role: %v", err)
	}

	if err := harn.putIdentity(limitedCtx, t, "puppet", auth.PrincipalUser,
		[]string{"powerful"}, "puppet-token"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("escalation via identity binding allowed: %v", err)
	}

	// Binding a role that does not exist is refused outright.
	if err := harn.putIdentity(harn.admin, t, "ghost", auth.PrincipalUser,
		[]string{"no-such-role"}, "ghost-token"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("binding a missing role allowed: %v", err)
	}
}

func TestGrantEncodingRoundTrip(t *testing.T) {
	t.Parallel()

	grants := []auth.Grant{
		{
			Verbs:      []auth.Verb{auth.VerbGet, auth.VerbWatch},
			Kind:       "Execution",
			PathPrefix: []string{"acme", "prod"},
			Where:      []auth.Constraint{{Path: "spec.placement", Equal: "${principal.name}"}},
			Parts:      []auth.Part{auth.PartStatus},
		},
	}

	decoded, err := auth.GrantsFromSpec(auth.GrantsToSpec(grants))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(decoded) != 1 {
		t.Fatalf("got %d grants", len(decoded))
	}

	got := decoded[0]
	if got.Kind != "Execution" || len(got.Verbs) != 2 || len(got.PathPrefix) != 2 ||
		len(got.Where) != 1 || len(got.Parts) != 1 {
		t.Fatalf("round trip lost data: %+v", got)
	}

	if !errors.Is(auth.CheckEscalation(context.Background(), decoded, "acme"), auth.ErrDenied) {
		t.Fatal("unauthenticated escalation check must deny")
	}
}
