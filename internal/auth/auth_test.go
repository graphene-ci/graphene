package auth_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store/kv/memory"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// processShape puts the kernel first, which is what makes "only this
// kernel's processes" expressible as a prefix rather than as a predicate.
var processShape = path.MustNewTPath("kernel", "name")

func open(t *testing.T) (kernel.Kernel, auth.Guard) {
	t.Helper()

	bytes := memory.New()

	t.Cleanup(func() { _ = bytes.Close() })

	k := kernel.New(bytes)

	if err := auth.Bootstrap(context.Background(), k); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	process := def.MustNew(
		kind.MustNew("Process"), processShape,
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-spec"}).
			Fields(schemapb.Str("bundle")).MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-status"}).
			Fields(schemapb.Str("phase")).MustBuild()),
	)

	if _, err := k.Define(context.Background(), process); err != nil {
		t.Fatalf("define: %v", err)
	}

	return k, auth.New(k)
}

// grant builds one stored grant as it lives in a role's spec.
func grant(verb, named, prefix string) any {
	return map[string]any{"verb": verb, "kind": named, "prefix": prefix}
}

// role writes a Role through the unguarded kernel.
func role(t *testing.T, k kernel.Kernel, name string, grants ...any) {
	t.Helper()

	id, err := auth.RoleId(name)
	if err != nil {
		t.Fatalf("role id: %v", err)
	}

	put(t, k, id, map[string]any{"grants": grants})
}

// identity writes an Identity naming roles.
func identity(t *testing.T, k kernel.Kernel, name string, roles ...string) auth.Principal {
	t.Helper()

	who, err := auth.NewPrincipal(name)
	if err != nil {
		t.Fatalf("principal: %v", err)
	}

	id, err := auth.IdentityId(who)
	if err != nil {
		t.Fatalf("identity id: %v", err)
	}

	put(t, k, id, map[string]any{"roles": asAny(roles), "digests": []any{}})

	return who
}

func put(t *testing.T, k kernel.Kernel, id resource.Id, spec map[string]any) {
	t.Helper()

	intent, err := resource.NewIntent(id, schemapb.MustStructFromGo(spec))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	if _, err := k.Put(context.Background(), intent, revision.Absent); err != nil {
		t.Fatalf("put %s: %v", id, err)
	}
}

// asAny is what schemapb wants: a list of values, not a list of strings.
func asAny(items []string) []any {
	out := make([]any, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}

	return out
}

func processId(t *testing.T, values ...string) resource.Id {
	t.Helper()

	at, err := processShape.New(values...)
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	return resource.NewId(kind.MustNew("Process"), at)
}

// A caller nobody granted anything is refused everything, and told the
// same thing whatever they asked about: a refusal that distinguished
// "not allowed" from "not there" would be a way to enumerate the store.
func TestACallerWithNoGrantsIsRefused(t *testing.T) {
	t.Parallel()

	k, guard := open(t)
	ctx := context.Background()

	who := identity(t, k, "nobody")

	if _, err := guard.As(who).Get(ctx, processId(t, "local", "web")); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("want ErrForbidden, got %v", err)
	}

	// An identity that does not exist is the same answer, not a different
	// one: the refusal must not confirm who is registered.
	stranger, err := auth.NewPrincipal("stranger")
	if err != nil {
		t.Fatalf("principal: %v", err)
	}

	if _, err := guard.As(stranger).Get(ctx, processId(t, "local", "web")); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("want ErrForbidden, got %v", err)
	}
}

// A grant is confined by a path PREFIX, which is what lets "only this
// kernel's processes" be said without a predicate language.
func TestAGrantIsConfinedByPathPrefix(t *testing.T) {
	t.Parallel()

	k, guard := open(t)
	ctx := context.Background()

	role(t, k, "local-agent",
		grant("get", "Process", "/local"),
		grant("report", "Process", "/local"),
	)

	who := identity(t, k, "agent", "local-agent")
	session := guard.As(who)

	put(t, k, processId(t, "local", "web"), map[string]any{"bundle": "b1"})
	put(t, k, processId(t, "remote", "web"), map[string]any{"bundle": "b1"})

	if _, err := session.Get(ctx, processId(t, "local", "web")); err != nil {
		t.Fatalf("reading its own kernel: %v", err)
	}

	if _, err := session.Get(ctx, processId(t, "remote", "web")); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("reading another kernel: want ErrForbidden, got %v", err)
	}

	// The verb is the method. Reading is granted; writing intent is not,
	// and no diff of what changed is involved in knowing that.
	intent, err := resource.NewIntent(processId(t, "local", "web"),
		schemapb.MustStructFromGo(map[string]any{"bundle": "b2"}))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	if _, err := session.Put(ctx, intent, 0); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("want ErrForbidden, got %v", err)
	}
}

// The no-escalation rule: writing a role hands out authority, and handing
// out more than you hold is how "may manage users" becomes "may do
// anything".
func TestARoleCannotHandOutMoreThanItsAuthorHolds(t *testing.T) {
	t.Parallel()

	k, guard := open(t)
	ctx := context.Background()

	// This caller may manage roles, and may read processes on one kernel.
	role(t, k, "role-admin",
		grant("put", "Role", ""),
		grant("get", "Process", "/local"),
	)

	who := identity(t, k, "admin", "role-admin")
	session := guard.As(who)

	id, err := auth.RoleId("wider")
	if err != nil {
		t.Fatalf("role id: %v", err)
	}

	// Handing out more of Process than it holds.
	tooMuch, err := resource.NewIntent(id, schemapb.MustStructFromGo(map[string]any{
		"grants": []any{grant("get", "Process", "")},
	}))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	if _, err := session.Put(ctx, tooMuch, revision.Absent); !errors.Is(err, auth.ErrEscalation) {
		t.Fatalf("want ErrEscalation, got %v", err)
	}

	// A different verb it does not hold at all.
	otherVerb, err := resource.NewIntent(id, schemapb.MustStructFromGo(map[string]any{
		"grants": []any{grant("delete", "Process", "/local")},
	}))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	if _, err := session.Put(ctx, otherVerb, revision.Absent); !errors.Is(err, auth.ErrEscalation) {
		t.Fatalf("want ErrEscalation, got %v", err)
	}

	// Handing out exactly what it holds, or less, is allowed.
	narrower, err := resource.NewIntent(id, schemapb.MustStructFromGo(map[string]any{
		"grants": []any{grant("get", "Process", "/local/web")},
	}))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	if _, err := session.Put(ctx, narrower, revision.Absent); err != nil {
		t.Fatalf("handing out less than it holds: %v", err)
	}
}

// A role cannot be deleted while an identity names it: without that,
// removing a role would silently strip permissions from callers nobody
// was looking at.
func TestARoleInUseCannotBeDeleted(t *testing.T) {
	t.Parallel()

	k, _ := open(t)
	ctx := context.Background()

	role(t, k, "held", grant("get", "Process", ""))
	identity(t, k, "holder", "held")

	id, err := auth.RoleId("held")
	if err != nil {
		t.Fatalf("role id: %v", err)
	}

	stored, err := k.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if _, err := k.Delete(ctx, id, stored.Revision); !errors.Is(err, kernel.ErrReferenced) {
		t.Fatalf("want ErrReferenced, got %v", err)
	}
}

// Bootstrap goes through Define, so running it again leaves one version
// of each kind rather than a new one every start.
func TestBootstrapIsIdempotent(t *testing.T) {
	t.Parallel()

	k, _ := open(t)
	ctx := context.Background()

	if err := auth.Bootstrap(ctx, k); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	head, err := k.Definition(ctx, auth.RoleKind)
	if err != nil {
		t.Fatalf("definition: %v", err)
	}

	if !head.Version().Eq(1) {
		t.Fatalf("bootstrapping twice left version %s", head.Version())
	}
}
