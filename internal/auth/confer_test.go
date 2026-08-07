package auth_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// A grant lives in a Role, but it is handed out in three places, and
// these are the other two: naming a Role from an Identity, and naming an
// Identity from something that then acts as it.

// runnerKind is a kind that runs as somebody — the shape of a Process,
// with nothing else about it, because the rule is not about Process. Any
// kind that declares a reference to an Identity is handing that
// identity's authority to whoever controls the resource.
func runnerKind(t *testing.T) def.Definition {
	t.Helper()

	field, err := path.NewFieldPath(def.SpecRoot, "identity")
	if err != nil {
		t.Fatalf("field path: %v", err)
	}

	runs, err := def.NewRef(field, auth.IdentityKind, def.Strong)
	if err != nil {
		t.Fatalf("ref: %v", err)
	}

	return def.MustNew(
		kind.MustNew("Runner"), path.MustNewTPath("name"),
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "runner-spec"}).
			Fields(schemapb.Str("identity")).MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "runner-status"}).
			MustBuild()),
		def.Reference(runs),
	)
}

func runnerId(t *testing.T, name string) resource.Id {
	t.Helper()

	at, err := path.MustNewTPath("name").New(name)
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	return resource.NewId(kind.MustNew("Runner"), at)
}

func intentFor(t *testing.T, id resource.Id, spec map[string]any) resource.Intent {
	t.Helper()

	intent, err := resource.NewIntent(id, schemapb.MustStructFromGo(spec))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	return intent
}

// Writing an Identity hands out every grant its roles hold. Without this
// check, "may manage users" is "may do anything" with one extra step:
// write an identity naming the strongest role there is, put a digest of a
// secret you chose on it, and log in as it.
func TestAnIdentityCannotNameARoleItsAuthorDoesNotHold(t *testing.T) {
	t.Parallel()

	k, guard := open(t)
	ctx := context.Background()

	role(t, k, "everything", grant("delete", "Process", ""))
	role(t, k, "user-admin",
		grant("put", auth.IdentityKind.String(), ""),
		grant("get", "Process", "/local"),
	)

	who := identity(t, k, "admin", "user-admin")
	session := guard.As(who)

	made, err := auth.NewPrincipal("made-up")
	if err != nil {
		t.Fatalf("principal: %v", err)
	}

	id, err := auth.IdentityId(made)
	if err != nil {
		t.Fatalf("identity id: %v", err)
	}

	tooMuch := intentFor(t, id, map[string]any{
		"roles":   []any{"everything"},
		"digests": []any{auth.Digest("chosen-by-the-attacker")},
	})

	if _, err := session.Put(ctx, tooMuch, revision.Absent); !errors.Is(err, auth.ErrEscalation) {
		t.Fatalf("want ErrEscalation, got %v", err)
	}

	// And an identity holding no more than its author does is ordinary
	// user management, which is what the grant was for.
	role(t, k, "narrow", grant("get", "Process", "/local"))

	fine := intentFor(t, id, map[string]any{
		"roles":   []any{"narrow"},
		"digests": []any{auth.Digest("chosen-by-the-attacker")},
	})

	if _, err := session.Put(ctx, fine, revision.Absent); err != nil {
		t.Fatalf("handing out what it holds: %v", err)
	}
}

// The one the vouch makes urgent. A kernel answers for a process as the
// identity its record names, and the bytes that process runs belong to
// whoever wrote that record — so writing `identity: root` on something
// you supply the code for IS becoming root, with no other step.
func TestSomethingThatRunsAsAnIdentityCannotNameOneBeyondItsAuthor(t *testing.T) {
	t.Parallel()

	k, guard := open(t)
	ctx := context.Background()

	if _, err := k.Define(ctx, runnerKind(t)); err != nil {
		t.Fatalf("define runner: %v", err)
	}

	role(t, k, "everything", grant("delete", "Process", ""))
	identity(t, k, "root", "everything")

	role(t, k, "may-run",
		grant("put", "Runner", ""),
		grant("get", "Process", "/local"),
	)

	session := guard.As(identity(t, k, "operator", "may-run"))

	asRoot := intentFor(t, runnerId(t, "sneaky"), map[string]any{"identity": "root"})

	if _, err := session.Put(ctx, asRoot, revision.Absent); !errors.Is(err, auth.ErrEscalation) {
		t.Fatalf("want ErrEscalation, got %v", err)
	}

	// Running as nobody is always allowed: it hands out nothing.
	asNobody := intentFor(t, runnerId(t, "quiet"), map[string]any{"identity": ""})

	if _, err := session.Put(ctx, asNobody, revision.Absent); err != nil {
		t.Fatalf("running as nobody: %v", err)
	}

	// And running as something no stronger than its author is the
	// ordinary case — this is a permission to run things, not a
	// permission to run nothing.
	identity(t, k, "lesser", "may-run")

	asLesser := intentFor(t, runnerId(t, "ordinary"), map[string]any{"identity": "lesser"})

	if _, err := session.Put(ctx, asLesser, revision.Absent); err != nil {
		t.Fatalf("running as its equal: %v", err)
	}
}

// The same rule, in the other half. A kind may declare a reference to an
// Identity in its STATUS — a controller saying what it found — and
// whoever may report on it would otherwise hand out that identity's
// authority with no check at all, because the check only ever read specs.
func TestAStatusCannotNameAnIdentityBeyondItsAuthor(t *testing.T) {
	t.Parallel()

	k, guard := open(t)
	ctx := context.Background()

	if _, err := k.Define(ctx, reporterKind(t)); err != nil {
		t.Fatalf("define reporter: %v", err)
	}

	role(t, k, "everything", grant("delete", "Process", ""))
	identity(t, k, "root", "everything")

	role(t, k, "may-report",
		grant("put", "Reporter", ""),
		grant("report", "Reporter", ""),
	)

	session := guard.As(identity(t, k, "operator", "may-report"))

	at, err := session.Put(ctx, intentFor(t, reporterId(t, "one"), map[string]any{}), revision.Absent)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	asRoot := schemapb.MustStructFromGo(map[string]any{"identity": "root"})

	if _, err := session.Report(ctx, reporterId(t, "one"), asRoot, at); !errors.Is(err, auth.ErrEscalation) {
		t.Fatalf("naming root in a status: want ErrEscalation, got %v", err)
	}

	// And naming nobody is the ordinary case a controller is for.
	nothing := schemapb.MustStructFromGo(map[string]any{"identity": ""})

	if _, err := session.Report(ctx, reporterId(t, "one"), nothing, at); err != nil {
		t.Fatalf("reporting nothing: %v", err)
	}
}

// reporterKind declares its reference in the STATUS half, which is the
// whole point of it.
func reporterKind(t *testing.T) def.Definition {
	t.Helper()

	field, err := path.NewFieldPath(def.StatusRoot, "identity")
	if err != nil {
		t.Fatalf("field path: %v", err)
	}

	found, err := def.NewRef(field, auth.IdentityKind, def.Weak)
	if err != nil {
		t.Fatalf("ref: %v", err)
	}

	return def.MustNew(
		kind.MustNew("Reporter"), path.MustNewTPath("name"),
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "reporter-spec"}).MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "reporter-status"}).
			Fields(schemapb.Str("identity")).MustBuild()),
		def.Reference(found),
	)
}

func reporterId(t *testing.T, name string) resource.Id {
	t.Helper()

	at, err := path.MustNewTPath("name").New(name)
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	return resource.NewId(kind.MustNew("Reporter"), at)
}

// The check reads the store, not the request, so an author who gains a
// role gains the right to hand it out — and one whose role is narrowed
// loses it. Nothing is remembered from when the grant was written.
func TestWhatMayBeHandedOutIsWhatTheAuthorHoldsNow(t *testing.T) {
	t.Parallel()

	k, guard := open(t)
	ctx := context.Background()

	if _, err := k.Define(ctx, runnerKind(t)); err != nil {
		t.Fatalf("define runner: %v", err)
	}

	role(t, k, "wide", grant("delete", "Process", ""))
	identity(t, k, "strong", "wide")

	role(t, k, "may-run", grant("put", "Runner", ""))

	who := identity(t, k, "operator", "may-run")

	asStrong := intentFor(t, runnerId(t, "worker"), map[string]any{"identity": "strong"})

	if _, err := guard.As(who).Put(ctx, asStrong, revision.Absent); !errors.Is(err, auth.ErrEscalation) {
		t.Fatalf("want ErrEscalation, got %v", err)
	}

	// The same write, after its author is given the same authority.
	widen(t, k, "may-run", grant("put", "Runner", ""), grant("delete", "Process", ""))

	if _, err := guard.As(who).Put(ctx, asStrong, revision.Absent); err != nil {
		t.Fatalf("after the author was given it: %v", err)
	}
}

// widen rewrites a role through the unguarded kernel.
func widen(t *testing.T, k kernel.Kernel, name string, grants ...any) {
	t.Helper()

	id, err := auth.RoleId(name)
	if err != nil {
		t.Fatalf("role id: %v", err)
	}

	stored, err := k.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}

	intent := intentFor(t, id, map[string]any{"grants": grants})

	if _, err := k.Put(context.Background(), intent, stored.Revision); err != nil {
		t.Fatalf("put %s: %v", id, err)
	}
}

// Every record says who wrote it, and nobody can say otherwise. The
// question "who changed this" used to have no answer at all.
func TestARecordSaysWhoWroteIt(t *testing.T) {
	t.Parallel()

	k, guard := open(t)
	ctx := context.Background()

	role(t, k, "writer", grant("put", "Process", ""), grant("report", "Process", ""))

	session := guard.As(identity(t, k, "operator", "writer"))

	id := processId(t, "local", "web")

	at, err := session.Put(ctx, intentFor(t, id, map[string]any{"bundle": "b1"}), revision.Absent)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	stored, err := k.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if stored.Value.Author().String() != "operator" {
		t.Fatalf("written by %q", stored.Value.Author())
	}

	// A status write is a write too, and by somebody else as often as not.
	role(t, k, "reporter", grant("report", "Process", ""))

	controller := guard.As(identity(t, k, "watcher", "reporter"))

	if _, err := controller.Report(ctx, id,
		schemapb.MustStructFromGo(map[string]any{"phase": "running"}), at); err != nil {
		t.Fatalf("report: %v", err)
	}

	stored, err = k.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if stored.Value.Author().String() != "watcher" {
		t.Fatalf("the status write was attributed to %q", stored.Value.Author())
	}

	// And a write through the unguarded kernel is the kernel's own: a
	// store being bootstrapped has no caller, and naming one would be
	// worse than saying there was none.
	if _, err := k.Report(ctx, id,
		schemapb.MustStructFromGo(map[string]any{"phase": "exited"}), stored.Revision); err != nil {
		t.Fatalf("report unguarded: %v", err)
	}

	stored, err = k.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !stored.Value.Author().IsZero() {
		t.Fatalf("the kernel's own write was attributed to %q", stored.Value.Author())
	}
}
