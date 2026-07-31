package registry_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	schemapb "github.com/gopherex/schemapb/go/schemapb"
	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/registry"
	"github.com/graphene-ci/graphene/internal/infrastructure/store/bbolt"
)

func newRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	st, err := bbolt.Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return registry.New(st)
}

func secretDef() *graphenepbv1.ResourceDefinition {
	spec := schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "secret-spec"}).
		Fields(schemapb.Str("value").Required()).
		MustBuild()
	return &graphenepbv1.ResourceDefinition{
		Kind:         "Secret",
		PathSegments: []string{"tenant", "env", "name"},
		SpecSchema:   spec,
	}
}

func vmDef() *graphenepbv1.ResourceDefinition {
	spec := schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "vm-spec"}).
		Fields(
			schemapb.Str("type").Required(),
			schemapb.Str("image").Required(),
		).
		MustBuild()
	status := schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "vm-status"}).
		Fields(
			schemapb.Str("id"),
			schemapb.Str("ip"),
		).
		MustBuild()
	return &graphenepbv1.ResourceDefinition{
		Kind:         "aws.vm",
		PathSegments: []string{"tenant", "env", "workflow", "name"},
		SpecSchema:   spec,
		StatusSchema: status,
	}
}

func TestDefineAssignsMonotonicVersions(t *testing.T) {
	r := newRegistry(t)
	ctx := context.Background()

	v1, err := r.Define(ctx, secretDef())
	if err != nil {
		t.Fatalf("define v1: %v", err)
	}
	v2, err := r.Define(ctx, secretDef())
	if err != nil {
		t.Fatalf("define v2: %v", err)
	}
	if v1 != 1 || v2 != 2 {
		t.Fatalf("versions: got %d,%d want 1,2", v1, v2)
	}

	latest, err := r.Get(ctx, "Secret", 0)
	if err != nil {
		t.Fatalf("get latest: %v", err)
	}
	if latest.GetVersion() != 2 {
		t.Fatalf("latest: got v%d want v2", latest.GetVersion())
	}
	pinned, err := r.Get(ctx, "Secret", 1)
	if err != nil {
		t.Fatalf("get v1: %v", err)
	}
	if pinned.GetVersion() != 1 {
		t.Fatalf("pinned: got v%d want v1", pinned.GetVersion())
	}
}

func TestGetErrors(t *testing.T) {
	r := newRegistry(t)
	ctx := context.Background()

	if _, err := r.Get(ctx, "Nope", 0); !errors.Is(err, registry.ErrUnknownKind) {
		t.Fatalf("unknown kind: got %v", err)
	}
	if _, err := r.Define(ctx, secretDef()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Get(ctx, "Secret", 42); !errors.Is(err, registry.ErrUnknownVersion) {
		t.Fatalf("unknown version: got %v", err)
	}
}

func TestDefineRejectsBadInput(t *testing.T) {
	r := newRegistry(t)
	ctx := context.Background()

	cases := map[string]*graphenepbv1.ResourceDefinition{
		"empty kind":    {PathSegments: []string{"a"}, SpecSchema: secretDef().SpecSchema},
		"reserved kind": {Kind: "Kind", PathSegments: []string{"a"}, SpecSchema: secretDef().SpecSchema},
		"no segments":   {Kind: "X", SpecSchema: secretDef().SpecSchema},
		"bad segment":   {Kind: "X", PathSegments: []string{"a/b"}, SpecSchema: secretDef().SpecSchema},
		"no schema":     {Kind: "X", PathSegments: []string{"a"}},
	}
	for name, def := range cases {
		if _, err := r.Define(ctx, def); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestListReturnsLatestPerKind(t *testing.T) {
	r := newRegistry(t)
	ctx := context.Background()

	if _, err := r.Define(ctx, secretDef()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Define(ctx, secretDef()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Define(ctx, vmDef()); err != nil {
		t.Fatal(err)
	}

	defs, err := r.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(defs) != 2 {
		t.Fatalf("list: got %d defs, want 2", len(defs))
	}
	got := map[string]uint32{}
	for _, d := range defs {
		got[d.GetKind()] = d.GetVersion()
	}
	if got["Secret"] != 2 || got["aws.vm"] != 1 {
		t.Fatalf("list versions: %v", got)
	}
}

func TestValidateInstance(t *testing.T) {
	r := newRegistry(t)
	ctx := context.Background()

	if _, err := r.Define(ctx, vmDef()); err != nil {
		t.Fatal(err)
	}

	okSpec := schemapb.MustStructFromGo(map[string]any{
		"type": "t3.medium", "image": "ubuntu-24.04",
	})
	okStatus := schemapb.MustStructFromGo(map[string]any{"id": "i-123", "ip": "10.0.0.5"})
	path := []string{"acme", "prod", "deploy", "app"}

	// Valid instance; version 0 resolves and returns the pinned version.
	pinned, err := r.ValidateInstance(ctx, "aws.vm", path, 0, okSpec, okStatus)
	if err != nil {
		t.Fatalf("valid instance rejected: %v", err)
	}
	if pinned != 1 {
		t.Fatalf("pinned version: got %d want 1", pinned)
	}

	// Empty status is fine: it is filled later by controllers.
	if _, err := r.ValidateInstance(ctx, "aws.vm", path, 0, okSpec, nil); err != nil {
		t.Fatalf("empty status rejected: %v", err)
	}

	// Wrong path arity.
	if _, err := r.ValidateInstance(ctx, "aws.vm", []string{"acme", "prod"}, 0, okSpec, nil); err == nil {
		t.Fatal("short path accepted")
	}

	// Missing required spec field.
	badSpec := schemapb.MustStructFromGo(map[string]any{"type": "t3.medium"})
	var verr *registry.ValidationError
	if _, err := r.ValidateInstance(ctx, "aws.vm", path, 0, badSpec, nil); !errors.As(err, &verr) {
		t.Fatalf("bad spec: want ValidationError, got %v", err)
	}

	// Instances of the reserved kind are rejected.
	if _, err := r.ValidateInstance(ctx, "Kind", []string{"x"}, 0, okSpec, nil); !errors.Is(err, registry.ErrReservedKind) {
		t.Fatalf("reserved kind: got %v", err)
	}
}
