package def_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
)

func processKind(t *testing.T) kind.Kind {
	t.Helper()

	named, err := kind.New("Process")
	if err != nil {
		t.Fatalf("kind: %v", err)
	}

	return named
}

func processShape(t *testing.T) path.TPath {
	t.Helper()

	shape, err := path.NewTPath("kernel", "name")
	if err != nil {
		t.Fatalf("shape: %v", err)
	}

	return shape
}

func specSchema(t *testing.T) def.SpecSchema {
	t.Helper()

	return def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-spec"}).
		Fields(schemapb.Str("bundle").Required(), schemapb.Str("identity")).
		MustBuild())
}

func statusSchema(t *testing.T) def.StatusSchema {
	t.Helper()

	return def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-status"}).
		Fields(schemapb.Str("phase")).
		MustBuild())
}

func ref(t *testing.T, field, kind string) def.Ref {
	t.Helper()

	built, err := def.ParseRef(field, kind, def.Strong)
	if err != nil {
		t.Fatalf("ref %s → %s: %v", field, kind, err)
	}

	return built
}

// The two schemas are the same Go type and stand side by side, so they
// are wrapped: swapping them has to stop compiling, because nothing else
// would notice until the first resource was written.
func TestSchemasCannotBeSwapped(t *testing.T) {
	t.Parallel()

	built, err := def.New(processKind(t), processShape(t), specSchema(t), statusSchema(t))
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	// The wrapper keeps the schema's own methods, so holding one is not
	// worse than holding a schema.
	if built.Spec().GetId().GetName() != "process-spec" {
		t.Fatalf("spec: %v", built.Spec().GetId())
	}

	if built.Status().GetId().GetName() != "process-status" {
		t.Fatalf("status: %v", built.Status().GetId())
	}
}

// Everything is required: each of these is a mistake worth hearing about
// when the kind is declared, not at the first write.
func TestEverythingIsRequired(t *testing.T) {
	t.Parallel()

	var noKind kind.Kind

	if _, err := def.New(noKind, processShape(t), specSchema(t), statusSchema(t)); !errors.Is(err, def.ErrNoKind) {
		t.Fatalf("no kind: %v", err)
	}

	var noShape path.TPath

	if _, err := def.New(processKind(t), noShape, specSchema(t), statusSchema(t)); !errors.Is(err, def.ErrNoShape) {
		t.Fatalf("no shape: %v", err)
	}

	if _, err := def.New(processKind(t), processShape(t), def.SpecSchema{}, statusSchema(t)); !errors.Is(err, def.ErrNoSchema) {
		t.Fatalf("no spec: %v", err)
	}

	if _, err := def.New(processKind(t), processShape(t), specSchema(t), def.StatusSchema{}); !errors.Is(err, def.ErrNoSchema) {
		t.Fatalf("no status: %v", err)
	}
}

// Two references on one field would give "what does this value point at"
// two answers and nothing to choose between them.
func TestOneReferencePerField(t *testing.T) {
	t.Parallel()

	_, err := def.New(processKind(t), processShape(t), specSchema(t), statusSchema(t),
		def.Reference(ref(t, "spec.bundle", "Bundle")),
		def.Reference(ref(t, "spec.bundle", "Blob")),
	)
	if !errors.Is(err, def.ErrDuplicateRef) {
		t.Fatalf("want ErrDuplicateRef, got %v", err)
	}

	// Two references on DIFFERENT fields are ordinary.
	built, err := def.New(processKind(t), processShape(t), specSchema(t), statusSchema(t),
		def.Reference(ref(t, "spec.bundle", "Bundle")),
		def.Reference(ref(t, "spec.identity", "Identity")),
	)
	if err != nil {
		t.Fatalf("two fields: %v", err)
	}

	if len(built.Refs()) != 2 {
		t.Fatalf("refs: %v", built.Refs())
	}
}

// A reference needs all three of its parts. The strength is one of them
// and has no default: every value of it is a different answer to "what
// happens when the target is deleted", and there is no safe guess among
// refusing, cascading and doing nothing.
func TestReferenceNeedsAllThreeParts(t *testing.T) {
	t.Parallel()

	if _, err := def.ParseRef("", "Bundle", def.Strong); err == nil {
		t.Fatal("a reference with no field was accepted")
	}

	if _, err := def.ParseRef("spec.bundle", "", def.Strong); err == nil {
		t.Fatal("a reference with no kind was accepted")
	}

	_, err := def.ParseRef("spec.bundle", "Bundle", def.NoStrength)
	if !errors.Is(err, def.ErrRefStrength) {
		t.Fatalf("a reference with no strength: want ErrRefStrength, got %v", err)
	}
}

// Eq asks the only question anyone asks of two definitions: is this the
// same shape. It decides whether a kind needs a new version, so
// everything that changes how an instance is validated has to count.
func TestEqIsSameShape(t *testing.T) {
	t.Parallel()

	build := func(refs ...def.Ref) def.Definition {
		t.Helper()

		built, err := def.New(processKind(t), processShape(t), specSchema(t), statusSchema(t), def.Reference(refs...))
		if err != nil {
			t.Fatalf("build: %v", err)
		}

		return built
	}

	if !build().Eq(build()) {
		t.Fatal("the same shape built twice compared different")
	}

	// References change what a Put must resolve before accepting it, so
	// they are part of the shape.
	if build().Eq(build(ref(t, "spec.bundle", "Bundle"))) {
		t.Fatal("adding a reference did not change the shape")
	}

	other, err := path.NewTPath("kernel", "process")
	if err != nil {
		t.Fatalf("other shape: %v", err)
	}

	renamed, err := def.New(processKind(t), other, specSchema(t), statusSchema(t))
	if err != nil {
		t.Fatalf("renamed: %v", err)
	}

	if build().Eq(renamed) {
		t.Fatal("renaming a path position did not change the shape")
	}
}

// The version is not part of the definition, and reads the way people
// write it.
func TestVersionIsNotPartOfTheShape(t *testing.T) {
	t.Parallel()

	if def.NoVersion.String() != "v0" || !def.NoVersion.IsZero() {
		t.Fatalf("no version reads as %q", def.NoVersion)
	}

	if def.NoVersion.Next().String() != "v1" {
		t.Fatalf("first version reads as %q", def.NoVersion.Next())
	}

	if !def.Version(2).Eq(def.Version(1).Next()) {
		t.Fatal("versions do not follow one another")
	}
}

// A reference names a field, and a field that is not there is a typo the
// author should hear about now — not at the first write, far from where
// the mistake was made.
func TestReferenceFieldMustExist(t *testing.T) {
	t.Parallel()

	_, err := def.New(processKind(t), processShape(t), specSchema(t), statusSchema(t),
		def.Reference(ref(t, "spec.bundel", "Bundle"))) // the letters swapped
	if !errors.Is(err, def.ErrRefNoField) {
		t.Fatalf("want ErrRefNoField, got %v", err)
	}

	// A homoglyph never gets this far: the closed ASCII alphabet of a
	// field name refuses it while the path is still being parsed, which
	// is a better failure than "no such field".
	if _, err := def.ParseRef("spec.bundlе", "Bundle", def.Strong); err == nil { // Cyrillic е
		t.Fatal("a homoglyph was accepted as a field name")
	}

	// And the message says which segment broke, not the whole path.
	if err == nil || !strings.Contains(err.Error(), "no field") {
		t.Fatalf("message does not point at the segment: %v", err)
	}
}

// Only spec and status carry a schema; the rest of the envelope belongs
// to the store, and what it means is not a kind author's to decide.
func TestReferenceLivesInSpecOrStatus(t *testing.T) {
	t.Parallel()

	for _, field := range []string{"revision", "key.kind", "metadata.name"} {
		_, err := def.New(processKind(t), processShape(t), specSchema(t), statusSchema(t),
			def.Reference(ref(t, field, "Bundle")))
		if !errors.Is(err, def.ErrRefRoot) {
			t.Fatalf("%q: want ErrRefRoot, got %v", field, err)
		}
	}

	// status is as good a place as spec.
	if _, err := def.New(processKind(t), processShape(t), specSchema(t), statusSchema(t),
		def.Reference(ref(t, "status.phase", "Bundle"))); err != nil {
		t.Fatalf("a reference in status: %v", err)
	}
}

// A reference is a path, so the field has to be a string — or a list of
// them, which is the plural of the same thing and how one identity names
// several roles.
func TestReferenceFieldMustHoldAPath(t *testing.T) {
	t.Parallel()

	spec := def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "s"}).
		Fields(
			schemapb.Str("bundle"),
			schemapb.List("roles", schemapb.Str("role")),
			schemapb.Int64("count"),
			schemapb.List("sizes", schemapb.Int64("size")),
			schemapb.Object("nested", schemapb.Str("inner")),
		).
		MustBuild())

	ok := []string{"spec.bundle", "spec.roles", "spec.nested.inner"}
	for _, field := range ok {
		if _, err := def.New(processKind(t), processShape(t), spec, statusSchema(t),
			def.Reference(ref(t, field, "Bundle"))); err != nil {
			t.Fatalf("%q refused: %v", field, err)
		}
	}

	refused := []string{"spec.count", "spec.sizes", "spec.nested"}
	for _, field := range refused {
		_, err := def.New(processKind(t), processShape(t), spec, statusSchema(t),
			def.Reference(ref(t, field, "Bundle")))
		if !errors.Is(err, def.ErrRefKindMismatch) {
			t.Fatalf("%q: want ErrRefKindMismatch, got %v", field, err)
		}
	}
}

// A schema that does not compile is a kind that will never validate
// anything, and the place to hear about it is here.
func TestSchemasMustCompile(t *testing.T) {
	t.Parallel()

	// A rule that references a field nobody declared: valid protobuf,
	// impossible to compile.
	broken := def.Spec(&schemapb.Schema{
		Id:    &schemapb.SchemaIdentity{Name: "broken"},
		Rules: []*schemapb.Schema_Field_Rule{{Expr: "nonexistent > 1", Message: "…"}},
	})

	_, err := def.New(processKind(t), processShape(t), broken, statusSchema(t))
	if !errors.Is(err, def.ErrSchemaBroken) {
		t.Fatalf("want ErrSchemaBroken, got %v", err)
	}
}
