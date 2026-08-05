package convert_test

import (
	"errors"
	"testing"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/convert"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// definition is the kind these tests convert instances of.
func definition(t *testing.T) def.Definition {
	t.Helper()

	named, err := kind.New("Process")
	if err != nil {
		t.Fatalf("kind: %v", err)
	}

	shape, err := path.NewTPath("kernel", "name")
	if err != nil {
		t.Fatalf("shape: %v", err)
	}

	spec := def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-spec"}).
		Fields(schemapb.Str("bundle").Required()).
		MustBuild())

	status := def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-status"}).
		Fields(schemapb.Str("phase")).
		MustBuild())

	built, err := def.New(named, shape, spec, status, ref(t, "spec.bundle", "Bundle"))
	if err != nil {
		t.Fatalf("definition: %v", err)
	}

	return built
}

func ref(t *testing.T, field, named string) def.Ref {
	t.Helper()

	built, err := def.ParseRef(field, named, def.Strong)
	if err != nil {
		t.Fatalf("ref %s: %v", field, err)
	}

	return built
}

func id(t *testing.T, d def.Definition, values ...string) resource.Id {
	t.Helper()

	at, err := d.Shape().New(values...)
	if err != nil {
		t.Fatalf("path %v: %v", values, err)
	}

	return resource.NewId(d.Kind(), at)
}

// EVERY field is set. The canary beside Snapshot catches a field added
// and never converted; only a round trip with nothing left at its zero
// value catches one converted in the wrong direction.
func TestAResourceSurvivesARoundTripWithEveryFieldSet(t *testing.T) {
	t.Parallel()

	definition := definition(t)

	finalizer, err := resource.NewFinalizer("graphene.io/gc")
	if err != nil {
		t.Fatalf("finalizer: %v", err)
	}

	intent, err := resource.NewIntent(
		id(t, definition, "local", "web"),
		schemapb.MustStructFromGo(map[string]any{"bundle": "b1"}),
		resource.WithFinalizers(finalizer),
	)
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	admitted, err := resource.Admit(definition, 7, intent, resource.Resource{})
	if err != nil {
		t.Fatalf("admit: %v", err)
	}

	reported, err := resource.Report(definition, admitted,
		schemapb.MustStructFromGo(map[string]any{"phase": "running"}))
	if err != nil {
		t.Fatalf("report: %v", err)
	}

	original, err := resource.MarkDeleting(reported)
	if err != nil {
		t.Fatalf("mark: %v", err)
	}

	back, err := convert.ResourceFromPb(convert.ResourceToPb(original))
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}

	switch {
	case !back.Id().Eq(original.Id()):
		t.Fatalf("id came back as %s", back.Id())
	case back.Spec().ToGo()["bundle"] != "b1":
		t.Fatalf("spec came back as %v", back.Spec().ToGo())
	case back.Status().ToGo()["phase"] != "running":
		t.Fatalf("status came back as %v", back.Status().ToGo())
	case len(back.Finalizers()) != 1 || !back.Finalizers()[0].Eq(finalizer):
		t.Fatalf("finalizers came back as %v", back.Finalizers())
	case back.Generation() != original.Generation():
		t.Fatalf("generation came back as %s", back.Generation())
	case !back.DefinitionVersion().Eq(7):
		t.Fatalf("version came back as %s", back.DefinitionVersion())
	case !back.IsDeleting():
		t.Fatal("the deletion mark did not survive")
	}
}

// The names of the path positions travel with the record, so it decodes
// without anybody asking the registry what the shape is.
func TestAPathIsRebuiltFromTheNamesTheRecordCarries(t *testing.T) {
	t.Parallel()

	definition := definition(t)
	message := convert.IdToPb(id(t, definition, "local", "web"))

	if len(message.GetPath()) != 2 {
		t.Fatalf("path came out as %v", message.GetPath())
	}

	if message.GetPath()[0].GetName() != "kernel" || message.GetPath()[0].GetValue() != "local" {
		t.Fatalf("first segment is %v", message.GetPath()[0])
	}

	back, err := convert.IdFromPb(message)
	if err != nil {
		t.Fatalf("id: %v", err)
	}

	// The shape came back too, which is what makes this the same id
	// rather than two strings that happen to match.
	if !back.Path().Shape().Eq(definition.Shape()) {
		t.Fatalf("shape came back as %s", back.Path().Shape())
	}
}

// A message refuses nothing, so everything it carries is checked on the
// way in. These are values no domain type would ever produce.
func TestAMessageThatWouldMakeAnImpossibleValueIsRefused(t *testing.T) {
	t.Parallel()

	definition := definition(t)
	good := convert.ResourceToPb(mustAdmit(t, definition))

	broken := convert.ResourceToPb(mustAdmit(t, definition))
	broken.Id.Kind = "not a kind"

	if _, err := convert.ResourceFromPb(broken); err == nil {
		t.Fatal("a kind nobody could have named was accepted")
	}

	broken = convert.ResourceToPb(mustAdmit(t, definition))
	broken.Generation = 0

	if _, err := convert.ResourceFromPb(broken); !errors.Is(err, resource.ErrNoResource) {
		t.Fatalf("a resource that was never admitted: %v", err)
	}

	broken = convert.ResourceToPb(mustAdmit(t, definition))
	broken.Id.Path[0].Name = broken.Id.Path[1].Name

	if _, err := convert.ResourceFromPb(broken); !errors.Is(err, path.ErrDuplicateName) {
		t.Fatalf("a shape naming one position twice: %v", err)
	}

	// The good one still passes, so the failures above are the fields and
	// not the fixture.
	if _, err := convert.ResourceFromPb(good); err != nil {
		t.Fatalf("the untouched message: %v", err)
	}
}

// A definition goes back through def.New, which compiles both schemas and
// resolves every reference — so one that loads is one that would have
// been accepted.
func TestADefinitionSurvivesARoundTrip(t *testing.T) {
	t.Parallel()

	published, err := def.Publish(definition(t), 3)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	back, err := convert.DefinitionFromPb(convert.DefinitionToPb(published))
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}

	if !back.Eq(published) {
		t.Fatalf("%s came back as %s", published, back)
	}

	if len(back.Definition().Refs()) != 1 {
		t.Fatalf("refs came back as %v", back.Definition().Refs())
	}
}

// A definition whose reference points at a field its schema does not have
// is refused on the way in, the same as it would be on the way out.
func TestADefinitionWithABrokenRefIsRefused(t *testing.T) {
	t.Parallel()

	published, err := def.Publish(definition(t), 3)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	message := convert.DefinitionToPb(published)
	message.Refs[0].Field = []string{"spec", "nothing"}

	if _, err := convert.DefinitionFromPb(message); !errors.Is(err, def.ErrRefNoField) {
		t.Fatalf("want ErrRefNoField, got %v", err)
	}
}

func mustAdmit(t *testing.T, definition def.Definition) resource.Resource {
	t.Helper()

	intent, err := resource.NewIntent(
		id(t, definition, "local", "web"),
		schemapb.MustStructFromGo(map[string]any{"bundle": "b1"}),
	)
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	admitted, err := resource.Admit(definition, 1, intent, resource.Resource{})
	if err != nil {
		t.Fatalf("admit: %v", err)
	}

	return admitted
}
