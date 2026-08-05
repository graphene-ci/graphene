package resource_test

import (
	"errors"
	"testing"

	"github.com/gopherex/schemapb/go/schemapb"
	"github.com/graphene-ci/graphene/internal/types/kind"

	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// The kind these tests admit against: a process with a required bundle
// and a status nobody has to fill in.
func definition(t *testing.T) def.Definition {
	t.Helper()

	kind, err := kind.New("Process")
	if err != nil {
		t.Fatalf("kind: %v", err)
	}

	shape, err := path.NewTPath("kernel", "name")
	if err != nil {
		t.Fatalf("shape: %v", err)
	}

	spec := def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-spec"}).
		Fields(schemapb.Str("bundle").Required(), schemapb.Str("identity")).
		MustBuild())

	status := def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-status"}).
		Fields(schemapb.Str("phase")).
		MustBuild())

	built, err := def.New(kind, shape, spec, status)
	if err != nil {
		t.Fatalf("definition: %v", err)
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

func spec(bundle string) *schemapb.StructValue {
	return schemapb.MustStructFromGo(map[string]any{"bundle": bundle})
}

func intent(t *testing.T, d def.Definition, bundle string) resource.Intent {
	t.Helper()

	stated, err := resource.NewIntent(id(t, d, "local", "web"), spec(bundle))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	return stated
}

func admit(t *testing.T, d def.Definition, in resource.Intent, previous resource.Resource) resource.Resource {
	t.Helper()

	admitted, err := resource.Admit(d, 1, in, previous)
	if err != nil {
		t.Fatalf("admit: %v", err)
	}

	return admitted
}

// An intent is what an author may say, and the fields the kernel owns are
// not on it to be said. This is the whole reason the type exists, so it
// is asserted rather than assumed: everything below comes out of Admit
// and nothing comes in.
func TestAnIntentCarriesOnlyWhatAnAuthorOwns(t *testing.T) {
	t.Parallel()

	definition := definition(t)
	admitted := admit(t, definition, intent(t, definition, "b1"), resource.Resource{})

	if admitted.Generation() != 1 {
		t.Fatalf("a creation started at generation %s", admitted.Generation())
	}

	if !admitted.DefinitionVersion().Eq(1) {
		t.Fatalf("pinned %s", admitted.DefinitionVersion())
	}

	if admitted.IsDeleting() || admitted.Status() != nil {
		t.Fatal("a fresh resource arrived already deleting or already reported on")
	}
}

// The spec is copied on the way in. An intent that kept the caller's
// message would be an immutable value with a mutable inside: the caller
// could go on editing what it had already submitted.
func TestTheSpecIsCopiedIn(t *testing.T) {
	t.Parallel()

	definition := definition(t)
	submitted := spec("b1")

	stated, err := resource.NewIntent(id(t, definition, "local", "web"), submitted)
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	submitted.Fields["bundle"] = schemapb.StrV("tampered")

	if stated.Spec().ToGo()["bundle"] != "b1" {
		t.Fatal("editing the submitted message reached inside the intent")
	}
}

// Generation counts INTENT. A status report moves the revision and must
// not move this, or the controller that wrote it wakes itself forever.
func TestGenerationMovesWithTheSpecAndNothingElse(t *testing.T) {
	t.Parallel()

	definition := definition(t)
	first := admit(t, definition, intent(t, definition, "b1"), resource.Resource{})

	same := admit(t, definition, intent(t, definition, "b1"), first)
	if same.Generation() != first.Generation() {
		t.Fatalf("an unchanged spec moved the generation to %s", same.Generation())
	}

	changed := admit(t, definition, intent(t, definition, "b2"), first)
	if !changed.Generation().After(first.Generation()) {
		t.Fatalf("a changed spec left the generation at %s", changed.Generation())
	}

	reported, err := resource.Report(definition, changed,
		schemapb.MustStructFromGo(map[string]any{"phase": "running"}))
	if err != nil {
		t.Fatalf("report: %v", err)
	}

	if reported.Generation() != changed.Generation() {
		t.Fatal("a status report moved the generation")
	}

	if reported.Status().ToGo()["phase"] != "running" {
		t.Fatalf("status came back as %v", reported.Status().ToGo())
	}
}

// A spec write must not erase what a controller reported, and must not
// revive something already on its way out.
func TestAdmissionCarriesTheKernelsOwnFieldsOver(t *testing.T) {
	t.Parallel()

	definition := definition(t)
	created := admit(t, definition, intent(t, definition, "b1"), resource.Resource{})

	reported, err := resource.Report(definition, created,
		schemapb.MustStructFromGo(map[string]any{"phase": "running"}))
	if err != nil {
		t.Fatalf("report: %v", err)
	}

	claimed, err := resource.Claim(reported, finalizer(t, "gc"))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	deleting, err := resource.MarkDeleting(claimed)
	if err != nil {
		t.Fatalf("mark: %v", err)
	}

	// The same spec, so the write is allowed even while deleting.
	again := admit(t, definition, intent(t, definition, "b1"), deleting)

	if !again.IsDeleting() {
		t.Fatal("a spec write revived a resource that was going away")
	}

	if again.Status().ToGo()["phase"] != "running" {
		t.Fatal("a spec write erased the controller's report")
	}

	if len(again.Finalizers()) != 1 {
		t.Fatalf("a spec write changed the claims on it: %v", again.Finalizers())
	}
}

// The two ways an update is really a different resource wearing the same
// path.
func TestAnAdmissionMustFollowFromWhatIsThere(t *testing.T) {
	t.Parallel()

	definition := definition(t)
	existing := admit(t, definition, intent(t, definition, "b1"), resource.Resource{})

	elsewhere, err := resource.NewIntent(id(t, definition, "local", "other"), spec("b1"))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	if _, err := resource.Admit(definition, 1, elsewhere, existing); !errors.Is(err, resource.ErrIdChanged) {
		t.Fatalf("writing over another id: want ErrIdChanged, got %v", err)
	}

	claimed, err := resource.Claim(
		admit(t, definition, intent(t, definition, "b1"), resource.Resource{}),
		finalizer(t, "gc"))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	deleting, err := resource.MarkDeleting(claimed)
	if err != nil {
		t.Fatalf("mark: %v", err)
	}

	if _, err := resource.Admit(definition, 1, intent(t, definition, "b2"),
		deleting); !errors.Is(err, resource.ErrDeleting) {
		t.Fatalf("changing the spec while deleting: want ErrDeleting, got %v", err)
	}
}

// A resource is admitted against the kind that describes it, or not at
// all: the wrong kind validates by the wrong schema, the wrong shape
// lands the key in another kind's space.
func TestAdmissionRefusesWhatTheDefinitionDoesNotDescribe(t *testing.T) {
	t.Parallel()

	definition := definition(t)

	otherKind, err := kind.New("Volume")
	if err != nil {
		t.Fatalf("kind: %v", err)
	}

	at, err := definition.Shape().New("local", "web")
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	foreign, err := resource.NewIntent(resource.NewId(otherKind, at), spec("b1"))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	if _, err := resource.Admit(definition, 1, foreign, resource.Resource{}); !errors.Is(err, resource.ErrKindMismatch) {
		t.Fatalf("want ErrKindMismatch, got %v", err)
	}

	otherShape, err := path.NewTPath("tenant", "name")
	if err != nil {
		t.Fatalf("shape: %v", err)
	}

	misshapen, err := otherShape.New("local", "web")
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	wrong, err := resource.NewIntent(resource.NewId(definition.Kind(), misshapen), spec("b1"))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	if _, err := resource.Admit(definition, 1, wrong, resource.Resource{}); !errors.Is(err, resource.ErrShapeMismatch) {
		t.Fatalf("want ErrShapeMismatch, got %v", err)
	}
}

// The spec is checked against the kind's schema, and every fault comes
// back at once: a person fixing a manifest wants the whole list.
func TestTheSpecIsCheckedAgainstTheSchema(t *testing.T) {
	t.Parallel()

	definition := definition(t)

	empty, err := resource.NewIntent(id(t, definition, "local", "web"),
		schemapb.MustStructFromGo(map[string]any{}))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	_, err = resource.Admit(definition, 1, empty, resource.Resource{})
	if !errors.Is(err, resource.ErrInvalid) {
		t.Fatalf("a missing required field: want ErrInvalid, got %v", err)
	}

	var invalid resource.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("no InvalidError to read the faults from: %v", err)
	}

	if invalid.Half != resource.SpecHalf || len(invalid.Faults) == 0 {
		t.Fatalf("faults came back as %+v", invalid)
	}
}

// A version is what a resource pins so that whatever reads it later knows
// which shape it was written under. Admitting without one pins nothing.
func TestAdmissionNeedsAVersionToPin(t *testing.T) {
	t.Parallel()

	definition := definition(t)

	if _, err := resource.Admit(definition, def.NoVersion, intent(t, definition, "b1"),
		resource.Resource{}); !errors.Is(err, resource.ErrNoVersion) {
		t.Fatalf("want ErrNoVersion, got %v", err)
	}
}

// Marking a deletion is for resources something is waiting on. With
// nothing to wait for, the record should go — a mark would leave a
// tombstone nobody ever clears.
func TestDeletionIsOnlyMarkedWhenSomethingIsWaiting(t *testing.T) {
	t.Parallel()

	definition := definition(t)
	plain := admit(t, definition, intent(t, definition, "b1"), resource.Resource{})

	if _, err := resource.MarkDeleting(plain); !errors.Is(err, resource.ErrNoFinalizers) {
		t.Fatalf("want ErrNoFinalizers, got %v", err)
	}

	claimed, err := resource.Claim(plain, finalizer(t, "gc"))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	marked, err := resource.MarkDeleting(claimed)
	if err != nil {
		t.Fatalf("mark: %v", err)
	}

	// A second delete of the same thing is the same request.
	twice, err := resource.MarkDeleting(marked)
	if err != nil || !twice.IsDeleting() {
		t.Fatalf("marking twice: %v", err)
	}
}

// A resource survives being written down and read back exactly, without
// being re-validated: it pins the version it was admitted under, and the
// definition may be versions on by now.
func TestRestoreIsTheInverseOfFlatten(t *testing.T) {
	t.Parallel()

	definition := definition(t)
	original := admit(t, definition,
		intent(t, definition, "b1"), resource.Resource{})

	restored, err := resource.Restore(original.Flatten())
	if err != nil {
		t.Fatalf("restore: %v", err)
	}

	if !restored.Id().Eq(original.Id()) ||
		restored.Generation() != original.Generation() ||
		!restored.DefinitionVersion().Eq(original.DefinitionVersion()) {
		t.Fatalf("%+v did not survive a round trip", restored.Flatten())
	}

	// A generation of zero is a resource that never went through an
	// admission, whatever the bytes claim.
	broken := original.Flatten()
	broken.Generation = resource.NoGeneration

	if _, err := resource.Restore(broken); !errors.Is(err, resource.ErrNoResource) {
		t.Fatalf("want ErrNoResource, got %v", err)
	}

	// A tombstone with nothing to wait for would never be cleared.
	orphan := original.Flatten()
	orphan.Deleting = true
	orphan.Finalizers = nil

	if _, err := resource.Restore(orphan); !errors.Is(err, resource.ErrNoFinalizers) {
		t.Fatalf("want ErrNoFinalizers, got %v", err)
	}
}
