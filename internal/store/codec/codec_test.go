package codec_test

import (
	"errors"
	"testing"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/common/str"
	"github.com/graphene-ci/graphene/internal/store/codec"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

func definition(t *testing.T) def.Definition {
	t.Helper()

	spec := def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-spec"}).
		Fields(schemapb.Str("bundle").Required()).
		MustBuild())

	status := def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-status"}).
		Fields(schemapb.Str("phase")).
		MustBuild())

	built, err := def.New(kind.MustNew("Process"), path.MustNewTPath("kernel", "name"), spec, status)
	if err != nil {
		t.Fatalf("definition: %v", err)
	}

	return built
}

func admitted(t *testing.T) resource.Resource {
	t.Helper()

	built := definition(t)

	at, err := built.Shape().New("local", "web")
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	intent, err := resource.NewIntent(resource.NewId(built.Kind(), at),
		schemapb.MustStructFromGo(map[string]any{"bundle": "b1"}))
	if err != nil {
		t.Fatalf("intent: %v", err)
	}

	value, err := resource.Admit(built, 4, intent, resource.Resource{})
	if err != nil {
		t.Fatalf("admit: %v", err)
	}

	return value
}

// A resource goes out and comes back, and its key comes off the value —
// so there is no way to write one resource under another one's key.
func TestAResourceEncodesAndDecodes(t *testing.T) {
	t.Parallel()

	var store codec.Resource

	original := admitted(t)

	raw, err := store.Encode(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	back, err := store.Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !back.Id().Eq(original.Id()) || !store.Id(back).Eq(original.Id()) {
		t.Fatalf("came back as %s", back.Id())
	}
}

// A definition is stored with the version the store gave it, because a
// bare definition cannot name itself.
func TestADefinitionEncodesAndDecodes(t *testing.T) {
	t.Parallel()

	var store codec.Definition

	published, err := def.Publish(definition(t), 2)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	raw, err := store.Encode(published)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	back, err := store.Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !back.Eq(published) {
		t.Fatalf("%s came back as %s", published, back)
	}

	at, err := def.PublishedPath(published.Kind(), published.Version())
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	want := resource.NewId(def.PublishedKind, at)
	if !store.Id(back).Eq(want) {
		t.Fatalf("addressed as %s, want %s", store.Id(back), want)
	}
}

// The tag is what makes a migration possible instead of archeological,
// and it earns its two bytes by telling three different failures apart —
// each of which is a different search.
func TestTheTagTellsThreeFailuresApart(t *testing.T) {
	t.Parallel()

	var store codec.Resource

	raw, err := store.Encode(admitted(t))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	if _, err := store.Decode(raw[:1]); !errors.Is(err, codec.ErrTruncated) {
		t.Fatalf("a cut value: want ErrTruncated, got %v", err)
	}

	// Somebody else's bytes. Without the tag these reach proto.Unmarshal,
	// which is cheerful about garbage and often hands back an empty
	// message rather than an error — a record that looks like it decoded.
	foreign := append([]byte{}, raw...)
	foreign[0] = 'x'

	if _, err := store.Decode(foreign); !errors.Is(err, codec.ErrForeign) {
		t.Fatalf("another system's bytes: want ErrForeign, got %v", err)
	}

	// Ours, from a layout this build does not read: a migration, not a
	// corruption, and the two are fixed differently.
	older := append([]byte{}, raw...)
	older[1] = 0xFF

	if _, err := store.Decode(older); !errors.Is(err, codec.ErrFormat) {
		t.Fatalf("an unknown format: want ErrFormat, got %v", err)
	}

	// Right tag, wrong body: real corruption.
	broken := append([]byte{}, raw...)
	broken[len(broken)-1] ^= 0xFF

	if _, err := store.Decode(broken); err == nil {
		t.Fatal("a corrupted body decoded")
	}
}

// Bytes that would make an impossible resource produce an error, not an
// impossible resource: decoding goes through the same door admission
// does.
func TestBytesThatWouldMakeAnImpossibleResourceAreRefused(t *testing.T) {
	t.Parallel()

	var store codec.Resource

	empty := []byte{0x67, 1}

	if _, err := store.Decode(empty); !errors.Is(err, resource.ErrNoId) {
		t.Fatalf("an empty message: want ErrNoId, got %v", err)
	}
}

// A head holds the current definition itself, not a pointer at a
// version — so reading it is one Get, and deleting an old version leaves
// nothing dangling.
func TestAHeadEncodesAndDecodes(t *testing.T) {
	t.Parallel()

	var store codec.Head

	published, err := def.Publish(definition(t), 2)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	head, err := def.NewHead(published)
	if err != nil {
		t.Fatalf("head: %v", err)
	}

	raw, err := store.Encode(head)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	back, err := store.Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !back.Eq(head) {
		t.Fatalf("%s came back as %s", head, back)
	}

	// The whole of the difference between the two codecs: where the same
	// value belongs.
	at, err := def.HeadPath(head.Kind())
	if err != nil {
		t.Fatalf("path: %v", err)
	}

	if want := resource.NewId(def.HeadKind, at); !store.Id(back).Eq(want) {
		t.Fatalf("addressed as %s, want %s", store.Id(back), want)
	}

	var versioned codec.Definition
	if store.Id(back).Eq(versioned.Id(published)) {
		t.Fatal("the head and the version it copies share a record")
	}
}

// Bytes that would make an impossible definition are refused on the way
// in, the same as they would be on the way out.
//
// The refusal comes from the kind rules and not from def, because that is
// the first thing an empty message fails: it names no kind, and an unnamed
// kind is caught where kind names are decided rather than three layers
// later.
func TestAHeadOfNothingIsRefused(t *testing.T) {
	t.Parallel()

	var store codec.Head

	if _, err := store.Decode([]byte{0x67, 1}); !errors.Is(err, str.ErrEmpty) {
		t.Fatalf("want an empty kind name, got %v", err)
	}
}
