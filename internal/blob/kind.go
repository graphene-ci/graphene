package blob

import (
	"encoding/hex"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// A blob is a resource, and the bytes are what it is about.
//
// THIS IS WHY, because it looks like bookkeeping and is not. A resource
// pointing at bytes used to point at nothing the kernel knew about: the
// id was a string, so nothing stopped a Process naming bytes nobody
// stored, and nothing stopped somebody removing bytes a Process was
// running. Two rules the kernel already enforces for everything else —
// refuse to point at what is not there, refuse to remove what is pointed
// at — could not reach the one place they were needed most.
//
// So the record about the bytes is an ordinary resource, an ordinary
// strong reference points at it, and both rules apply without a line
// being written for them. What is left outside is only the bytes
// themselves, which are large, streamed, and nobody's reference.
//
// Collecting bytes nothing points at is a controller's job, and the
// controller is a client: list Blob, ask Holders, delete the ones nobody
// holds. The kernel refuses the ones somebody does.
const (
	sizeField     = "size"
	checksumField = "sha256"
)

// Kind names the record about one blob, and Shape addresses it by the id
// the store issued.
//
//nolint:gochecknoglobals // a validated value cannot be a const; treated as one
var (
	Kind  = kind.MustNew("Blob")
	Shape = path.MustNewTPath("id")
)

// Definition is what a blob's record is shaped like.
//
// The SPEC is what was stored and cannot change: an id names those bytes
// and no others, so size and checksum are facts about it rather than
// intent. They are in the spec anyway, because whoever uploaded the bytes
// is the author of the record — there is no controller here to report
// anything, and a status nobody writes would be a half of a resource
// kept for symmetry.
func Definition() def.Definition {
	return def.MustNew(
		Kind,
		Shape,
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "blob-spec"}).
			Fields(
				schemapb.UInt64(sizeField),
				// Hex rather than bytes, because this is read by people
				// as often as by programs and compared by eye when
				// something does not match.
				schemapb.Str(checksumField),
			).
			MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "blob-status"}).MustBuild()),
	)
}

// Said is the record for bytes that were just stored.
func Said(info Info) *schemapb.StructValue {
	return schemapb.MustStructFromGo(map[string]any{
		sizeField:     info.Size,
		checksumField: hex.EncodeToString(info.SHA256),
	})
}

// Told reads back what the record says about a blob.
//
// The id comes from where the record was FOUND rather than from a field
// inside it: a resource is addressed by its path, so an id written in the
// spec as well would be a second place for it to be, and one of the two
// would eventually be wrong.
func Told(id Id, stored resource.Resource) Info {
	size, _ := schemapb.As[uint64](stored.Spec().GetFields()[sizeField])
	written, _ := schemapb.As[string](stored.Spec().GetFields()[checksumField])

	sum, err := hex.DecodeString(written)
	if err != nil {
		// A checksum that will not decode is one nobody can compare
		// against, which is what an absent one already means.
		sum = nil
	}

	return Info{Id: id, Size: size, SHA256: sum}
}

// ResourceId addresses the record about one blob.
func ResourceId(id Id) (resource.Id, error) {
	at, err := Shape.New(id.String())
	if err != nil {
		return resource.Id{}, err
	}

	return resource.NewId(Kind, at), nil
}
