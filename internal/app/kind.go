package app

import (
	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// The kernel's own record, and the fields it carries.
//
// SECRETS DO NOT GO IN HERE, and the rule is worth writing at the top of
// the file that will tempt somebody to break it. A record named after
// configuration attracts them — a connection string, a webhook, a token
// for some registry — and a resource spec is the worst place for one:
// it is validated, watched, listed, and printed back inside the text of
// validation errors.
//
// When secrets are needed they will need a mechanism of their own, and
// that is a design nobody has done yet.
//
// The KIND of a value is part of the contract too, not only the name of
// the field. schemapb maps a Go integer to its widest form, so a schema
// that said uint32 while the writer produced a uint64 would store a
// number the reader could not find — and finding nothing is
// indistinguishable from the field being unset, which is how it would be
// read as a default rather than as a mistake.
//
// So the integers here are uint64 everywhere: schema, writer and reader.
const (
	listenField = "listen"
	cacheField  = "cache"

	osField      = "os"
	archField    = "arch"
	versionField = "version"
)

// KernelKind names the record a kernel keeps about itself.
//
// One record per kernel, addressed by name, which is what makes it
// useful to more than the kernel it describes: delivering a controller to
// another kernel means knowing which platform to build it for, and that
// is what the status half is for.
var (
	KernelKind  = kind.MustNew("Kernel")
	KernelShape = path.MustNewTPath("kernel")
)

// Kernel is the definition of a kernel's own record.
//
// The two halves split by who writes them, the way they always do. An
// administrator sets the SPEC — what this kernel should do. The kernel
// reports the STATUS — what it is, which is not anybody's to choose.
func Kernel() def.Definition {
	return def.MustNew(
		KernelKind,
		KernelShape,
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "kernel-spec"}).
			Fields(
				schemapb.Str(listenField),
				schemapb.UInt64(cacheField),
			).
			MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "kernel-status"}).
			Fields(
				schemapb.Str(osField),
				schemapb.Str(archField),
				schemapb.Str(versionField),
			).
			MustBuild()),
	)
}

// KernelId addresses one kernel's record.
func KernelId(name string) (resource.Id, error) {
	at, err := KernelShape.New(name)
	if err != nil {
		return resource.Id{}, err
	}

	return resource.NewId(KernelKind, at), nil
}
