package report

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
// The kind of a value used to be part of the contract by hand, and got it
// wrong: schemapb writes a Go integer in its widest form, so a reader
// asking for the narrow one found nothing — and nothing is
// indistinguishable from unset, which reads as a default rather than as a
// mistake.
//
// schemapb.As closes that: it converts across kinds when the value is
// represented exactly, and reports PRESENCE rather than handing back a
// silent zero. The reader no longer has to guess how the writer spelled a
// number, only what it meant.
const (
	osField       = "os"
	archField     = "arch"
	versionField  = "version"
	listenField   = "listen"
	storeField    = "store"
	cacheField    = "cache"
	upstreamField = "upstream"
)

// KernelKind names the record a kernel keeps about itself.
//
// One record per kernel, addressed by name, which is what makes it
// useful to more than the kernel it describes: delivering a controller to
// another kernel means knowing which platform to build it for, and that
// is what the status half is for.
//
//nolint:gochecknoglobals // a validated value cannot be a const; treated as one
var (
	KernelKind  = kind.MustNew("Kernel")
	KernelShape = path.MustNewTPath("kernel")
)

// Definition is what a kernel's own record is shaped like.
//
// The SPEC IS EMPTY, and that is the whole design rather than an omission.
// A kernel is configured by a file, so nothing about it is anybody's to
// set from here — and the reason is recoverability: a configuration that
// could only be reached through the kernel could not be fixed when it was
// the thing that broke the kernel.
//
// What is left is a report. The kernel writes what it is running with,
// and a fleet can be asked how it is configured over the same API as
// everything else. Being told and reporting are different directions, and
// only one of them can be locked out.
func Definition() def.Definition {
	return def.MustNew(
		KernelKind,
		KernelShape,
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "kernel-spec"}).
			MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "kernel-status"}).
			Fields(
				schemapb.Str(osField),
				schemapb.Str(archField),
				schemapb.Str(versionField),
				schemapb.Str(listenField),
				schemapb.Str(storeField),
				schemapb.UInt64(cacheField),
				schemapb.Str(upstreamField),
			).
			MustBuild()),
	)
}

// Id addresses one kernel's record.
func Id(name string) (resource.Id, error) {
	at, err := KernelShape.New(name)
	if err != nil {
		return resource.Id{}, err
	}

	return resource.NewId(KernelKind, at), nil
}
