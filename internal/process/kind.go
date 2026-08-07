// Package process is what a kernel runs, and the record that says so.
//
// It is the whole execution surface of the kernel, and it is one kind:
// bytes, a kernel to run them on, and an identity to run them as. It
// exists for one reason — a controller is an ordinary client, so anything
// that can watch and write already is one, but on a machine where nothing
// runs yet nobody can start the first thing. Only the kernel there can.
//
// Everything people expect around it — scheduling, packaging, retry
// policy, pipelines — is somebody else's controller built on this, and
// none of it belongs here. k8s draws the line in the same place: the
// apiserver knows Pods, and Deployment, Job and CronJob live outside it.
package process

import (
	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/blob"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// The fields of a process, spelled once.
const (
	blobField     = "blob"
	formatField   = "format"
	argsField     = "args"
	envField      = "env"
	nameField     = "name"
	valueField    = "value"
	identityField = "identity"
	restartField  = "restart"

	phaseField    = "phase"
	exitCodeField = "exit_code"
	errorField    = "error"
	startsField   = "starts"
)

// The formats a kernel can turn bytes into a process with.
//
// The vocabulary lists only what can actually be run today. Accepting
// "oci" before anything runs it would mean storing a Process that
// validates, gets scheduled, and silently never starts; adding it later
// is one word and a new definition version.
const (
	// RawExec is the floor rather than a fallback: a bare machine has no
	// container runtime, and the first thing anybody wants to run there
	// is the thing that would have made running possible.
	RawExec = "raw-exec"
)

// What a process should do when it ends.
const (
	// RestartNever — it was asked for once and it happened. The record
	// stays: what became of it is the answer somebody is waiting to read.
	RestartNever = "never"
	// RestartAlways — a resident thing, a driver or a daemon, for which
	// an exit is a fault to recover from.
	RestartAlways = "always"
)

// Phases, as the status records them.
const (
	PhasePending = "pending"
	PhaseRunning = "running"
	PhaseExited  = "exited"
	PhaseFailed  = "failed"
)

// Kind names a process, and Shape addresses one.
//
// THE KERNEL IS THE FIRST SEGMENT, and that is not decoration. A grant is
// confined by a path prefix and nothing else, so a kind that did not put
// "which kernel" in its path could not be confined to one — and a kernel
// able to write another kernel's processes could run anything anywhere.
// It also makes the agent's watch a prefix rather than a filter.
//
//nolint:gochecknoglobals // a validated value cannot be a const; treated as one
var (
	Kind  = kind.MustNew("Process")
	Shape = path.MustNewTPath("kernel", "name")
)

// Definition is what a process record is shaped like.
//
// The identity is a DECLARED reference and not a string that happens to
// name one, and that is what makes writing it a guarded act: naming an
// identity is handing out everything that identity holds, so the kernel
// refuses a caller who does not hold it themselves. Declaring it also
// means the identity cannot be deleted while something runs as it.
func Definition() def.Definition {
	identity, err := def.NewRef(
		mustFieldPath(def.SpecRoot, identityField), auth.IdentityKind, def.Strong)
	if err != nil {
		panic("process definition: " + err.Error())
	}

	// The bytes are a REFERENCE and not a string that happens to hold an
	// id. That is what makes them real to the kernel: a process cannot
	// name bytes nobody stored, and the bytes cannot be removed while
	// something is running them. It is also what lets a manifest name a
	// file and have it uploaded on the way in.
	bytes, err := def.NewRef(
		mustFieldPath(def.SpecRoot, blobField), blob.Kind, def.Strong)
	if err != nil {
		panic("process definition: " + err.Error())
	}

	return def.MustNew(
		Kind,
		Shape,
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-spec"}).
			Fields(
				// Where the bytes are. A blob id, not a checksum: the
				// checksum says whether they arrived intact, never where
				// they live.
				schemapb.Str(blobField).Required(),
				schemapb.Str(formatField).In(RawExec).Required(),
				schemapb.List(argsField, schemapb.Str("arg")),
				// A list of pairs rather than a map: map VALUES carry a
				// schema of their own here, which would make every
				// variable an object wrapping a string. Pairs say what is
				// meant and read the way k8s writes the same thing.
				schemapb.List(envField,
					schemapb.Object("variable",
						schemapb.Str(nameField).Required(),
						schemapb.Str(valueField).Required(),
					),
				),
				// Which Identity the process acts as. Absent means no
				// credentials at all, which is right for anything that
				// never calls back.
				schemapb.Str(identityField),
				schemapb.Str(restartField).In(RestartNever, RestartAlways),
			).
			MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "process-status"}).
			Fields(
				schemapb.Str(phaseField).In(PhasePending, PhaseRunning, PhaseExited, PhaseFailed),
				schemapb.Int64(exitCodeField),
				schemapb.Str(errorField),
				// How many times the kernel has started it, so a crash
				// loop is visible in a listing rather than only in a log
				// somebody has to go and find.
				schemapb.Int64(startsField),
			).
			MustBuild()),
		def.Reference(identity, bytes),
	)
}

// mustFieldPath builds a field path written into the binary.
func mustFieldPath(names ...string) path.FieldPath {
	built, err := path.NewFieldPath(names...)
	if err != nil {
		panic("process field path: " + err.Error())
	}

	return built
}

// Id addresses one process on one kernel.
func Id(kernel, name string) (resource.Id, error) {
	at, err := Shape.New(kernel, name)
	if err != nil {
		return resource.Id{}, err
	}

	return resource.NewId(Kind, at), nil
}

// On addresses every process on one kernel — the prefix an agent watches.
func On(kernel string) (resource.Id, error) {
	at, err := Shape.New(kernel)
	if err != nil {
		return resource.Id{}, err
	}

	return resource.NewId(Kind, at), nil
}
