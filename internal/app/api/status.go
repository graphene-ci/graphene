// Package api is what a kernel answers, and nothing about how it is
// reached.
//
// It implements the generated service and stops there: no listener, no
// socket, no protocol. That split is what lets one implementation answer
// on more than one transport — gRPC and HTTP reach the same methods
// rather than each growing a copy of the rules.
//
// It is a translation and nothing more: a request becomes the arguments
// of a session's method, and what comes back becomes a reply. There is no
// decision here that the kernel has not already made — no filtering, no
// defaulting, no retrying — because every one of those would be a rule
// living where nobody would think to look for it.
package api

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/blob"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// known maps the failures the kernel names to the codes a caller acts on.
//
// The mapping is by what a caller should DO, not by how the failure feels.
// That is why a stale write and a lagged watcher are not the same code
// even though both mean "you are out of date": one is retried after a
// re-read, the other cannot be retried at all and needs a fresh snapshot.
//
// Order matters below — the first match wins — so anything that wraps
// another must come first.
//
//nolint:gochecknoglobals // a validated value cannot be a const; treated as one
var known = []struct {
	is   error
	code codes.Code
}{
	// The caller is not who it needs to be, or is not allowed. These are
	// two different fixes: one is a credential, the other a grant.
	{auth.ErrNoPrincipal, codes.Unauthenticated},
	// A credential that is not one and a credential that does not match
	// are the same answer, for the same reason the lookup behind them is:
	// telling them apart turns a login into a way to find out who is
	// registered.
	{auth.ErrMalformedToken, codes.Unauthenticated},
	{auth.ErrBadToken, codes.Unauthenticated},
	{auth.ErrForbidden, codes.PermissionDenied},
	{auth.ErrEscalation, codes.PermissionDenied},

	// The caller read something and somebody else wrote it. Re-read and
	// decide again; this is the one failure that is normal.
	{revision.ErrConflict, codes.Aborted},

	// The caller is out of date beyond repair. Retrying never helps: the
	// history it wants is gone, so it starts over from a snapshot.
	{revision.ErrCompacted, codes.OutOfRange},
	{kv.ErrLagged, codes.OutOfRange},

	{store.ErrNotFound, codes.NotFound},
	{blob.ErrNotFound, codes.NotFound},

	// The bytes are not what they were said to be. Two faults and two
	// codes, because the answers differ: corrupted content is DataLoss
	// and cannot be fixed by sending the same thing again, while a count
	// that disagrees is the caller's arithmetic and is InvalidArgument.
	{blob.ErrChecksumMismatch, codes.DataLoss},
	{blob.ErrSizeMismatch, codes.InvalidArgument},

	// The request was well formed and the world was not ready for it.
	// Nothing about the request would be different next time; something
	// about the store has to change first.
	{kernel.ErrNoSuchKind, codes.FailedPrecondition},
	{kernel.ErrNoSuchVersion, codes.FailedPrecondition},
	{kernel.ErrKindInUse, codes.FailedPrecondition},
	{kernel.ErrReferenced, codes.FailedPrecondition},
	{kernel.ErrRefMissing, codes.FailedPrecondition},
	{kernel.ErrReservedKind, codes.FailedPrecondition},
	{kernel.ErrOwnerChanged, codes.FailedPrecondition},
	{resource.ErrDeleting, codes.FailedPrecondition},
	{resource.ErrClaimWhileDeleting, codes.FailedPrecondition},

	// The request itself is wrong, and no amount of waiting fixes it.
	{resource.ErrInvalid, codes.InvalidArgument},
	{resource.ErrNoId, codes.InvalidArgument},
	{resource.ErrNotExact, codes.InvalidArgument},
	{resource.ErrNoSpec, codes.InvalidArgument},
	{resource.ErrNoStatus, codes.InvalidArgument},
	{resource.ErrNoIntent, codes.InvalidArgument},
	{resource.ErrKindMismatch, codes.InvalidArgument},
	{resource.ErrShapeMismatch, codes.InvalidArgument},
	{auth.ErrUnknownVerb, codes.InvalidArgument},

	// The kernel is going away.
	{kv.ErrClosed, codes.Unavailable},
}

// fail turns what the kernel said into what a caller is told.
//
// A failure this list does not name becomes Internal with a flat message,
// and the original goes to the log instead. It is not modesty: an
// unexpected error's text is written for whoever is reading the source,
// and handing that to a caller is handing out the shape of the inside.
//
// The named ones keep their text, and that is the trade. Every one of
// them was written to be read by whoever hit it.
func fail(err error, log func(error)) error {
	if err == nil {
		return nil
	}

	for _, mapped := range known {
		if errors.Is(err, mapped.is) {
			return status.Error(mapped.code, err.Error())
		}
	}

	// A cancelled or expired call is the caller's own doing and is not
	// worth a line in anybody's log.
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "cancelled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	}

	log(err)

	return status.Error(codes.Internal, "internal error")
}
