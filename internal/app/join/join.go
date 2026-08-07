// Package join is what a kernel needs to be allowed to do, on the kernel
// above it.
//
// THESE GRANTS ARE NOT A POLICY ANYBODY CHOSE. Each one is a consequence
// of something a kernel does, and each was learned by watching a kernel
// fail without it. Left to be typed by hand, every installation reinvents
// them and gets them subtly wrong — too narrow and the kernel silently
// stops reporting, too wide and a machine at the edge of the network can
// read the whole system. So they ship in the binary, as a function of the
// kernel's name.
//
// Only a SUBORDINATE needs them. A kernel that keeps its own store writes
// its record and reads its processes through that store, with no guard in
// front of it; there is nobody there to ask. What this package describes
// is the other case: a kernel whose store is a link away, talking to it
// the way anybody else does.
package join

import (
	"context"
	"errors"
	"fmt"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/app/report"
	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/blob"
	"github.com/graphene-ci/graphene/internal/process"
	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// prefix is what a kernel's own role is called. Named after the kernel
// because the grants are confined to that kernel's paths and fit no other
// one: a shared role would have to name no prefix, which is the same as
// letting any kernel write every kernel's records.
const prefix = "kernel-"

// ErrExists — something is already there under that name.
//
// Refused rather than overwritten. Joining a kernel mints a credential,
// and doing that over an identity somebody is using would take a machine
// off the network in a way nobody would connect to the command they ran.
var ErrExists = errors.New("that kernel has already joined")

// RoleName is what one kernel's role is called.
func RoleName(kernel string) string { return prefix + kernel }

// Writer is the little of a kernel this package needs. An interface
// because joining is done THROUGH the guard, by somebody who holds what
// they are handing out — that check is the whole reason this is not done
// unguarded.
type Writer interface {
	Get(ctx context.Context, id resource.Id) (store.Value[resource.Resource], error)
	Put(ctx context.Context, intent resource.Intent, expect revision.Revision) (revision.Revision, error)
}

// Join makes the role and the identity one kernel needs, and hands back
// its credential.
//
// The credential exists in the clear once, here, and what is stored is a
// digest of it. Whoever runs this writes it into that kernel's
// configuration or loses it.
func Join(ctx context.Context, to Writer, kernel string) (auth.Token, error) {
	who, err := auth.NewPrincipal(kernel)
	if err != nil {
		return "", fmt.Errorf("kernel name: %w", err)
	}

	identityId, err := auth.IdentityId(who)
	if err != nil {
		return "", err
	}

	if _, err := to.Get(ctx, identityId); err == nil {
		return "", fmt.Errorf("%w: %s", ErrExists, kernel)
	} else if !errors.Is(err, store.ErrNotFound) {
		return "", err
	}

	if err := writeRole(ctx, to, kernel); err != nil {
		return "", err
	}

	token, err := auth.Issue(who)
	if err != nil {
		return "", err
	}

	_, secret, err := auth.Split(token.String())
	if err != nil {
		return "", err
	}

	identity, err := resource.NewIntent(identityId, schemapb.MustStructFromGo(map[string]any{
		auth.RolesField:   []any{RoleName(kernel)},
		auth.DigestsField: []any{auth.Digest(secret)},
	}))
	if err != nil {
		return "", err
	}

	if _, err := to.Put(ctx, identity, revision.Absent); err != nil {
		return "", fmt.Errorf("create %s: %w", identityId, err)
	}

	return token, nil
}

// writeRole puts down the grants, or leaves the ones already there.
func writeRole(ctx context.Context, to Writer, kernel string) error {
	id, err := auth.RoleId(RoleName(kernel))
	if err != nil {
		return err
	}

	switch _, err := to.Get(ctx, id); {
	case err == nil:
		// A role left behind by a kernel that was removed and is being
		// added again. Its grants are a function of the name, so the one
		// that is there is the one this would write.
		return nil
	case !errors.Is(err, store.ErrNotFound):
		return err
	}

	granted, err := Grants(kernel)
	if err != nil {
		return err
	}

	written, err := resource.NewIntent(id,
		schemapb.MustStructFromGo(map[string]any{"grants": granted}))
	if err != nil {
		return err
	}

	if _, err := to.Put(ctx, written, revision.Absent); err != nil {
		return fmt.Errorf("create %s: %w", id, err)
	}

	return nil
}

// Grants is everything a kernel named `kernel` may do, and why.
//
// Read it as a list of sentences about what a kernel is:
//
//	its own record   — it says what it is running and that it is alive.
//	                   Confined to its own path, because a kernel that
//	                   could write another's could keep a dead machine
//	                   looking alive, or move where the fleet thinks a
//	                   machine is.
//	its own processes — it reads what it was told to run and reports what
//	                   became of it. NOT put: an agent that could write
//	                   what to run would be arguing with its orders.
//	bytes            — it fetches what it was told to run. Read only, and
//	                   without a prefix, because a blob id is not a path
//	                   and there is nothing to confine it by.
func Grants(kernel string) ([]any, error) {
	if _, err := report.Id(kernel); err != nil {
		return nil, fmt.Errorf("kernel name: %w", err)
	}

	at := "/" + kernel

	granted := []any{}

	for _, verb := range []auth.Verb{auth.Get, auth.Put, auth.Report} {
		granted = append(granted, grant(verb, report.KernelKind.String(), at))
	}

	for _, verb := range []auth.Verb{auth.Get, auth.List, auth.Watch, auth.Report} {
		granted = append(granted, grant(verb, process.Kind.String(), at))
	}

	// Bytes, read only, everywhere: an id is not a path, so there is no
	// prefix that means "the bytes this kernel runs" — a kernel learns an
	// id by reading a process it was given, and can read no other.
	granted = append(granted, grant(auth.Get, blob.Kind.String(), ""))

	return granted, nil
}

func grant(verb auth.Verb, kind, at string) any {
	return map[string]any{"verb": verb.String(), "kind": kind, "prefix": at}
}
