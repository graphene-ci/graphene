package app

import (
	"context"

	"github.com/gopherex/xlog"

	"github.com/graphene-ci/graphene/internal/app/config"
	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/kernel"
)

// begin makes the first caller if this store has never had one, and
// writes the credential into the kernel's own file.
//
// ONE FILE PER KERNEL. The credential goes where the store it belongs to
// is named, beside it, the same way a subordinate's credential sits
// beside the address it opens. A second file would be a second thing to
// find, to back up and to lose.
//
// The secret exists in the clear only between being made and being
// written. What the store keeps is a digest, so a credential that failed
// to reach the file is one nobody will ever have again — the store would
// have to be replaced to get another. That is why the write is fatal
// rather than logged.
//
// A file that already NAMES a token is taken as given: the store gets
// that identity rather than a minted one, which is how a kernel can be
// installed with a credential somebody chose in advance.
//
// A subordinate does none of this. Its identities are the kernel above's,
// and its own credential was written in its configuration by whoever
// created it up there.
func begin(
	ctx context.Context,
	k kernel.Kernel,
	live *config.Live,
	local config.Local,
	log *xlog.Logger,
) error {
	token, made, err := auth.Begin(ctx, k, local.Token())
	if err != nil {
		return err
	}

	if !made {
		return nil
	}

	if local.Token() != "" {
		// The file said who to make, and it has been made. Nothing to
		// write back: the credential was already where it belongs.
		return nil
	}

	if err := live.Begun(token.String()); err != nil {
		return err
	}

	// The FILE and not the token. This line goes to a journal that is
	// read by more people, and kept for longer, than the file is.
	log.Info("first identity created",
		xlog.String("name", auth.First),
		xlog.String("written", live.Path()))

	return nil
}
