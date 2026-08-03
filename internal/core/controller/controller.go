// Package controller is the runtime a reconciliation loop runs in.
//
// A controller is an ordinary client: it watches a kind and writes what it
// decided back through the same API everyone else uses. Nothing here is
// privileged, and nothing here knows where the truth lives — the Stream it
// is given may read this process's own store or a kernel a link away.
//
// The kernel's own loops (the lease controller, the process agent) are
// built with exactly this, which is the only way to be sure the thing we
// hand to others actually works.
package controller

import (
	"context"

	"github.com/graphene-ci/graphene/internal/core/auth"
)

// SystemContext returns ctx carrying the system principal the kernel's own
// in-process controllers act as.
func SystemContext(ctx context.Context) context.Context {
	return auth.WithCredentials(ctx, auth.FullAccess(auth.PrincipalSystem, "controller"))
}
