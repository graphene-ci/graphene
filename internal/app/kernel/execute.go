package kernel

import (
	"context"
	"path/filepath"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/agent"
	"github.com/graphene-ci/graphene/internal/core/blob"
	"github.com/graphene-ci/graphene/internal/core/builtin"
	"github.com/graphene-ci/graphene/internal/core/controller"
	"github.com/graphene-ci/graphene/internal/infrastructure/blob/cache"
	"github.com/graphene-ci/graphene/internal/infrastructure/gateway"
	"github.com/graphene-ci/graphene/internal/infrastructure/runner/rawexec"
)

// runAgent starts the thing that makes a kernel a kernel: it runs the
// processes placed on it.
//
// Where it reads from and where the bytes come from are the only
// difference between a worker and a kernel holding its own truth, and
// both are passed in — the agent itself is the same code either way.
func (k *Kernel) runAgent(ctx context.Context, group *errgroup.Group,
	stream controller.Stream, writer controller.Writer, bytes blob.Reader, door agent.Gateway,
) {
	worker := &agent.Agent{
		Kernel:  k.cfg.Identity.Name,
		Stream:  stream,
		Writer:  writer,
		Fetch:   cache.New(filepath.Join(k.cfg.DataDir, "cache"), bytes),
		Runner:  rawexec.New(filepath.Join(k.cfg.DataDir, "run")),
		Gateway: door,
		Log:     k.log,
	}

	group.Go(func() error { return worker.Run(ctx) })
}

// socketDir is where a process's door is opened. Under the data
// directory, so it lives and dies with the kernel's own state.
func (k *Kernel) socketDir() string {
	return filepath.Join(k.cfg.DataDir, "processes")
}

// runLinkedAgent runs processes against the kernel on the far side of the
// link: it watches, writes and fetches through the ordinary API, which is
// the whole reason a controller does not care where truth lives.
func (k *Kernel) runLinkedAgent(ctx context.Context, group *errgroup.Group, conn *grpc.ClientConn) {
	resources := graphenepbv1.NewResourceServiceClient(conn)

	k.runAgent(ctx, group,
		controller.Remote(resources, builtin.KindProcess, k.cfg.Identity.Name),
		controller.OverClient(resources),
		cache.OverClient(graphenepbv1.NewBlobServiceClient(conn)),
		gateway.OverClient(k.socketDir(), conn),
	)
}

// runLocalAgent runs processes against this kernel's own store. A single
// machine is not a special case: it is the same agent with a shorter path
// to the truth.
func (k *Kernel) runLocalAgent(ctx context.Context, group *errgroup.Group) {
	k.runAgent(controller.SystemContext(ctx), group,
		controller.Local(k.store, builtin.KindProcess, k.cfg.Identity.Name),
		controller.OverService(k.resources),
		cache.OverStore(k.blobs),
		gateway.OverService(k.socketDir(), k.cfg.Identity.Name, k.resources, k.blobSvc, k.tokens),
	)
}
