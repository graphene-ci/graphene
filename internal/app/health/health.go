// Package health is what a kernel answers about itself to whoever is
// deciding whether to send it work.
//
// It speaks grpc.health.v1 because that is the protocol every supervisor
// already knows — kubelet, a load balancer, grpc_health_probe — and it
// answers on the SAME port the kernel serves on. A separate health port
// can be listening while the thing it reports on is not, and then the
// answer is about the wrong process.
package health

import (
	"context"
	"time"

	"github.com/gopherex/xlog"
	"github.com/gopherex/xprobe/pkg/probe"
	"github.com/gopherex/xprobe/pkg/reporter"
	"github.com/gopherex/xprobe/pkg/runner"
	"github.com/gopherex/xprobe/pkg/state"
	grpcprobe "github.com/gopherex/xprobe/pkg/transport/grpc"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/graphene-ci/graphene/internal/types/revision"
)

// How often the store is asked whether it is still there, and how long it
// is given to say so.
//
// The check is a single read of the current revision, so it is cheap
// enough to repeat and complete enough to matter: it goes through the
// store, which is the only part of a kernel that can be alive as a
// process and dead as a service.
const (
	every  = 5 * time.Second
	within = 2 * time.Second
)

// whole is the name grpc.health.v1 gives the server as a whole, and the
// one a probe asks for when it has not been told otherwise.
const whole = ""

// Health is the kernel's own answer about itself: one cached status, one
// poller that keeps it true, and the service that reads it.
//
// Cached rather than checked per request, because the protocol has a
// Watch: somebody has to notice a change without being asked, and a
// status computed only when somebody asks can never be streamed.
type Health struct {
	poll   *runner.Runner
	server *grpcprobe.Server
}

// New builds the health of one kernel.
func New(from Source, log *xlog.Logger) *Health {
	registry := state.NewRegistry()

	return &Health{
		poll: runner.New(
			Probe(from),
			registry.Get(whole),
			runner.WithName("kernel"),
			runner.WithInterval(every),
			runner.WithTimeout(within),
			// Check once before the first tick, so a kernel that came up
			// healthy says so immediately rather than answering UNKNOWN
			// for the first interval — which a supervisor reads as "not
			// ready yet" and waits out.
			runner.RunImmediately(),
			runner.WithReporter(reported(log)),
		),
		server: grpcprobe.New(registry),
	}
}

// Source is what a kernel's health is asked of: anything that can be
// made to answer a read.
//
// An interface because the answer means the same thing whichever kind of
// kernel it is. A kernel that keeps a store is well when the store
// answers; a subordinate is well when the kernel above does, which is
// exactly as true and exactly as useful — a proxy whose upstream is gone
// has nothing to offer anybody.
type Source interface {
	Revision(ctx context.Context) (revision.Revision, error)
}

// Probe is the question itself, apart from anything that asks it.
//
// A read through the STORE. A kernel whose store has gone — the file
// unlinked, the disk gone, the handle closed — is a process that is up
// and a service that is not, and that difference is the only thing a
// health check is for. The current revision is the cheapest read there
// is and still goes all the way down.
func Probe(from Source) probe.Probe {
	return probe.FromError(func(ctx context.Context) error {
		_, err := from.Revision(ctx)

		return err
	})
}

// Server is the service to register, and the only thing this package
// hands to the transport.
func (h *Health) Server() hv1.HealthServer { return h.server }

// Poll keeps the status true until ctx is done.
//
// It blocks, and it is started from the composition root with everything
// else that does — the library also offers a Start that spawns its own
// goroutine, and taking it would put one outside the manager that waits
// for them.
func (h *Health) Poll(ctx context.Context) { h.poll.Run(ctx) }

// reported writes down the moments the answer changed.
//
// Transitions only, which is the whole value of it: "the store stopped
// answering at 14:02 and started again at 14:09" is two lines, and the
// same fact sampled every five seconds is a log nobody reads.
func reported(log *xlog.Logger) reporter.Reporter {
	return reporter.Func(func(_ context.Context, event reporter.Event) {
		log.Info("health",
			xlog.String("probe", event.Name),
			xlog.String("was", event.Prev.String()),
			xlog.String("now", event.Cur.String()))
	})
}
