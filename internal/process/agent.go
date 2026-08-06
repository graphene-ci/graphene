package process

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"

	"github.com/gopherex/schemapb/go/schemapb"
	"github.com/gopherex/xlog"

	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Kernel is what an agent needs of a kernel, and no more.
//
// Narrow on purpose: an agent reads the processes placed on it and says
// what became of them. It does not Put — the spec belongs to whoever
// asked for the process, and an agent that could rewrite it would be
// arguing with its own instructions — and it does not Define, Delete or
// Claim. Anything it cannot name here, it cannot do.
//
// The kernel it holds may be this process's own or one a link away. Both
// satisfy this, which is the whole reason an agent runs on a machine that
// keeps nothing.
type Kernel interface {
	// Revision is the cursor to take before a snapshot.
	Revision(ctx context.Context) (revision.Revision, error)

	// List walks what is there now.
	List(ctx context.Context, prefix resource.Id) iter.Seq2[store.Value[resource.Resource], error]

	// Watch follows what happens after a revision. It delivers no
	// snapshot of its own: the caller takes one with List at a revision
	// it read first, which is what makes the two together gapless.
	Watch(ctx context.Context, prefix resource.Id, after revision.Revision) (Stream, error)

	// Get reads one back, which reporting needs: every write is guarded,
	// so saying what happened means knowing what is there.
	Get(ctx context.Context, id resource.Id) (store.Value[resource.Resource], error)

	// Report writes the status half. A separate verb from Put because it
	// is a different party writing a different part.
	Report(ctx context.Context, id resource.Id, status *schemapb.StructValue,
		expect revision.Revision) (revision.Revision, error)
}

// Stream is a watch, pulled. The store's own stream satisfies it, and so
// does one that reaches across a link.
type Stream interface {
	Next(ctx context.Context) (store.Event[resource.Resource], error)
	Close() error
}

// Agent runs the processes placed on one kernel.
//
// It is a controller and nothing more privileged: it watches a prefix and
// reports what it finds, through the same API anybody else would use.
// What makes it the kernel's own is only that it is the one thing able to
// turn bytes into a process on this machine.
type Agent struct {
	// Name is this kernel's name — the first path segment of every
	// process it is responsible for, and nothing else is watched.
	Name   string
	Kernel Kernel
	Fetch  Fetcher
	Runner Runner
	Doors  Gateway
	Log    *xlog.Logger

	mu      sync.Mutex
	running map[string]*supervisor
}

// Run follows this kernel's processes until ctx is done.
//
// A snapshot first, then the stream from the revision read BEFORE it.
// Taking the cursor first is what makes the pair gapless: anything
// written between the read and the walk is delivered by the watch, and
// anything the walk already saw arrives again, which is harmless because
// starting a process that is running is a comparison of generations.
func (a *Agent) Run(ctx context.Context) error {
	a.running = map[string]*supervisor{}
	defer a.stopAll()

	prefix, err := On(a.Name)
	if err != nil {
		return fmt.Errorf("process: agent: %w", err)
	}

	for {
		if err := a.follow(ctx, prefix); err != nil {
			if ctx.Err() != nil {
				return nil
			}

			// A watch that ended is not a reason to stop running
			// processes. It is reported and taken again from a fresh
			// snapshot, which is the only answer to a cursor that can no
			// longer be served.
			a.report("watch ended", err)
		}

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

// follow takes one snapshot and consumes one stream to its end.
func (a *Agent) follow(ctx context.Context, prefix resource.Id) error {
	cursor, err := a.Kernel.Revision(ctx)
	if err != nil {
		return fmt.Errorf("read revision: %w", err)
	}

	for stored, err := range a.Kernel.List(ctx, prefix) {
		if err != nil {
			return fmt.Errorf("snapshot: %w", err)
		}

		a.ensure(ctx, stored.Value)
	}

	stream, err := a.Kernel.Watch(ctx, prefix, cursor)
	if err != nil {
		return fmt.Errorf("watch: %w", err)
	}

	defer func() { _ = stream.Close() }()

	for {
		event, err := stream.Next(ctx)
		if err != nil {
			return fmt.Errorf("next: %w", err)
		}

		a.handle(ctx, event)
	}
}

func (a *Agent) handle(ctx context.Context, event store.Event[resource.Resource]) {
	found := event.Value.Value

	// A record on its way out stops its process at the MARK rather than
	// at the end of finalization, which is what makes deleting one the
	// way to stop it.
	if event.Kind == store.EventDelete || found.IsDeleting() {
		a.stop(name(found))

		return
	}

	a.ensure(ctx, found)
}

// ensure makes what is running match what is written. A supervisor
// already running the same intent is left alone; a changed spec is a
// restart, because a process cannot be edited in place.
func (a *Agent) ensure(ctx context.Context, found resource.Resource) {
	wanted, err := specOf(found)
	if err != nil {
		a.report("unusable process record", err)

		return
	}

	called := name(found)

	a.mu.Lock()
	defer a.mu.Unlock()

	if current, running := a.running[called]; running {
		if current.generation == found.Generation() {
			return
		}

		current.stop()
		delete(a.running, called)
	}

	next := &supervisor{
		agent:      a,
		id:         found.Id(),
		name:       called,
		generation: found.Generation(),
		spec:       wanted,
		done:       make(chan struct{}),
	}
	a.running[called] = next

	go next.run(ctx)
}

func (a *Agent) stop(at string) {
	a.mu.Lock()
	current, running := a.running[at]
	delete(a.running, at)
	a.mu.Unlock()

	if running {
		current.stop()
	}
}

func (a *Agent) stopAll() {
	a.mu.Lock()
	all := a.running
	a.running = map[string]*supervisor{}
	a.mu.Unlock()

	for _, current := range all {
		current.stop()
	}
}

// setStatus records what became of a process.
//
// Only the status: the spec belongs to whoever asked for the process. The
// kernel's Report is a different verb from Put for exactly this reason,
// so the narrowness is in the permission rather than in this agent's good
// behavior.
func (a *Agent) setStatus(ctx context.Context, id resource.Id, fields map[string]any) {
	status, err := schemapb.StructFromGo(fields)
	if err != nil {
		a.report("build status", err)

		return
	}

	// Read, report, and on a lost race read again. Every write here is
	// guarded — there is no unconditional one and should not be — so the
	// answer to somebody else having written is to look at what they
	// wrote, not to force ours over it.
	for range reportAttempts {
		current, err := a.Kernel.Get(ctx, id)
		if err != nil {
			if ctx.Err() == nil {
				a.report("read process before reporting", err)
			}

			return
		}

		_, err = a.Kernel.Report(ctx, id, status, current.Revision)

		switch {
		case err == nil:
			return
		case errors.Is(err, revision.ErrConflict):
			continue
		default:
			if ctx.Err() == nil {
				a.report("report process", err)
			}

			return
		}
	}
}

// reportAttempts bounds the read-report loop. A status write that keeps
// losing is a status somebody else is writing more often than we are, and
// the answer to that is a log line rather than a spin.
const reportAttempts = 5

func (a *Agent) report(message string, err error) {
	if a.Log == nil {
		return
	}

	a.Log.Warn(message, xlog.String("kernel", a.Name), xlog.Err(err))
}

// name is a process's own name — the second segment of its path.
func name(found resource.Resource) string {
	values := found.Id().Path().Values()
	if len(values) < 2 { //nolint:mnd // a process is /{kernel}/{name}
		return ""
	}

	return values[1]
}

var errNoSpec = errors.New("process: the record has no spec")
