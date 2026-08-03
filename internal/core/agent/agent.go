// Package agent runs the processes placed on one kernel.
//
// It is the only privileged thing about a kernel's execution surface, and
// it is deliberately small: watch the Process records under this kernel's
// name, make reality match them, write down what happened. Everything
// people expect around it — scheduling, packaging, retry policy,
// pipelines — is somebody else's controller, built on the same API.
//
// The agent is itself written as an ordinary controller (core/controller),
// so it runs the same whether its kernel holds the truth or reaches it
// over a link. That is not a convenience: a worker kernel has no store, so
// anything that could only run beside one would be useless here.
package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/builtin"
	"github.com/graphene-ci/graphene/internal/core/controller"
	"github.com/graphene-ci/graphene/internal/core/key"
	"github.com/graphene-ci/graphene/internal/core/store"
)

// Phases of a Process, as the status records them.
const (
	PhasePending = "pending"
	PhaseRunning = "running"
	PhaseExited  = "exited"
	PhaseFailed  = "failed"
)

// restartDelay keeps a crash loop from becoming a busy loop. Backing off
// further with each failure is a policy, and policy lives in controllers
// above — this is only the floor that keeps a broken binary from eating
// the machine.
const restartDelay = time.Second

// Fetcher turns a blob id into a local path. Caching, integrity checking
// and where the bytes come from are its business, not the agent's.
type Fetcher interface {
	Fetch(ctx context.Context, blobID string) (string, error)
}

// Spec is one process to start.
type Spec struct {
	Path string
	Args []string
	Env  map[string]string
	// Process is the resource's own name — what the kernel vouches for
	// when this process calls back.
	Process string
}

// Started is a process that is running now.
type Started interface {
	// Wait blocks until it ends and reports its exit code.
	Wait() (int, error)
	// Stop asks it to end and waits for it to.
	Stop() error
}

// Runner starts prepared bytes. raw-exec is the only implementation the
// kernel ships; anything heavier belongs behind the same interface.
type Runner interface {
	Start(ctx context.Context, spec Spec) (Started, error)
}

// Agent reconciles the Process records under one kernel's name.
type Agent struct {
	// Kernel is this kernel's name — the first path segment of every
	// Process it is responsible for, and nothing else is watched.
	Kernel  string
	Stream  controller.Stream
	Writer  controller.Writer
	Fetch   Fetcher
	Runner  Runner
	Gateway Gateway
	Log     *slog.Logger

	mu      sync.Mutex
	running map[string]*supervisor
}

// Run follows this kernel's processes until ctx is done.
func (a *Agent) Run(ctx context.Context) error {
	a.running = map[string]*supervisor{}

	loop := &controller.Loop{
		Stream:  a.Stream,
		Handle:  a.handle,
		OnError: func(err error) { a.logf("process event", err) },
	}

	defer a.stopAll()

	if err := loop.Run(ctx); err != nil {
		return fmt.Errorf("agent: %w", err)
	}

	return nil
}

func (a *Agent) handle(ctx context.Context, event controller.Event) error {
	res := event.Resource

	path := res.GetKey().GetPath()
	if len(path) != processPathSegments {
		return nil
	}

	name := path[1]

	// A deleting record is on its way out: stopping at the mark rather
	// than at the end of finalization is what makes deletion the way to
	// stop something.
	if event.Type == store.EventDelete || res.GetDeleting() {
		a.stop(name)

		return nil
	}

	a.ensure(ctx, name, res)

	return nil
}

// ensure makes the running reality match the record. A supervisor already
// running the same intent is left alone; a changed spec is a restart,
// because a process cannot be edited in place.
func (a *Agent) ensure(ctx context.Context, name string, res *graphenepbv1.Resource) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if current, ok := a.running[name]; ok {
		if current.generation == res.GetGeneration() {
			return
		}

		current.stop()
		delete(a.running, name)
	}

	sup := &supervisor{
		agent:      a,
		name:       name,
		generation: res.GetGeneration(),
		spec:       processSpec(res),
		done:       make(chan struct{}),
	}
	a.running[name] = sup

	go sup.run(ctx)
}

func (a *Agent) stop(name string) {
	a.mu.Lock()
	sup, ok := a.running[name]
	delete(a.running, name)
	a.mu.Unlock()

	if ok {
		sup.stop()
	}
}

func (a *Agent) stopAll() {
	a.mu.Lock()
	all := a.running
	a.running = map[string]*supervisor{}
	a.mu.Unlock()

	for _, sup := range all {
		sup.stop()
	}
}

// setStatus records what became of a process. Only the status is touched:
// the spec belongs to whoever asked for the process, and an agent that
// rewrote it would be arguing with its own instructions.
func (a *Agent) setStatus(ctx context.Context, name string, mutate func(fields map[string]any)) {
	err := controller.Update(ctx, a.Writer, key.New(builtin.KindProcess, a.Kernel, name),
		func(res *graphenepbv1.Resource) bool {
			fields := res.GetStatus().ToGo()
			if fields == nil {
				fields = map[string]any{}
			}

			mutate(fields)
			res.Status = schemapb.MustStructFromGo(fields)

			return true
		})
	if err != nil {
		a.logf("write process status", err)
	}
}

func (a *Agent) logf(message string, err error) {
	if a.Log == nil {
		return
	}

	a.Log.Warn(message, "kernel", a.Kernel, "error", err)
}

// processPathSegments — a Process lives at {kernel, name}.
const processPathSegments = 2

func processSpec(res *graphenepbv1.Resource) spec {
	fields := res.GetSpec().ToGo()

	out := spec{env: map[string]string{}}
	out.blob, _ = fields["blob"].(string)
	out.identity, _ = fields["identity"].(string)
	out.restart, _ = fields["restart"].(string)

	if args, listed := fields["args"].([]any); listed {
		for _, arg := range args {
			if text, isText := arg.(string); isText {
				out.args = append(out.args, text)
			}
		}
	}

	if env, listed := fields["env"].([]any); listed {
		for _, item := range env {
			pair, isPair := item.(map[string]any)
			if !isPair {
				continue
			}

			name, named := pair["name"].(string)
			value, valued := pair["value"].(string)

			if named && valued {
				out.env[name] = value
			}
		}
	}

	return out
}

type spec struct {
	blob     string
	args     []string
	env      map[string]string
	identity string
	restart  string
}

// EnvSocket and EnvProcess are the whole of what a process is told about
// the system it is in: where to talk, and what it is called. No token,
// because there is none — the kernel vouches for it on that socket.
//
// They live here because they are a contract between the side that starts
// a process and any client the process happens to run, and a contract
// written down twice is a contract that drifts.
const (
	EnvSocket  = "GRAPHENE_SOCKET"
	EnvProcess = "GRAPHENE_PROCESS"
)

// Gateway gives a process its way back into the system: a door opened
// before it starts and taken away when it ends.
//
// A process holds no credentials — the door is the credential. That is
// why it is opened per process and why it is closed the moment the
// process is done with: a door outliving its process would be a way in
// for whatever came next.
type Gateway interface {
	Open(process string) (Opened, error)
}

// Opened is one process's door.
type Opened interface {
	// Env is what the process is told about where it can talk.
	Env() map[string]string
	// Close stops answering and takes the door away.
	Close() error
}
