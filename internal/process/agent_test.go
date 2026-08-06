package process_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/blob"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/process"
	"github.com/graphene-ci/graphene/internal/store/kv/memory"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

const kernelName = "k1"

// A blob id the tests use. Issued rather than written out, because an id
// is issued and a literal would be the one thing the type forbids.
func someBlob(t *testing.T) blob.Id {
	t.Helper()

	id, err := blob.Issue()
	if err != nil {
		t.Fatalf("issue: %v", err)
	}

	return id
}

// runner stands in for the machine: it records what it was asked to
// start and lets the test decide when each process ends.
type runner struct {
	mu      sync.Mutex
	started []process.Spec
	exits   chan int
}

func newRunner() *runner { return &runner{exits: make(chan int, 8)} }

func (r *runner) Start(_ context.Context, spec process.Spec) (process.Started, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.started = append(r.started, spec)

	return &started{exits: r.exits, stopped: make(chan struct{})}, nil
}

func (r *runner) starts() []process.Spec {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]process.Spec(nil), r.started...)
}

type started struct {
	exits   chan int
	stopped chan struct{}
	once    sync.Once
}

func (s *started) Wait() (int, error) {
	select {
	case code := <-s.exits:
		return code, nil
	case <-s.stopped:
		return 0, nil
	}
}

func (s *started) Stop() error {
	s.once.Do(func() { close(s.stopped) })

	return nil
}

type fetcher struct{ err error }

func (f fetcher) Fetch(_ context.Context, id blob.Id) (string, error) {
	if f.err != nil {
		return "", f.err
	}

	return "/somewhere/" + id.String(), nil
}

// gateway records the doors opened and whether they were shut.
type gateway struct {
	mu     sync.Mutex
	opened int
	closed int
}

func (g *gateway) Open(name string) (process.Door, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.opened++

	return &door{gateway: g, name: name}, nil
}

func (g *gateway) counts() (int, int) {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.opened, g.closed
}

type door struct {
	gateway *gateway
	name    string
}

func (d *door) Env() map[string]string {
	return map[string]string{"GRAPHENE_SOCKET": "/doors/" + d.name, "GRAPHENE_PROCESS": d.name}
}

func (d *door) Close() error {
	d.gateway.mu.Lock()
	defer d.gateway.mu.Unlock()

	d.gateway.closed++

	return nil
}

type world struct {
	kernel  kernel.Kernel
	runner  *runner
	gateway *gateway
	ctx     context.Context
	t       *testing.T
}

func newWorld(t *testing.T, fetch process.Fetcher) *world {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	bytes := memory.New()

	t.Cleanup(func() { _ = bytes.Close() })

	k := kernel.New(bytes)
	if _, err := k.Define(ctx, process.Definition()); err != nil {
		t.Fatalf("define: %v", err)
	}

	run := newRunner()

	doors := &gateway{}

	agent := &process.Agent{
		Name:   kernelName,
		Kernel: process.Here(k),
		Fetch:  fetch,
		Runner: run,
		Doors:  doors,
	}

	go func() { _ = agent.Run(ctx) }()

	return &world{kernel: k, runner: run, gateway: doors, ctx: ctx, t: t}
}

// put writes a process record, creating or updating it.
func (w *world) put(name string, fields map[string]any) {
	w.t.Helper()

	id, err := process.Id(kernelName, name)
	if err != nil {
		w.t.Fatalf("id: %v", err)
	}

	spec, err := schemapb.StructFromGo(fields)
	if err != nil {
		w.t.Fatalf("spec: %v", err)
	}

	intent, err := resource.NewIntent(id, spec)
	if err != nil {
		w.t.Fatalf("intent: %v", err)
	}

	expect := revision.Absent
	if current, err := w.kernel.Get(w.ctx, id); err == nil {
		expect = current.Revision
	}

	if _, err := w.kernel.Put(w.ctx, intent, expect); err != nil {
		w.t.Fatalf("put %s: %v", name, err)
	}
}

// waitPhase blocks until the status says what the test expects, so the
// assertions are about the agent's decisions and not about timing.
func (w *world) waitPhase(name, want string) {
	w.t.Helper()

	id, err := process.Id(kernelName, name)
	if err != nil {
		w.t.Fatalf("id: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		stored, err := w.kernel.Get(w.ctx, id)
		if err == nil {
			if phase, ok := schemapb.As[string](stored.Value.Status().GetFields()["phase"]); ok && phase == want {
				return
			}
		}

		time.Sleep(10 * time.Millisecond)
	}

	w.t.Fatalf("process %s never reached %q", name, want)
}

func spec(id blob.Id, extra map[string]any) map[string]any {
	fields := map[string]any{"blob": id.String(), "format": process.RawExec}
	for name, value := range extra {
		fields[name] = value
	}

	return fields
}

// The plain path: a record appears, the bytes are fetched, a door is
// opened, the process starts, and the outcome is written where whoever
// asked can read it.
func TestRunsAndReportsTheOutcome(t *testing.T) {
	t.Parallel()

	world := newWorld(t, fetcher{})
	id := someBlob(t)

	world.put("one", spec(id, map[string]any{
		"args": []any{"--serve"},
		"env":  []any{map[string]any{"name": "MINE", "value": "kept"}},
	}))
	world.waitPhase("one", process.PhaseRunning)

	starts := world.runner.starts()
	if len(starts) != 1 || starts[0].Path != "/somewhere/"+id.String() {
		t.Fatalf("started: %+v", starts)
	}

	if len(starts[0].Args) != 1 || starts[0].Args[0] != "--serve" {
		t.Fatalf("args lost: %+v", starts[0])
	}

	// The record's own environment survives, and the door's is laid over
	// it: where a process talks is not a preference somebody states.
	if starts[0].Env["MINE"] != "kept" || starts[0].Env["GRAPHENE_PROCESS"] != "one" {
		t.Fatalf("environment: %+v", starts[0].Env)
	}

	world.runner.exits <- 0

	world.waitPhase("one", process.PhaseExited)
}

// A non-zero exit is a failure, not an exit: the difference is the whole
// reason anybody reads the status.
func TestNonZeroExitIsFailure(t *testing.T) {
	t.Parallel()

	world := newWorld(t, fetcher{})
	world.put("one", spec(someBlob(t), nil))
	world.waitPhase("one", process.PhaseRunning)

	world.runner.exits <- 3

	world.waitPhase("one", process.PhaseFailed)
}

// restart: always means an exit is a fault to recover from, and the
// counter makes a crash loop visible in a listing.
func TestRestartAlwaysStartsItAgain(t *testing.T) {
	t.Parallel()

	world := newWorld(t, fetcher{})
	world.put("driver", spec(someBlob(t), map[string]any{"restart": process.RestartAlways}))
	world.waitPhase("driver", process.PhaseRunning)

	world.runner.exits <- 1

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if len(world.runner.starts()) > 1 {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("never restarted: %d starts", len(world.runner.starts()))
}

// A run asked for once stays ended. Restarting it would be the agent
// inventing intent nobody expressed.
func TestRestartNeverStaysEnded(t *testing.T) {
	t.Parallel()

	world := newWorld(t, fetcher{})
	world.put("once", spec(someBlob(t), map[string]any{"restart": process.RestartNever}))
	world.waitPhase("once", process.PhaseRunning)

	world.runner.exits <- 0

	world.waitPhase("once", process.PhaseExited)

	time.Sleep(2 * restartDelayForTest)

	if got := len(world.runner.starts()); got != 1 {
		t.Fatalf("a one-shot process was started %d times", got)
	}
}

// Bytes that cannot be fetched are a failure of the process, not of the
// agent: the loop keeps running and the reason is written down.
func TestUnfetchableBytesFailTheProcess(t *testing.T) {
	t.Parallel()

	world := newWorld(t, fetcher{err: errors.New("no such blob")})
	world.put("one", spec(someBlob(t), nil))
	world.waitPhase("one", process.PhaseFailed)

	// And no door was left open for a process that never started.
	opened, closed := world.gateway.counts()
	if opened != closed {
		t.Fatalf("%d doors opened, %d closed", opened, closed)
	}
}

// The door is shut when the process ends. One that outlived its process
// would be a way in for whatever came next.
func TestDoorIsShutWhenTheProcessEnds(t *testing.T) {
	t.Parallel()

	world := newWorld(t, fetcher{})
	world.put("one", spec(someBlob(t), nil))
	world.waitPhase("one", process.PhaseRunning)

	if opened, closed := world.gateway.counts(); opened != 1 || closed != 0 {
		t.Fatalf("while running: %d opened, %d closed", opened, closed)
	}

	world.runner.exits <- 0

	world.waitPhase("one", process.PhaseExited)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, closed := world.gateway.counts(); closed == 1 {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("the door outlived the process")
}

// restartDelayForTest mirrors the agent's floor between restarts.
const restartDelayForTest = time.Second
