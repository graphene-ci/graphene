package agent_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/agent"
	"github.com/graphene-ci/graphene/internal/core/auth"
	"github.com/graphene-ci/graphene/internal/core/builtin"
	"github.com/graphene-ci/graphene/internal/core/controller"
	"github.com/graphene-ci/graphene/internal/core/key"
	"github.com/graphene-ci/graphene/internal/core/registry"
	"github.com/graphene-ci/graphene/internal/core/service"
	"github.com/graphene-ci/graphene/internal/infrastructure/store/bbolt"
)

const kernelName = "k1"

// fakeRunner stands in for the machine: it records what it was asked to
// start and lets the test decide when each process ends.
type fakeRunner struct {
	mu      sync.Mutex
	started []agent.Spec
	exits   chan int
	fail    error
}

func newRunner() *fakeRunner {
	return &fakeRunner{exits: make(chan int, 8)}
}

func (r *fakeRunner) Start(_ context.Context, spec agent.Spec) (agent.Started, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.fail != nil {
		return nil, r.fail
	}

	r.started = append(r.started, spec)

	return &fakeStarted{exits: r.exits, stopped: make(chan struct{})}, nil
}

func (r *fakeRunner) starts() []agent.Spec {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]agent.Spec(nil), r.started...)
}

type fakeStarted struct {
	exits   chan int
	stopped chan struct{}
	once    sync.Once
}

func (s *fakeStarted) Wait() (int, error) {
	select {
	case code := <-s.exits:
		return code, nil
	case <-s.stopped:
		return 0, nil
	}
}

func (s *fakeStarted) Stop() error {
	s.once.Do(func() { close(s.stopped) })

	return nil
}

// fakeGateway records the doors opened and whether they were shut.
type fakeGateway struct {
	mu     sync.Mutex
	opened []string
	closed int
}

func (g *fakeGateway) Open(process string) (agent.Opened, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.opened = append(g.opened, process)

	return &fakeDoor{gateway: g, process: process}, nil
}

func (g *fakeGateway) counts() (int, int) {
	g.mu.Lock()
	defer g.mu.Unlock()

	return len(g.opened), g.closed
}

type fakeDoor struct {
	gateway *fakeGateway
	process string
}

func (d *fakeDoor) Env() map[string]string {
	return map[string]string{"GRAPHENE_SOCKET": "/sock/" + d.process, "GRAPHENE_PROCESS": d.process}
}

func (d *fakeDoor) Close() error {
	d.gateway.mu.Lock()
	defer d.gateway.mu.Unlock()

	d.gateway.closed++

	return nil
}

type fakeFetcher struct{ err error }

func (f fakeFetcher) Fetch(_ context.Context, blobID string) (string, error) {
	if f.err != nil {
		return "", f.err
	}

	return "/tmp/" + blobID, nil
}

type env struct {
	resources *service.Resources
	writer    controller.Writer
	runner    *fakeRunner
	gateway   *fakeGateway
	ctx       context.Context
	t         *testing.T
}

func newEnv(t *testing.T, fetcher agent.Fetcher) *env {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	st, err := bbolt.Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	t.Cleanup(func() { _ = st.Close() })

	system := auth.WithCredentials(ctx, auth.FullAccess(auth.PrincipalSystem, "test"))

	reg := registry.New(st)
	if err := builtin.Ensure(system, reg); err != nil {
		t.Fatalf("ensure builtins: %v", err)
	}

	resources := service.NewResources(st, reg)
	runner := newRunner()
	gateway := &fakeGateway{}

	worker := &agent.Agent{
		Kernel:  kernelName,
		Stream:  controller.Local(st, builtin.KindProcess, kernelName),
		Writer:  controller.OverService(resources),
		Fetch:   fetcher,
		Runner:  runner,
		Gateway: gateway,
	}

	go func() { _ = worker.Run(system) }()

	return &env{
		resources: resources,
		writer:    controller.OverService(resources),
		runner:    runner,
		gateway:   gateway,
		ctx:       system,
		t:         t,
	}
}

func (e *env) put(name string, fields map[string]any) {
	e.t.Helper()

	current, err := e.writer.Get(e.ctx, key.New(builtin.KindProcess, kernelName, name))

	var expected uint64
	if err == nil {
		expected = current.GetRevision()
	} else if !errors.Is(err, controller.ErrAbsent) {
		e.t.Fatalf("read process: %v", err)
	}

	if _, err := e.resources.Put(e.ctx, &graphenepbv1.PutRequest{
		Resource: &graphenepbv1.Resource{
			Key:  &graphenepbv1.Key{Kind: builtin.KindProcess, Path: []string{kernelName, name}},
			Spec: schemapb.MustStructFromGo(fields),
		},
		ExpectedRevision: expected,
	}); err != nil {
		e.t.Fatalf("put process: %v", err)
	}
}

// waitPhase blocks until the status says what the test expects, so the
// assertions are about the agent's decisions and not about timing.
func (e *env) waitPhase(name, want string) {
	e.t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		res, err := e.writer.Get(e.ctx, key.New(builtin.KindProcess, kernelName, name))
		if err == nil {
			if phase, _ := res.GetStatus().ToGo()["phase"].(string); phase == want {
				return
			}
		}

		time.Sleep(10 * time.Millisecond)
	}

	e.t.Fatalf("process %s never reached phase %q", name, want)
}

func runSpec(extra map[string]any) map[string]any {
	fields := map[string]any{"blob": "b1", "format": "raw-exec"}
	for name, value := range extra {
		fields[name] = value
	}

	return fields
}

// The plain path: a record appears, the bytes are fetched and started, and
// the outcome is written where whoever asked can read it.
func TestRunsAndReportsTheOutcome(t *testing.T) {
	t.Parallel()

	e := newEnv(t, fakeFetcher{})

	e.put("one", runSpec(map[string]any{"args": []any{"--serve"}}))
	e.waitPhase("one", agent.PhaseRunning)

	starts := e.runner.starts()
	if len(starts) != 1 || starts[0].Path != "/tmp/b1" {
		t.Fatalf("started: %+v", starts)
	}

	if len(starts[0].Args) != 1 || starts[0].Args[0] != "--serve" {
		t.Fatalf("args lost: %+v", starts[0])
	}

	// The process is told which record it is, because that is what its
	// kernel will vouch for when it calls back.
	if starts[0].Process != "one" {
		t.Fatalf("process name not passed: %+v", starts[0])
	}

	e.runner.exits <- 0

	e.waitPhase("one", agent.PhaseExited)
}

// A non-zero exit is a failure, not an exit: the difference is the whole
// reason anyone reads the status.
func TestNonZeroExitIsFailure(t *testing.T) {
	t.Parallel()

	e := newEnv(t, fakeFetcher{})

	e.put("one", runSpec(nil))
	e.waitPhase("one", agent.PhaseRunning)

	e.runner.exits <- 3

	e.waitPhase("one", agent.PhaseFailed)

	res, err := e.writer.Get(e.ctx, key.New(builtin.KindProcess, kernelName, "one"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if code, _ := res.GetStatus().ToGo()["exit_code"].(int64); code != 3 {
		t.Fatalf("exit code: %v", res.GetStatus().ToGo())
	}
}

// restart: always means an exit is a fault to recover from, so the agent
// starts it again — and the counter makes a crash loop visible instead of
// leaving it to a log nobody reads.
func TestRestartAlwaysStartsItAgain(t *testing.T) {
	t.Parallel()

	e := newEnv(t, fakeFetcher{})

	e.put("driver", runSpec(map[string]any{"restart": "always"}))
	e.waitPhase("driver", agent.PhaseRunning)

	e.runner.exits <- 1

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if len(e.runner.starts()) > 1 {
			res, err := e.writer.Get(e.ctx, key.New(builtin.KindProcess, kernelName, "driver"))
			if err == nil {
				if starts, _ := res.GetStatus().ToGo()["starts"].(int64); starts > 1 {
					return
				}
			}
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("never restarted: %d starts", len(e.runner.starts()))
}

// A run that was asked for once stays ended. Restarting it would be the
// agent inventing intent nobody expressed.
func TestRestartNeverStaysEnded(t *testing.T) {
	t.Parallel()

	e := newEnv(t, fakeFetcher{})

	e.put("once", runSpec(map[string]any{"restart": "never"}))
	e.waitPhase("once", agent.PhaseRunning)

	e.runner.exits <- 0

	e.waitPhase("once", agent.PhaseExited)

	time.Sleep(100 * time.Millisecond)

	if got := len(e.runner.starts()); got != 1 {
		t.Fatalf("a one-shot process was started %d times", got)
	}
}

// Bytes that cannot be fetched are a failure of the process, not of the
// agent: the loop keeps running and the reason is written down.
func TestUnfetchableBytesFailTheProcess(t *testing.T) {
	t.Parallel()

	e := newEnv(t, fakeFetcher{err: errors.New("no such blob")})

	e.put("one", runSpec(nil))
	e.waitPhase("one", agent.PhaseFailed)

	res, err := e.writer.Get(e.ctx, key.New(builtin.KindProcess, kernelName, "one"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if text, _ := res.GetStatus().ToGo()["error"].(string); text == "" {
		t.Fatalf("no reason recorded: %v", res.GetStatus().ToGo())
	}
}

// Deleting the record is how a process is stopped, and it has to bite at
// the mark rather than at the end of finalization.
func TestDeletingTheRecordStopsIt(t *testing.T) {
	t.Parallel()

	e := newEnv(t, fakeFetcher{})

	e.put("one", runSpec(map[string]any{"restart": "always"}))
	e.waitPhase("one", agent.PhaseRunning)

	current, err := e.writer.Get(e.ctx, key.New(builtin.KindProcess, kernelName, "one"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if _, err := e.resources.Delete(e.ctx, &graphenepbv1.DeleteRequest{
		Key:              current.GetKey(),
		ExpectedRevision: current.GetRevision(),
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	before := len(e.runner.starts())

	time.Sleep(2 * time.Second) // longer than the restart delay

	if got := len(e.runner.starts()); got != before {
		t.Fatalf("a deleted process was started again: %d → %d", before, got)
	}
}

// A process is told where to talk and what it is called, and nothing
// else: it has no token because there is none to have.
func TestProcessIsToldItsDoor(t *testing.T) {
	t.Parallel()

	e := newEnv(t, fakeFetcher{})

	e.put("one", runSpec(map[string]any{
		"env": []any{map[string]any{"name": "MINE", "value": "kept"}},
	}))
	e.waitPhase("one", agent.PhaseRunning)

	started := e.runner.starts()[0]
	if started.Env["GRAPHENE_SOCKET"] != "/sock/one" || started.Env["GRAPHENE_PROCESS"] != "one" {
		t.Fatalf("door not passed: %v", started.Env)
	}

	// The record's own variables survive alongside.
	if started.Env["MINE"] != "kept" {
		t.Fatalf("the record's environment was lost: %v", started.Env)
	}

	if len(started.Env) != 3 {
		t.Fatalf("something else was handed over: %v", started.Env)
	}
}

// The door is shut when the process ends. One that outlived its process
// would be a way in for whatever came next.
func TestDoorIsShutWhenTheProcessEnds(t *testing.T) {
	t.Parallel()

	e := newEnv(t, fakeFetcher{})

	e.put("one", runSpec(nil))
	e.waitPhase("one", agent.PhaseRunning)

	opened, closed := e.gateway.counts()
	if opened != 1 || closed != 0 {
		t.Fatalf("while running: %d opened, %d closed", opened, closed)
	}

	e.runner.exits <- 0

	e.waitPhase("one", agent.PhaseExited)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, shut := e.gateway.counts(); shut == 1 {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("the door outlived the process")
}
