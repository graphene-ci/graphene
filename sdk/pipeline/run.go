// Package pipeline is the SDK a pipeline is written against. It is the one
// package of ours the user imports, so it stays thin on purpose: a test in
// this directory refuses any module the user did not ask for.
//
// What a pipeline looks like:
//
//	func main() { pipeline.Serve(Perf) }
//
//	func Perf(run pipeline.Run, p Params) error {
//	    defer run.Teardown()
//	    vm := pipeline.Apply(run, "node-0", &yandexv1.Instance{...})
//	    ready := pipeline.Await(run, vm)
//	    ...
//	}
package pipeline

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/graphene-ci/graphene/sdk/agent"
)

// Run is the handle a pipeline holds on its own execution. It is passed by
// value and is safe to copy: what changes lives behind the pointer inside.
type Run struct {
	s *state
}

type state struct {
	ctx    workflow.Context
	owner  agent.OwnerRef
	ready  workflow.ReceiveChannel
	scheme *runtime.Scheme

	// arrived holds readiness that showed up before anyone asked for it.
	// Signals do not queue per record, they queue per workflow, so a
	// pipeline that applies three machines and then awaits the first one
	// must not drop the other two on the floor.
	arrived map[string]agent.ReadySignal

	// created is everything the run has asked for, in order. Teardown
	// hands it back rather than searching the cluster: the run knows what
	// it made, and we promised not to know every kind it might have used.
	created []agent.ObjectRef

	keep       time.Duration
	keepReason string
	torn       bool

	// unwinding says a failure is propagating right now, and wanted says
	// teardown was asked for while it was.
	//
	// Temporal runs workflow code as a coroutine that yields whenever it
	// waits, and it REFUSES to yield while a panic is unwinding. Since a
	// step failure travels by panic and teardown waits on an activity,
	// `defer run.Teardown()` — the shape the whole design is written
	// around — would kill the workflow with "yield during panic
	// unwinding" instead of cleaning up.
	//
	// So teardown asked for during unwinding is not performed there. It
	// is remembered, and Serve performs it after recovering, when the
	// coroutine may block again.
	unwinding bool
	wanted    bool
}

// StepError is what a step raises when it cannot go on. It travels by panic
// and is caught in exactly one place — the adapter in Serve — so that a
// pipeline reads as a program and not as a chain of error checks.
type StepError struct {
	Op  string
	Err error
}

func (f *StepError) Error() string { return f.Op + ": " + f.Err.Error() }

func (f *StepError) Unwrap() error { return f.Err }

// ErrNoKind means an object did not say what kind it is and no scheme
// could tell us.
var ErrNoKind = errors.New("не удалось определить вид объекта")

// fail stops the pipeline. Inside a workflow a refusal is not a value to
// carry around: it is the end of the program, and the recovery sits where
// teardown does.
func fail(op string, err error) {
	panic(&StepError{Op: op, Err: err})
}

// raise stops the pipeline, remembering that the failure is in flight.
func (r Run) raise(op string, err error) {
	r.s.unwinding = true

	fail(op, err)
}

// Unwind performs a teardown that was asked for while a failure was
// propagating. Serve calls it after recovering; nothing else should.
func (r Run) Unwind() {
	if !r.s.wanted {
		return
	}

	r.s.unwinding = false
	r.s.wanted = false

	r.Teardown()
}

// Owner reports the run that owns everything this pipeline creates.
func (r Run) Owner() agent.OwnerRef { return r.s.owner }

// Sleep waits, durably. A pipeline that sleeps for a day costs nothing
// while it sleeps and survives every restart underneath it.
func (r Run) Sleep(d time.Duration) {
	err := workflow.Sleep(r.s.ctx, d)
	if err == nil {
		return
	}

	// Отмена — не отказ шага. Пайплайн, которому сказали остановиться,
	// остановился; если завернуть это в свою ошибку, прогон запишется
	// упавшим, и человек будет искать поломку там, где её нет.
	if temporal.IsCanceledError(err) {
		r.s.unwinding = true

		panic(err)
	}

	r.raise("sleep", err)
}

// Keep hands what the run made to a stand that outlives it by d.
//
// Not a delayed teardown. A delay still ends in a teardown, and the person
// who comes in the morning to look at the machine that failed the test
// finds nothing. Keeping means the machines outlive the run — which means
// somebody else answers for them, and that somebody is a record with an
// end of its own.
//
// The end is not optional: ownership without one is how a cloud account
// fills with things nobody remembers creating.
func (r Run) Keep(d time.Duration, reason ...string) {
	if d > r.s.keep {
		r.s.keep = d
	}

	if len(reason) > 0 && reason[0] != "" {
		r.s.keepReason = reason[0]
	}
}

// Teardown removes everything the run still owns. It is safe to call more
// than once and safe to call from a defer: it runs on a context detached
// from cancellation, because the case it exists for is exactly the one
// where the ordinary context is already dead.
func (r Run) Teardown() {
	if r.s.torn {
		return
	}

	// Отказ ещё разматывается — блокироваться нельзя. Запомним; Serve
	// снесёт, когда паника поймана и корутине снова можно ждать.
	if r.s.unwinding {
		r.s.wanted = true

		return
	}

	r.s.torn = true

	ctx, cancel := workflow.NewDisconnectedContext(r.s.ctx)
	defer cancel()

	if r.s.keep > 0 {
		r.handOver(ctx)

		return
	}

	in := agent.TeardownInput{Owner: r.s.owner, Refs: r.s.created}

	var out agent.TeardownOutput
	if err := workflow.ExecuteActivity(ctx, agent.ActivityTeardown, in).Get(ctx, &out); err != nil {
		workflow.GetLogger(ctx).Error("снос не прошёл", "ошибка", err)
	}
}

// handOver gives what the run made to a stand.
//
// The stand's end is computed from the workflow's own clock, not from the
// wall: a workflow replayed tomorrow must arrive at the same moment it did
// today, or the same run would keep the stand for a different length of
// time every time it recovered.
func (r Run) handOver(ctx workflow.Context) {
	in := agent.KeepInput{
		Owner:  r.s.owner,
		Until:  workflow.Now(ctx).Add(r.s.keep),
		Reason: r.s.keepReason,
		Refs:   r.s.created,
	}

	var out agent.KeepOutput
	if err := workflow.ExecuteActivity(ctx, agent.ActivityKeep, in).Get(ctx, &out); err != nil {
		workflow.GetLogger(ctx).Error("стенд не оставлен", "ошибка", err)
	}
}

// gvkOf works out what kind an object is.
//
// An object built as &yandexv1.Instance{Spec: ...} carries no apiVersion
// and no kind: generated types leave TypeMeta empty and the machinery
// fills it from a scheme. We have no scheme of the provider's types unless
// somebody hands us one, and no global registry of them exists to consult.
// So: the object's own TypeMeta if it is set, then the scheme given to
// Serve, and otherwise a refusal that names the line to add.
func (r Run) gvkOf(obj runtime.Object) (string, string, error) {
	if gvk := obj.GetObjectKind().GroupVersionKind(); !gvk.Empty() {
		return gvk.GroupVersion().String(), gvk.Kind, nil
	}

	if r.s.scheme != nil {
		kinds, _, err := r.s.scheme.ObjectKinds(obj)
		if err == nil && len(kinds) > 0 {
			return kinds[0].GroupVersion().String(), kinds[0].Kind, nil
		}
	}

	return "", "", fmt.Errorf("%w %T: добавь pipeline.Scheme(<пакет>.AddToScheme) в Serve", ErrNoKind, obj)
}
