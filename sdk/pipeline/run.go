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

	keep time.Duration
	torn bool
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

// Owner reports the run that owns everything this pipeline creates.
func (r Run) Owner() agent.OwnerRef { return r.s.owner }

// Sleep waits, durably. A pipeline that sleeps for a day costs nothing
// while it sleeps and survives every restart underneath it.
func (r Run) Sleep(d time.Duration) {
	if err := workflow.Sleep(r.s.ctx, d); err != nil {
		fail("sleep", err)
	}
}

// Keep delays teardown by d. What the run owns stays that long after the
// pipeline finishes — long enough for a person to log in and look at the
// machine that failed the test.
//
// It delays teardown; it does not cancel it. Ownership without an end is
// how a cloud account fills up with things nobody remembers creating.
func (r Run) Keep(d time.Duration) {
	if d > r.s.keep {
		r.s.keep = d
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

	r.s.torn = true

	ctx, cancel := workflow.NewDisconnectedContext(r.s.ctx)
	defer cancel()

	if r.s.keep > 0 {
		if err := workflow.Sleep(ctx, r.s.keep); err != nil {
			return
		}
	}

	in := agent.TeardownInput{Owner: r.s.owner, Refs: r.s.created}

	var out agent.TeardownOutput
	if err := workflow.ExecuteActivity(ctx, agent.ActivityTeardown, in).Get(ctx, &out); err != nil {
		workflow.GetLogger(ctx).Error("снос не прошёл", "ошибка", err)
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
