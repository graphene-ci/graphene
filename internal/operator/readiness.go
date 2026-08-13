package operator

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/graphene-ci/graphene/api/v1"
	"github.com/graphene-ci/graphene/internal/worker"
	"github.com/graphene-ci/graphene/pkg/agent"
)

// Signaller wakes a workflow that is waiting for a record.
type Signaller interface {
	Signal(ctx context.Context, workflowID, name string, payload agent.ReadySignal) error
}

// ReadinessReconciler watches the records runs create and tells the waiting
// workflow when one becomes ready.
//
// This is the other half of Await. The workflow does not poll and holds no
// worker slot while a machine boots: it sleeps in its history until this
// sends the signal.
//
// Scope, stated plainly: it watches ONE kind, and in M1 that kind is Probe.
// Watching whatever a pipeline happens to apply means dynamic informers over
// a set of kinds discovered at runtime, and the set has to come from
// somewhere — the run would have to record which kinds it used. That is real
// work with real failure modes, and M3 is where the first foreign kind
// actually appears. Written down rather than half-built.
type ReadinessReconciler struct {
	kube     client.Client
	signal   Signaller
	resource client.Object
}

// NewReadinessReconciler builds a watcher over one kind.
func NewReadinessReconciler(kube client.Client, signal Signaller, resource client.Object) *ReadinessReconciler {
	return &ReadinessReconciler{kube: kube, signal: signal, resource: resource}
}

// Reconcile reports one record's readiness to whoever is waiting for it.
//
// Signals are delivered more than once when this runs more than once, and
// that is fine: the workflow keeps readiness by memo, and hearing the same
// thing twice does not change what it knows.
func (r *ReadinessReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(r.resource.GetObjectKind().GroupVersionKind())

	if err := r.kube.Get(ctx, req.NamespacedName, obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	memo := obj.GetAnnotations()[worker.AnnotationMemo]
	runName := obj.GetLabels()[worker.LabelRun]

	if memo == "" || runName == "" {
		// Not ours: somebody created a record of this kind by hand.
		return ctrl.Result{}, nil
	}

	ready, reason := readiness(obj)
	if !ready {
		return ctrl.Result{}, nil
	}

	var run v1.Run

	key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: runName}
	if err := r.kube.Get(ctx, key, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if run.Status.WorkflowID == "" || run.Status.Phase.Terminal() {
		return ctrl.Result{}, nil
	}

	payload := agent.ReadySignal{
		Name:   memo,
		Ready:  true,
		Reason: reason,
		Status: statusOf(obj),
	}

	if err := r.signal.Signal(ctx, run.Status.WorkflowID, agent.SignalReady, payload); err != nil {
		return ctrl.Result{}, fmt.Errorf("сигнал готовности не дошёл: %w", err)
	}

	return ctrl.Result{}, nil
}

// SetupWithManager wires the watcher to its kind.
func (r *ReadinessReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := ctrl.NewControllerManagedBy(mgr).For(r.resource).Complete(r); err != nil {
		return fmt.Errorf("контроллер готовности не собрался: %w", err)
	}

	return nil
}

// readiness reads the Ready condition, which is what every kind in this
// ecosystem uses to say it has arrived — ours and Crossplane's alike.
func readiness(obj *unstructured.Unstructured) (bool, string) {
	conditions, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found {
		return false, ""
	}

	for _, one := range conditions {
		condition, ok := one.(map[string]any)
		if !ok {
			continue
		}

		if condition["type"] != "Ready" {
			continue
		}

		reason, _ := condition["reason"].(string)

		return condition["status"] == "True", reason
	}

	return false, ""
}

// statusOf is the record's status as the cluster has it — the address, the
// id, whatever the provider filled in.
func statusOf(obj *unstructured.Unstructured) []byte {
	status, found, err := unstructured.NestedMap(obj.Object, "status")
	if err != nil || !found {
		return nil
	}

	raw, err := json.Marshal(status)
	if err != nil {
		return nil
	}

	return raw
}
