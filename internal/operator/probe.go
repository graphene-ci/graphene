package operator

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/graphene-ci/graphene/api/v1"
)

// ProbeReconciler makes a Probe ready after its delay.
//
// A Probe is the wiring's own test subject: a record that becomes ready by
// itself, so that "record created → workflow woken → run finished" can be
// checked without a cloud provider or an agent in the way. When something
// breaks later, a Probe answers "is it the wiring or is it the provider" in
// one step.
type ProbeReconciler struct {
	kube client.Client
	// now is injectable so a test does not have to wait.
	now func() time.Time
}

// NewProbeReconciler builds one.
func NewProbeReconciler(kube client.Client) *ProbeReconciler {
	return &ProbeReconciler{kube: kube, now: time.Now}
}

// Reconcile makes the probe ready once enough time has passed since it was
// created, and asks to be called again when that moment arrives.
func (r *ProbeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var probe v1.Probe
	if err := r.kube.Get(ctx, req.NamespacedName, &probe); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if meta.IsStatusConditionTrue(probe.Status.Conditions, v1.ConditionReady) {
		return ctrl.Result{}, nil
	}

	elapsed := r.now().Sub(probe.CreationTimestamp.Time)
	if left := probe.Spec.After.Duration - elapsed; left > 0 {
		return ctrl.Result{RequeueAfter: left}, nil
	}

	meta.SetStatusCondition(&probe.Status.Conditions, metav1.Condition{
		Type:    v1.ConditionReady,
		Status:  metav1.ConditionTrue,
		Reason:  "Elapsed",
		Message: "срок вышел",
	})

	if err := r.kube.Status().Update(ctx, &probe); err != nil {
		return ctrl.Result{}, fmt.Errorf("готовность пробы не записалась: %w", err)
	}

	return ctrl.Result{}, nil
}

// SetupWithManager wires the reconciler to probes.
func (r *ProbeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := ctrl.NewControllerManagedBy(mgr).For(&v1.Probe{}).Complete(r); err != nil {
		return fmt.Errorf("контроллер проб не собрался: %w", err)
	}

	return nil
}
