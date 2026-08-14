package operator

import (
	"context"
	"fmt"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

// StandReconciler ends a stand when its time is up.
//
// This is the reason a stand may exist at all. "Keep the machines for a
// day" is only allowed because there is something that removes them on the
// day after — an owner with a reason to die. Without this controller,
// Keep would be the polite way of leaking a cloud account.
//
// It deletes the stand and nothing else: what the stand owns goes with it
// through the cluster's own collector, the same way it would have gone with
// the run.
type StandReconciler struct {
	kube client.Client
	// Now is injectable so a test does not have to wait a day.
	Now func() time.Time
}

// NewStandReconciler builds one.
func NewStandReconciler(kube client.Client) *StandReconciler {
	return &StandReconciler{kube: kube, Now: time.Now}
}

// Reconcile removes the stand once its end has passed, and otherwise asks
// to be called again at that moment — a stand that nobody looks at must
// still expire.
func (r *StandReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var stand v1.Stand
	if err := r.kube.Get(ctx, req.NamespacedName, &stand); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if left := stand.Spec.Until.Sub(r.Now()); left > 0 {
		return ctrl.Result{RequeueAfter: left}, nil
	}

	if err := r.kube.Delete(ctx, &stand); err != nil {
		return ctrl.Result{}, fmt.Errorf("стенд %s не снёсся: %w", stand.Name, err)
	}

	return ctrl.Result{}, nil
}

// SetupWithManager wires the reconciler to stands.
func (r *StandReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := ctrl.NewControllerManagedBy(mgr).For(&v1.Stand{}).Complete(r); err != nil {
		return fmt.Errorf("контроллер стендов не собрался: %w", err)
	}

	return nil
}
