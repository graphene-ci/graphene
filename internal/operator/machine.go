package operator

import (
	"context"
	"fmt"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1 "github.com/graphene-ci/graphene/api/v1"
)

// LeaseSeconds is how long an agent's mark is good for. The agent renews
// well inside it; a machine whose lease is older than this has stopped
// answering.
//
// The same number kubelet uses for the same job, and for the same reason:
// short enough that a dead machine is noticed while somebody still cares,
// long enough that an ordinary network hiccup is not news.
const LeaseSeconds = 40

// MachineReconciler decides whether a machine can be given new work.
//
// It has no Temporal client, and that is the design rather than an
// omission. Readiness is about whether NEW work may start; the fate of work
// already running is Temporal's, decided by its own timeouts. The two
// clocks are independent, and joining them would mean killing live steps
// because of a network hiccup.
type MachineReconciler struct {
	kube client.Client
	// Now is injectable so a test does not have to wait forty seconds.
	Now func() time.Time
}

// NewMachineReconciler builds one.
func NewMachineReconciler(kube client.Client) *MachineReconciler {
	return &MachineReconciler{kube: kube, Now: time.Now}
}

// Reconcile sets the machine's readiness from its lease, and asks to be
// called again when that lease would go stale — otherwise a machine that
// went quiet would keep its readiness until something unrelated happened
// to wake this controller.
func (r *MachineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var machine v1.Machine
	if err := r.kube.Get(ctx, req.NamespacedName, &machine); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	left, err := r.leaseLeft(ctx, req.NamespacedName)
	if err != nil {
		return ctrl.Result{}, err
	}

	condition := metav1.Condition{
		Type:    v1.ConditionReady,
		Status:  metav1.ConditionTrue,
		Reason:  "AgentAnswering",
		Message: "агент отмечается",
	}

	if left <= 0 {
		condition.Status = metav1.ConditionFalse
		condition.Reason = "AgentSilent"
		condition.Message = fmt.Sprintf("агент молчит дольше %d с", LeaseSeconds)
	}

	if !meta.SetStatusCondition(&machine.Status.Conditions, condition) {
		return ctrl.Result{RequeueAfter: requeueFor(left)}, nil
	}

	if err := r.kube.Status().Update(ctx, &machine); err != nil {
		return ctrl.Result{}, fmt.Errorf("готовность машины не записалась: %w", err)
	}

	return ctrl.Result{RequeueAfter: requeueFor(left)}, nil
}

// leaseLeft reports how long the machine's mark is still good for. A
// missing lease is an agent that has never checked in, which is not an
// error — it is the answer.
func (r *MachineReconciler) leaseLeft(ctx context.Context, key types.NamespacedName) (time.Duration, error) {
	var lease coordinationv1.Lease

	err := r.kube.Get(ctx, key, &lease)
	if apierrors.IsNotFound(err) {
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("аренда не читается: %w", err)
	}

	if lease.Spec.RenewTime == nil {
		return 0, nil
	}

	span := time.Duration(LeaseSeconds) * time.Second
	if lease.Spec.LeaseDurationSeconds != nil {
		span = time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
	}

	return lease.Spec.RenewTime.Add(span).Sub(r.Now()), nil
}

// requeueFor says when to look again: at the moment the lease would go
// stale, or one lease later once it already has.
func requeueFor(left time.Duration) time.Duration {
	if left > 0 {
		return left
	}

	return time.Duration(LeaseSeconds) * time.Second
}

// SetupWithManager wires the reconciler to machines and to their leases.
func (r *MachineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		For(&v1.Machine{}).
		WatchesRawSource(source.Kind(mgr.GetCache(), &coordinationv1.Lease{},
			handler.TypedEnqueueRequestsFromMapFunc(sameName))).
		Complete(r)
	if err != nil {
		return fmt.Errorf("контроллер машин не собрался: %w", err)
	}

	return nil
}

// sameName maps a lease to the machine of the same name. The agent renews
// its lease constantly, and each renewal is what tells this controller the
// machine is still there.
func sameName(_ context.Context, lease *coordinationv1.Lease) []reconcile.Request {
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Namespace: lease.GetNamespace(), Name: lease.GetName()},
	}}
}
