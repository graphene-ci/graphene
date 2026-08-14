package operator

import (
	"context"
	"fmt"
	"regexp"
	"strings"
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

	"github.com/graphene-ci/graphene/sdk/agent"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

// FactPrefix is where a machine's facts appear as labels.
//
// Facts live in the status because a fact is whatever the machine turned
// out to have, and that is not ours to constrain. Labels live in metadata
// because that is what selectors read — and choosing a machine by what it
// has is the whole point of having facts at all.
//
// The projection goes ONE way: facts are the truth, labels are their
// shadow. One writer per field, or the two drift and nobody can say which
// lied.
const FactPrefix = v1.Group + "/fact-"

// labelLimit is what kubernetes accepts for a label's value.
const labelLimit = 63

// LeaseSeconds is how long an agent's mark is good for — the agent's own
// number, because both sides must mean the same thing by silence.
//
// The same order kubelet uses for the same job, and for the same reason:
// short enough that a dead machine is noticed while somebody still cares,
// long enough that an ordinary network hiccup is not news.
const LeaseSeconds = agent.LeaseSeconds

// validLabelValue is the shape kubernetes requires of a label value.
var validLabelValue = regexp.MustCompile(`^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$`)

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

	if err := r.project(ctx, &machine); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.free(ctx, &machine); err != nil {
		return ctrl.Result{}, err
	}

	before := machine.DeepCopy()

	if !meta.SetStatusCondition(&machine.Status.Conditions, condition) {
		return ctrl.Result{RequeueAfter: requeueFor(left)}, nil
	}

	// Заплатка, а не запись целиком, и это не оптимизация. У статуса
	// машины ДВА писателя: этот контроллер пишет готовность, а захват
	// пишет системный воркер из другого процесса. Update посылает статус
	// целиком, поэтому слегка устаревшая копия здесь стирала бы чужой
	// захват — что и случилось на первой же сквозной проверке.
	if err := r.kube.Status().Patch(ctx, &machine, client.MergeFrom(before)); err != nil {
		return ctrl.Result{}, fmt.Errorf("готовность машины не записалась: %w", err)
	}

	return ctrl.Result{RequeueAfter: requeueFor(left)}, nil
}

// free lets go of a machine whose holder is gone.
//
// A claim points at a run or a stand together with its UID, so this is not
// a guess: the holder either exists with that identity or it does not. A
// name alone would not do — a later run can be called the same thing, and
// it would inherit machines it never asked for.
//
// This is why claiming needed no timer and no reaper of its own. The claim
// ends when its holder ends, which is the same rule everything else here
// follows.
func (r *MachineReconciler) free(ctx context.Context, machine *v1.Machine) error {
	claim := machine.Status.Claim
	if claim == nil {
		return nil
	}

	if r.alive(ctx, machine.Namespace, claim) {
		return nil
	}

	before := machine.DeepCopy()
	machine.Status.Claim = nil

	if err := r.kube.Status().Patch(ctx, machine, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("машина %s не освободилась: %w", machine.Name, err)
	}

	return nil
}

// alive answers whether the thing holding this machine is still there.
func (r *MachineReconciler) alive(ctx context.Context, namespace string, claim *v1.ClaimRef) bool {
	key := types.NamespacedName{Namespace: namespace, Name: claim.Name}

	switch claim.Kind {
	case "Run":
		var run v1.Run
		if err := r.kube.Get(ctx, key, &run); err != nil {
			return false
		}

		return claim.UID == "" || string(run.UID) == claim.UID
	case "Stand":
		var stand v1.Stand
		if err := r.kube.Get(ctx, key, &stand); err != nil {
			return false
		}

		return claim.UID == "" || string(stand.UID) == claim.UID
	default:
		// Держатель неизвестного вида — держатель, за которого никто не
		// отвечает. Освобождаем.
		return false
	}
}

// project copies the machine's facts into labels, so that a pipeline can
// ask for a machine by what it has.
//
// Only what fits: a label value is at most 63 characters from a limited
// alphabet, and a fact is an arbitrary string. What does not fit stays a
// fact and simply cannot be selected on — losing the truth to make the
// selection prettier would be the wrong trade.
//
// Labels a PERSON put on the machine are never touched. Only our own
// prefix is ours to manage, and a projection that removed somebody's
// "team=perf" would be a projection nobody could trust.
func (r *MachineReconciler) project(ctx context.Context, machine *v1.Machine) error {
	// Снимок ДО правки: GetLabels отдаёт ту же карту, что лежит в
	// объекте, и правка на месте сделала бы заплатку пустой.
	before := machine.DeepCopy()

	labels := map[string]string{}
	for name, value := range machine.GetLabels() {
		labels[name] = value
	}

	changed := false

	// Факт исчез — метка обязана исчезнуть: иначе машина продолжит
	// выбираться по докеру, которого на ней больше нет.
	for name := range labels {
		if !strings.HasPrefix(name, FactPrefix) {
			continue
		}

		fact := strings.TrimPrefix(name, FactPrefix)
		if value, found := machine.Status.Facts[fact]; !found || !labelValue(value) {
			delete(labels, name)

			changed = true
		}
	}

	for fact, value := range machine.Status.Facts {
		if !labelName(fact) || !labelValue(value) {
			continue
		}

		if labels[FactPrefix+fact] != value {
			labels[FactPrefix+fact] = value
			changed = true
		}
	}

	if !changed {
		return nil
	}

	machine.SetLabels(labels)

	if err := r.kube.Patch(ctx, machine, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("факты не спроецировались в метки: %w", err)
	}

	return nil
}

// labelValue answers whether kubernetes would accept this as a label value.
func labelValue(value string) bool {
	if value == "" || len(value) > labelLimit {
		return false
	}

	return validLabelValue.MatchString(value)
}

// labelName answers whether the fact's name can be part of a label's name.
func labelName(name string) bool {
	return name != "" && len(name) <= labelLimit && validLabelValue.MatchString(name)
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
