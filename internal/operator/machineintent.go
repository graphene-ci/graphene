package operator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

// retryInstall is how long to wait before trying an unreachable machine
// again. A machine that is down comes back; a typo in the address does
// not, and the reason on the record says which it looks like.
const retryInstall = 30 * time.Second

// ErrNoKey means the secret named by the intent is not there or is empty.
var ErrNoKey = errors.New("ключ для входа не найден")

// InstallRequest is one trip to a machine that already exists.
type InstallRequest struct {
	Address string
	User    string
	Key     []byte
	// HostKey is what the machine must prove it is.
	HostKey string
	Script  string
}

// Installer goes to the machine and runs the script.
//
// A function rather than an ssh client so that this controller can be
// checked without a machine — and so that ssh, with its own failure modes
// and its own timeouts, stays at the edge.
type Installer func(ctx context.Context, req InstallRequest) error

// MachineIntentReconciler puts an agent on a machine somebody else made.
//
// This is the half of the promise that is easy to forget: the system works
// with what it did not create. The machine is there, ssh to it is there,
// and the installation is ours to perform.
//
// Note what it does NOT do: it does not create a Machine record. The agent
// does that when it connects, exactly as on a cloud VM. One proof of
// existence — an agent that came up — and not two.
type MachineIntentReconciler struct {
	kube    client.Client
	install Installer
}

// NewMachineIntentReconciler builds one.
func NewMachineIntentReconciler(kube client.Client, install Installer) *MachineIntentReconciler {
	return &MachineIntentReconciler{kube: kube, install: install}
}

// Reconcile makes the agent exist on the machine, once.
func (r *MachineIntentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var intent v1.MachineIntent
	if err := r.kube.Get(ctx, req.NamespacedName, &intent); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Уже поставлено — второй раз не ходим. Скрипт и сам сходится, но
	// ходить по чужой машине лишний раз незачем.
	if meta.IsStatusConditionTrue(intent.Status.Conditions, v1.ConditionReady) {
		return ctrl.Result{}, nil
	}

	// Не зная, кто должен ответить по этому адресу, не идём вовсе.
	// Доверие при первом подключении — это то, что делает человек за
	// терминалом; здесь мы открываем на той стороне корневую оболочку и
	// кормим её скриптом с токеном установки внутри.
	if strings.TrimSpace(intent.Spec.HostKey) == "" {
		return ctrl.Result{}, r.record(ctx, &intent, ErrNoHostKey)
	}

	key, err := r.key(ctx, &intent)
	if err != nil {
		return ctrl.Result{RequeueAfter: retryInstall}, r.record(ctx, &intent, err)
	}

	err = r.install(ctx, InstallRequest{
		Address: intent.Spec.Address,
		User:    intent.Spec.User,
		Key:     key,
		HostKey: intent.Spec.HostKey,
		Script:  intent.Spec.Script,
	})
	if err != nil {
		return ctrl.Result{RequeueAfter: retryInstall}, r.record(ctx, &intent, err)
	}

	return ctrl.Result{}, r.record(ctx, &intent, nil)
}

// key resolves the private key. The value is read here and goes no
// further than the trip it is needed for.
func (r *MachineIntentReconciler) key(ctx context.Context, intent *v1.MachineIntent) ([]byte, error) {
	var secret corev1.Secret

	name := types.NamespacedName{Namespace: intent.Namespace, Name: intent.Spec.Key.Name}
	if err := r.kube.Get(ctx, name, &secret); err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrNoKey, intent.Spec.Key.Name, err)
	}

	if wanted := intent.Spec.Key.Key; wanted != "" {
		value, found := secret.Data[wanted]
		if !found || len(value) == 0 {
			return nil, fmt.Errorf("%w: в секрете %s нет ключа %s", ErrNoKey, secret.Name, wanted)
		}

		return value, nil
	}

	for _, value := range secret.Data {
		if len(value) > 0 {
			return value, nil
		}
	}

	return nil, fmt.Errorf("%w: секрет %s пуст", ErrNoKey, secret.Name)
}

// record writes how it went. A failure is a condition with a reason, not a
// silence: a machine nobody could reach must say so on its own record.
func (r *MachineIntentReconciler) record(ctx context.Context, intent *v1.MachineIntent, why error) error {
	condition := metav1.Condition{
		Type:    v1.ConditionReady,
		Status:  metav1.ConditionTrue,
		Reason:  "AgentInstalled",
		Message: "агент поставлен",
	}

	if why != nil {
		condition.Status = metav1.ConditionFalse
		condition.Reason = "InstallFailed"
		condition.Message = why.Error()
	}

	if !meta.SetStatusCondition(&intent.Status.Conditions, condition) {
		return nil
	}

	if err := r.kube.Status().Update(ctx, intent); err != nil {
		return fmt.Errorf("состояние установки не записалось: %w", err)
	}

	return nil
}

// SetupWithManager wires the reconciler to intents.
func (r *MachineIntentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := ctrl.NewControllerManagedBy(mgr).For(&v1.MachineIntent{}).Complete(r); err != nil {
		return fmt.Errorf("контроллер установок не собрался: %w", err)
	}

	return nil
}
