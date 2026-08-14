package operator_test

import (
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/graphene-ci/graphene/internal/operator"
	"github.com/graphene-ci/graphene/sdk/agent"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

func machineScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	sch := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, v1.AddToScheme} {
		if err := add(sch); err != nil {
			t.Fatalf("схема не собралась: %v", err)
		}
	}

	return sch
}

func machine() *v1.Machine {
	return &v1.Machine{
		ObjectMeta: metav1.ObjectMeta{Name: "node-0", Namespace: "default"},
		Status: v1.MachineStatus{
			Queue: agent.InstallationQueue("perf-42-node-0"),
			Facts: map[string]string{"os": "linux"},
		},
	}
}

func lease(renewed time.Time) *coordinationv1.Lease {
	seconds := int32(operator.LeaseSeconds)
	renew := metav1.NewMicroTime(renewed)

	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "node-0", Namespace: "default"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptr("node-0"),
			LeaseDurationSeconds: &seconds,
			RenewTime:            &renew,
		},
	}
}

func ptr(value string) *string { return &value }

func reconcileMachine(t *testing.T, kube client.Client, now time.Time) ctrl.Result {
	t.Helper()

	reconciler := operator.NewMachineReconciler(kube)
	reconciler.Now = func() time.Time { return now }

	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "node-0"}}

	result, err := reconciler.Reconcile(t.Context(), request)
	if err != nil {
		t.Fatalf("сверка не прошла: %v", err)
	}

	return result
}

func loadMachine(t *testing.T, kube client.Client) *v1.Machine {
	t.Helper()

	var loaded v1.Machine
	if err := kube.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "node-0"}, &loaded); err != nil {
		t.Fatalf("машина не читается: %v", err)
	}

	return &loaded
}

func TestFreshLeaseMeansReady(t *testing.T) {
	t.Parallel()

	now := time.Now()
	kube := fake.NewClientBuilder().WithScheme(machineScheme(t)).
		WithObjects(machine(), lease(now.Add(-2*time.Second))).
		WithStatusSubresource(&v1.Machine{}).Build()

	result := reconcileMachine(t, kube, now)

	if !meta.IsStatusConditionTrue(loadMachine(t, kube).Status.Conditions, "Ready") {
		t.Fatal("свежая аренда, а машина не готова")
	}

	// Пересверка назначена на момент, когда аренда протухнет: иначе
	// неготовность заметил бы только следующий случайный повод.
	if result.RequeueAfter <= 0 {
		t.Fatal("пересверка не назначена — протухание некому заметить")
	}
}

func TestStaleLeaseMeansNotReady(t *testing.T) {
	t.Parallel()

	now := time.Now()
	stale := now.Add(-2 * operator.LeaseSeconds * time.Second)

	kube := fake.NewClientBuilder().WithScheme(machineScheme(t)).
		WithObjects(machine(), lease(stale)).
		WithStatusSubresource(&v1.Machine{}).Build()

	reconcileMachine(t, kube, now)

	after := loadMachine(t, kube)
	if meta.IsStatusConditionTrue(after.Status.Conditions, "Ready") {
		t.Fatal("аренда протухла, а машина готова")
	}

	condition := meta.FindStatusCondition(after.Status.Conditions, "Ready")
	if condition == nil || condition.Reason == "" {
		t.Fatalf("неготовность без причины: %+v", condition)
	}
}

// Машина без аренды вообще — это агент, который ни разу не отметился.
func TestNoLeaseMeansNotReady(t *testing.T) {
	t.Parallel()

	kube := fake.NewClientBuilder().WithScheme(machineScheme(t)).
		WithObjects(machine()).WithStatusSubresource(&v1.Machine{}).Build()

	reconcileMachine(t, kube, time.Now())

	if meta.IsStatusConditionTrue(loadMachine(t, kube).Status.Conditions, "Ready") {
		t.Fatal("аренды нет, а машина готова")
	}
}

// Снятие готовности НЕ ТРОГАЕТ то, что уже идёт.
//
// Готовность — про то, можно ли начинать новое; за судьбу идущего шага
// отвечает Temporal своими таймаутами. Смешать эти два срока значит
// убивать работающие шаги из-за сетевой икоты, и именно поэтому у этого
// контроллера нет и не может быть клиента Temporal: проверяем тем, что
// протухание меняет ровно условие Ready и ничего больше.
func TestLosingReadinessTouchesNothingElse(t *testing.T) {
	t.Parallel()

	now := time.Now()
	before := machine()

	kube := fake.NewClientBuilder().WithScheme(machineScheme(t)).
		WithObjects(before, lease(now.Add(-2*operator.LeaseSeconds*time.Second))).
		WithStatusSubresource(&v1.Machine{}).Build()

	reconcileMachine(t, kube, now)

	after := loadMachine(t, kube)
	if after.Status.Queue != before.Status.Queue {
		t.Fatalf("очередь тронули: %q", after.Status.Queue)
	}

	if after.Status.Facts["os"] != "linux" {
		t.Fatalf("факты тронули: %v", after.Status.Facts)
	}
}

// Факты становятся метками, потому что выбирают машины по меткам —
// селектор в статус не смотрит.
func TestFactsBecomeLabels(t *testing.T) {
	t.Parallel()

	now := time.Now()
	subject := machine()
	subject.Labels = map[string]string{"team": "perf"}
	subject.Status.Facts = map[string]string{
		"os":     "linux",
		"docker": "27.3.1",
		// Ядро с точками и дефисами — допустимое значение метки.
		"kernel": "6.8.0-137-generic",
		// А это в метку не влезет: пробелы и длина.
		"cpu": "Intel(R) Xeon(R) Gold 6248R CPU @ 3.00GHz с очень длинным именем",
	}

	kube := fake.NewClientBuilder().WithScheme(machineScheme(t)).
		WithObjects(subject, lease(now.Add(-2*time.Second))).
		WithStatusSubresource(&v1.Machine{}).Build()

	reconcileMachine(t, kube, now)

	after := loadMachine(t, kube)

	if after.Labels[operator.FactPrefix+"docker"] != "27.3.1" {
		t.Fatalf("докер не спроецировался: %v", after.Labels)
	}

	if after.Labels[operator.FactPrefix+"kernel"] != "6.8.0-137-generic" {
		t.Fatalf("ядро не спроецировалось: %v", after.Labels)
	}

	// Не влезло в метку — осталось фактом. Терять правду ради удобства
	// выборки нельзя.
	if _, projected := after.Labels[operator.FactPrefix+"cpu"]; projected {
		t.Fatalf("непригодное значение попало в метку: %v", after.Labels)
	}

	if after.Status.Facts["cpu"] == "" {
		t.Fatal("факт потерян, хотя меткой стать не мог")
	}

	// Метки человека — не наши.
	if after.Labels["team"] != "perf" {
		t.Fatalf("метку человека тронули: %v", after.Labels)
	}
}

// Факт исчез — метка обязана исчезнуть, иначе машина продолжит
// выбираться по докеру, которого на ней больше нет.
func TestLostFactLosesItsLabel(t *testing.T) {
	t.Parallel()

	now := time.Now()
	subject := machine()
	subject.Labels = map[string]string{operator.FactPrefix + "docker": "27.3.1"}
	subject.Status.Facts = map[string]string{"os": "linux"}

	kube := fake.NewClientBuilder().WithScheme(machineScheme(t)).
		WithObjects(subject, lease(now.Add(-2*time.Second))).
		WithStatusSubresource(&v1.Machine{}).Build()

	reconcileMachine(t, kube, now)

	if _, left := loadMachine(t, kube).Labels[operator.FactPrefix+"docker"]; left {
		t.Fatal("докер сняли с машины, а метка осталась")
	}
}
