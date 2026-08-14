package operator_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/graphene-ci/graphene/internal/operator"
	"github.com/graphene-ci/graphene/sdk/agent"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

// started records what the reconciler asked Temporal to do.
type started struct {
	calls    []operator.StartRequest
	fail     error
	phase    v1.RunPhase
	stopped  int
	canceled int
}

func (s *started) Start(_ context.Context, req operator.StartRequest) (string, error) {
	if s.fail != nil {
		return "", s.fail
	}

	s.calls = append(s.calls, req)

	return "temporal-run-1", nil
}

func (s *started) Cancel(_ context.Context, _ string) error {
	s.canceled++

	return nil
}

func (s *started) Stop(_ context.Context, _ string) error {
	s.stopped++

	return nil
}

func (s *started) Phase(_ context.Context, _ string) (v1.RunPhase, string, error) {
	if s.phase == "" {
		return v1.RunRunning, "", nil
	}

	return s.phase, "", nil
}

func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	sch := runtime.NewScheme()
	if err := v1.AddToScheme(sch); err != nil {
		t.Fatalf("схема не собралась: %v", err)
	}

	return sch
}

func fixtures() (*v1.Run, *v1.PipelineRevision) {
	revision := &v1.PipelineRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "perf-7f3a91c", Namespace: "default"},
		Spec: v1.PipelineRevisionSpec{
			PipelineRef: v1.LocalRef{Name: "perf"},
			Image:       "registry.example.com/perf@sha256:0e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85",
			Queue:       "perf-7f3a91c",
		},
	}

	run := &v1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "perf-42", Namespace: "default", UID: types.UID("uid-42")},
		Spec: v1.RunSpec{
			RevisionRef: v1.LocalRef{Name: "perf-7f3a91c"},
			Params:      &apiextensionsv1.JSON{Raw: []byte(`{"nodes":3}`)},
		},
	}

	return run, revision
}

func reconcile(t *testing.T, kube client.Client, temporal operator.Temporal, known operator.Known) {
	t.Helper()

	reconciler := operator.NewRunReconciler(kube, temporal, known, nil, nil)

	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "perf-42"}}
	if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
		t.Fatalf("сверка не прошла: %v", err)
	}
}

func load(t *testing.T, kube client.Client) *v1.Run {
	t.Helper()

	var run v1.Run
	if err := kube.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "perf-42"}, &run); err != nil {
		t.Fatalf("прогон не читается: %v", err)
	}

	return &run
}

// Запись Run поднимает воркфлоу. Идентификатор воркфлоу равен имени
// записи — именно это делает старт безопасным для повтора: вторая попытка
// сталкивается с первой вместо того, чтобы завести второй прогон.
func TestRunStartsWorkflow(t *testing.T) {
	t.Parallel()

	run, revision := fixtures()
	kube := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(run, revision).WithStatusSubresource(run).Build()

	temporal := &started{}
	reconcile(t, kube, temporal, nil)

	if len(temporal.calls) != 1 {
		t.Fatalf("воркфлоу запущен %d раз", len(temporal.calls))
	}

	call := temporal.calls[0]
	if call.WorkflowID != "perf-42" {
		t.Fatalf("идентификатор воркфлоу %q, а не имя записи", call.WorkflowID)
	}

	if call.Queue != "perf-7f3a91c" {
		t.Fatalf("очередь %q, а у ревизии своя", call.Queue)
	}

	if call.Input.Owner != (agent.OwnerRef{Namespace: "default", Name: "perf-42", UID: "uid-42"}) {
		t.Fatalf("владелец передан неверно: %+v", call.Input.Owner)
	}

	if string(call.Input.Params) != `{"nodes":3}` {
		t.Fatalf("параметры поехали: %s", call.Input.Params)
	}

	after := load(t, kube)
	if after.Status.WorkflowID != "perf-42" || after.Status.TemporalRunID != "temporal-run-1" {
		t.Fatalf("статус не записан: %+v", after.Status)
	}

	if after.Status.Phase != v1.RunRunning {
		t.Fatalf("фаза %q после старта", after.Status.Phase)
	}
}

// Сверка идёт много раз — на каждое изменение записи и на каждый
// перезапуск оператора. Второй проход не имеет права стартовать второй
// воркфлоу.
func TestRunDoesNotStartTwice(t *testing.T) {
	t.Parallel()

	run, revision := fixtures()
	kube := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(run, revision).WithStatusSubresource(run).Build()

	temporal := &started{}

	reconcile(t, kube, temporal, nil)
	reconcile(t, kube, temporal, nil)
	reconcile(t, kube, temporal, nil)

	if len(temporal.calls) != 1 {
		t.Fatalf("воркфлоу запущен %d раз вместо одного", len(temporal.calls))
	}
}

// Прогон, который просит несуществующую ревизию, отказывает внятно и не
// висит в Pending без объяснения.
func TestRunWithoutRevisionFails(t *testing.T) {
	t.Parallel()

	run, _ := fixtures()
	kube := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(run).WithStatusSubresource(run).Build()

	temporal := &started{}
	reconcile(t, kube, temporal, nil)

	if len(temporal.calls) != 0 {
		t.Fatal("воркфлоу запущен без ревизии")
	}

	after := load(t, kube)
	if after.Status.Phase != v1.RunFailed {
		t.Fatalf("фаза %q, а ревизии нет", after.Status.Phase)
	}

	if after.Status.Reason == "" {
		t.Fatal("отказ без причины")
	}
}

// Завершённый прогон не трогают: запись — это история, а не живой
// процесс, и сверка по ней ничего не запускает.
func TestRunInTerminalPhaseIsLeftAlone(t *testing.T) {
	t.Parallel()

	run, revision := fixtures()
	run.Status.Phase = v1.RunSucceeded
	run.Status.WorkflowID = "perf-42"

	kube := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(run, revision).WithStatusSubresource(run).Build()

	temporal := &started{fail: errors.New("сюда ходить не должны")}
	reconcile(t, kube, temporal, nil)

	if load(t, kube).Status.Phase != v1.RunSucceeded {
		t.Fatal("фаза завершённого прогона изменилась")
	}
}

// everything says the cluster serves whatever is asked.
func everything(_ context.Context, _ agent.Kind) (bool, error) { return true, nil }

// nothing says the cluster serves none of it.
func nothing(_ context.Context, _ agent.Kind) (bool, error) { return false, nil }

// Отказ приходит ДО старта воркфлоу и называет вид. Половина построенного
// стенда стоит денег и требует уборки; отказ до первого шага не стоит
// ничего.
func TestRunRefusesWhenAKindIsMissing(t *testing.T) {
	t.Parallel()

	run, revision := fixtures()
	revision.Status.Requires = []v1.Requirement{
		{Group: "compute.yandex.crossplane.io", Version: "v1alpha1", Kind: "Instance"},
	}

	kube := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(run, revision).WithStatusSubresource(run).Build()

	temporal := &started{}
	reconcile(t, kube, temporal, nothing)

	if len(temporal.calls) != 0 {
		t.Fatal("воркфлоу запущен, хотя вида нет")
	}

	after := load(t, kube)
	if after.Status.Phase != v1.RunFailed {
		t.Fatalf("фаза %q, а вида нет", after.Status.Phase)
	}

	if !strings.Contains(after.Status.Reason, "Instance") {
		t.Fatalf("отказ не называет вид: %q", after.Status.Reason)
	}
}

func TestRunStartsWhenEveryKindIsThere(t *testing.T) {
	t.Parallel()

	run, revision := fixtures()
	revision.Status.Requires = []v1.Requirement{
		{Group: "compute.yandex.crossplane.io", Version: "v1alpha1", Kind: "Instance"},
	}

	kube := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(run, revision).WithStatusSubresource(run).Build()

	temporal := &started{}
	reconcile(t, kube, temporal, everything)

	if len(temporal.calls) != 1 {
		t.Fatalf("воркфлоу запущен %d раз", len(temporal.calls))
	}
}

// Икота discovery не должна останавливать работу: она сообщает о пропаже,
// а не создаёт её.
func TestDiscoveryFailureDoesNotRefuseTheRun(t *testing.T) {
	t.Parallel()

	run, revision := fixtures()
	revision.Status.Requires = []v1.Requirement{
		{Group: "compute.yandex.crossplane.io", Version: "v1alpha1", Kind: "Instance"},
	}

	kube := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(run, revision).WithStatusSubresource(run).Build()

	temporal := &started{}
	reconcile(t, kube, temporal, func(_ context.Context, _ agent.Kind) (bool, error) {
		return false, errors.New("кэш не отвечает")
	})

	if len(temporal.calls) != 1 {
		t.Fatalf("икота discovery остановила прогон: запусков %d", len(temporal.calls))
	}
}

// swept records what the reconciler asked to be removed, and how much it
// pretends is still there.
type swept struct {
	calls int
	left  int
	kinds []agent.Kind
}

func (s *swept) Sweep(_ context.Context, _ agent.OwnerRef, kinds []agent.Kind) (int, error) {
	s.calls++
	s.kinds = kinds

	return s.left, nil
}

func deleting(t *testing.T, kube client.Client, temporal operator.Temporal, sweep operator.Sweeper) ctrl.Result {
	t.Helper()

	reconciler := operator.NewRunReconciler(kube, temporal, nil, nil, sweep)

	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "perf-42"}}

	result, err := reconciler.Reconcile(t.Context(), request)
	if err != nil {
		t.Fatalf("сверка не прошла: %v", err)
	}

	return result
}

// Финализатор ставится при старте: без него удаление записи убрало бы
// запись, а машины оставило.
func TestRunGetsAFinalizer(t *testing.T) {
	t.Parallel()

	run, revision := fixtures()
	kube := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(run, revision).WithStatusSubresource(run).Build()

	reconciler := operator.NewRunReconciler(kube, &started{}, nil, nil, &swept{})

	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "perf-42"}}
	if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
		t.Fatalf("сверка не прошла: %v", err)
	}

	if len(load(t, kube).Finalizers) == 0 {
		t.Fatal("финализатор не поставлен")
	}
}

// Пока в облаке что-то осталось, запись не исчезает: иначе «удалил
// прогон» значило бы «перестал видеть счёт».
func TestRunWaitsWhileSomethingIsLeft(t *testing.T) {
	t.Parallel()

	run, revision := fixtures()
	run.Finalizers = []string{"graphene-ci.dev/teardown"}
	run.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	run.Status.Phase = v1.RunRunning
	run.Status.WorkflowID = "perf-42"

	kube := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(run, revision).WithStatusSubresource(run).Build()

	temporal := &started{}
	sweeper := &swept{left: 2}

	result := deleting(t, kube, temporal, sweeper)

	if result.RequeueAfter <= 0 {
		t.Fatal("осталось неубранное, а пересверка не назначена")
	}

	if temporal.stopped != 1 {
		t.Fatalf("воркфлоу остановлен %d раз: идущий пайплайн успеет создать ещё", temporal.stopped)
	}

	if sweeper.calls == 0 {
		t.Fatal("снос не запрошен")
	}
}

// Когда не осталось ничего — финализатор снимается и запись уходит.
func TestRunLetsGoWhenNothingIsLeft(t *testing.T) {
	t.Parallel()

	run, revision := fixtures()
	run.Finalizers = []string{"graphene-ci.dev/teardown"}
	run.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	run.Status.Phase = v1.RunSucceeded

	kube := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(run, revision).WithStatusSubresource(run).Build()

	deleting(t, kube, &started{}, &swept{left: 0})

	var gone v1.Run

	err := kube.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "perf-42"}, &gone)
	if err == nil && len(gone.Finalizers) > 0 {
		t.Fatal("убирать нечего, а финализатор держит запись")
	}
}

// Просьба об отмене доходит до воркфлоу — и это ОТМЕНА, а не убийство:
// отменённый пайплайн получает возможность прибрать за собой, ради чего
// Teardown и написан на отвязанном контексте.
func TestCancelReachesTheWorkflow(t *testing.T) {
	t.Parallel()

	run, revision := fixtures()
	run.Spec.Cancel = true
	run.Status.Phase = v1.RunRunning
	run.Status.WorkflowID = "perf-42"

	kube := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(run, revision).WithStatusSubresource(run).Build()

	temporal := &started{phase: v1.RunRunning}
	reconcile(t, kube, temporal, nil)

	if temporal.canceled != 1 {
		t.Fatalf("отмена отправлена %d раз", temporal.canceled)
	}

	if temporal.stopped != 0 {
		t.Fatal("вместо отмены воркфлоу убили: пайплайн не успеет прибрать за собой")
	}
}

// Завершённый прогон отменять нечего.
func TestCancelDoesNotTouchAFinishedRun(t *testing.T) {
	t.Parallel()

	run, revision := fixtures()
	run.Spec.Cancel = true
	run.Status.Phase = v1.RunSucceeded
	run.Status.WorkflowID = "perf-42"

	kube := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(run, revision).WithStatusSubresource(run).Build()

	temporal := &started{}
	reconcile(t, kube, temporal, nil)

	if temporal.canceled != 0 {
		t.Fatal("отменяли то, что уже кончилось")
	}
}

// Прогон живёт свой срок и уходит. Запись — это история того, что
// случилось, и её стоит хранить, но не бесконечно: вместе с ней уходит
// история воркфлоу, которая и заполняет место.
func TestFinishedRunIsKeptForItsTime(t *testing.T) {
	t.Parallel()

	run, revision := fixtures()
	run.Status.Phase = v1.RunSucceeded
	finished := metav1.NewTime(time.Now().Add(-time.Hour))
	run.Status.FinishedAt = &finished

	pipeline := &v1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "perf", Namespace: "default"},
		Spec:       v1.PipelineSpec{Retention: &metav1.Duration{Duration: 24 * time.Hour}},
	}

	kube := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(run, revision, pipeline).WithStatusSubresource(run).Build()

	reconciler := operator.NewRunReconciler(kube, &started{}, nil, nil, nil)

	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "perf-42"}}

	result, err := reconciler.Reconcile(t.Context(), request)
	if err != nil {
		t.Fatalf("сверка не прошла: %v", err)
	}

	if result.RequeueAfter <= 0 {
		t.Fatal("срок не вышел, а пересверка не назначена — истечение некому заметить")
	}

	var still v1.Run
	if err := kube.Get(t.Context(), request.NamespacedName, &still); err != nil {
		t.Fatalf("прогон убрали раньше срока: %v", err)
	}
}

// Срок вышел — прогон уходит, и политика пайплайна главнее умолчания.
func TestFinishedRunGoesWhenItsTimeIsUp(t *testing.T) {
	t.Parallel()

	run, revision := fixtures()
	run.Status.Phase = v1.RunSucceeded
	finished := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	run.Status.FinishedAt = &finished

	pipeline := &v1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "perf", Namespace: "default"},
		Spec:       v1.PipelineSpec{Retention: &metav1.Duration{Duration: time.Hour}},
	}

	kube := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(run, revision, pipeline).WithStatusSubresource(run).Build()

	reconciler := operator.NewRunReconciler(kube, &started{}, nil, nil, nil)

	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "perf-42"}}
	if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
		t.Fatalf("сверка не прошла: %v", err)
	}

	var gone v1.Run
	if err := kube.Get(t.Context(), request.NamespacedName, &gone); err == nil {
		t.Fatal("срок вышел, а прогон остался: истории копятся вечно")
	}
}
