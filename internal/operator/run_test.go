package operator_test

import (
	"context"
	"errors"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "github.com/graphene-ci/graphene/api/v1"
	"github.com/graphene-ci/graphene/internal/operator"
	"github.com/graphene-ci/graphene/pkg/agent"
)

// started records what the reconciler asked Temporal to do.
type started struct {
	calls []operator.StartRequest
	fail  error
	phase v1.RunPhase
}

func (s *started) Start(_ context.Context, req operator.StartRequest) (string, error) {
	if s.fail != nil {
		return "", s.fail
	}

	s.calls = append(s.calls, req)

	return "temporal-run-1", nil
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

func reconcile(t *testing.T, kube client.Client, temporal operator.Temporal) {
	t.Helper()

	reconciler := operator.NewRunReconciler(kube, temporal)

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
	reconcile(t, kube, temporal)

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

	reconcile(t, kube, temporal)
	reconcile(t, kube, temporal)
	reconcile(t, kube, temporal)

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
	reconcile(t, kube, temporal)

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
	reconcile(t, kube, temporal)

	if load(t, kube).Status.Phase != v1.RunSucceeded {
		t.Fatal("фаза завершённого прогона изменилась")
	}
}
