package operator_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/graphene-ci/graphene/internal/kube"
	"github.com/graphene-ci/graphene/internal/operator"
	"github.com/graphene-ci/graphene/internal/worker"
	"github.com/graphene-ci/graphene/sdk/agent"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

// heard collects the signals readiness sent.
type heard struct {
	mu      sync.Mutex
	signals []agent.ReadySignal
}

func (h *heard) Signal(_ context.Context, _, _ string, payload agent.ReadySignal) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.signals = append(h.signals, payload)

	return nil
}

func (h *heard) len() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return len(h.signals)
}

// Чужой вид, которого не существовало, когда этот бинарь собирали.
func foreign() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group: "compute.yandex.crossplane.io", Version: "v1alpha1", Resource: "instances",
	}
}

func foreignKind() agent.Kind {
	return agent.Kind{Group: "compute.yandex.crossplane.io", Version: "v1alpha1", Kind: "Instance"}
}

func resolver() kube.Resolver {
	return func(_ context.Context, gvk schema.GroupVersionKind) (schema.GroupVersionResource, bool, error) {
		if gvk.Kind != "Instance" {
			return schema.GroupVersionResource{}, false, errors.New("вид неизвестен: " + gvk.String())
		}

		return foreign(), true, nil
	}
}

// instance is a record of a foreign kind, made by us, already ready.
func instance(runName string, ready bool) *unstructured.Unstructured {
	status := "False"
	if ready {
		status = "True"
	}

	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "compute.yandex.crossplane.io/v1alpha1",
		"kind":       "Instance",
		"metadata": map[string]any{
			"name":      "perf-42-node-0",
			"namespace": "default",
			"labels": map[string]any{
				worker.LabelRun:     runName,
				worker.LabelManaged: "true",
			},
			"annotations": map[string]any{worker.AnnotationMemo: "node-0"},
		},
		"status": map[string]any{
			"atProvider": map[string]any{"id": "epd123"},
			"conditions": []any{map[string]any{
				"type": "Ready", "status": status, "reason": "Available",
			}},
		},
	}}
}

func running(name string) *v1.Run {
	return &v1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Status:     v1.RunStatus{Phase: v1.RunRunning, WorkflowID: name},
	}
}

// Готовность ЧУЖОГО вида доезжает до воркфлоу.
//
// Это то, ради чего задача существует: вид неизвестен на момент сборки —
// его CRD ставит человек, когда ему понадобился провайдер, — и информер
// заводится по требованию ревизии, а не по списку в нашем коде.
func TestForeignKindReadinessReachesTheWorkflow(t *testing.T) {
	t.Parallel()

	source := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{foreign(): "InstanceList"})

	records := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(running("perf-42")).WithStatusSubresource(&v1.Run{}).Build()

	listener := &heard{}
	readiness := operator.NewReadiness(records, source, resolver(), listener)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	if err := readiness.Watch(ctx, foreignKind()); err != nil {
		t.Fatalf("наблюдение не встало: %v", err)
	}

	go func() { _ = readiness.Start(ctx) }()

	_, err := source.Resource(foreign()).Namespace("default").
		Create(ctx, instance("perf-42", true), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("запись не создалась: %v", err)
	}

	await := time.After(10 * time.Second)

	for listener.len() == 0 {
		select {
		case <-await:
			t.Fatal("готовность чужого вида не доехала")
		case <-time.After(20 * time.Millisecond):
		}
	}

	got := listener.signals[0]
	if got.Name != "node-0" || !got.Ready {
		t.Fatalf("сигнал не тот: %+v", got)
	}

	// Статус доезжает целиком: пайплайн читает из него адрес, id — то,
	// что вписал провайдер и чего не было в спеке.
	if len(got.Status) == 0 {
		t.Fatal("статус записи не доехал")
	}
}

// Вид, которого кластер не обслуживает, — внятный отказ, а не молчаливое
// ничего.
func TestWatchingAnUnknownKindRefuses(t *testing.T) {
	t.Parallel()

	source := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{foreign(): "InstanceList"})

	readiness := operator.NewReadiness(
		fake.NewClientBuilder().WithScheme(scheme(t)).Build(), source, resolver(), &heard{})

	err := readiness.Watch(t.Context(), agent.Kind{Group: "нет", Version: "v1", Kind: "Ничего"})
	if !errors.Is(err, operator.ErrNotWatchable) {
		t.Fatalf("ожидали ErrNotWatchable, получили %v", err)
	}
}

// Просят каждый прогон каждой ревизии — второй раз обязан быть дёшев и
// не заводить второй информер.
func TestWatchingTwiceIsHarmless(t *testing.T) {
	t.Parallel()

	source := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{foreign(): "InstanceList"})

	readiness := operator.NewReadiness(
		fake.NewClientBuilder().WithScheme(scheme(t)).Build(), source, resolver(), &heard{})

	for range 3 {
		if err := readiness.Watch(t.Context(), foreignKind()); err != nil {
			t.Fatalf("повторное наблюдение не встало: %v", err)
		}
	}
}
