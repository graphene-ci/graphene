package worker_test

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	v1 "github.com/graphene-ci/graphene/api/v1"
	"github.com/graphene-ci/graphene/internal/worker"
	"github.com/graphene-ci/graphene/pkg/agent"
)

func probeGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: v1.Group, Version: v1.Version, Resource: "probes"}
}

func newApplier(t *testing.T) (*worker.Applier, *dynamicfake.FakeDynamicClient) {
	t.Helper()

	// Схема нарочно пустая. Воркер работает с unstructured — в этом весь
	// смысл: новый провайдер не требует пересборки ничего нашего. Если
	// подсунуть сюда схему с нашими типами, поддельный клиент начнёт
	// превращать записи в типизированные и проверит не то, что мы делаем.
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{probeGVR(): "ProbeList"})

	resolve := func(_ context.Context, gvk schema.GroupVersionKind) (schema.GroupVersionResource, bool, error) {
		if gvk.Kind != "Probe" {
			return schema.GroupVersionResource{}, false, errors.New("вид неизвестен: " + gvk.String())
		}

		return probeGVR(), true, nil
	}

	return worker.NewApplier(client, resolve), client
}

func input(memo string) agent.ApplyInput {
	return agent.ApplyInput{
		Name:     memo,
		Manifest: []byte(`{"apiVersion":"graphene-ci.dev/v1","kind":"Probe","spec":{"after":"1s"}}`),
		Owner: agent.OwnerRef{
			Namespace: "default",
			Name:      "perf-42",
			UID:       "0f3d5b2a-1c44-4a0e-9f2b-7f1d9c8e5a31",
		},
	}
}

// Activity выполняется не менее одного раза. Второй вызов с той же
// памяткой обязан находить первую запись, а не заводить вторую машину.
func TestApplyIsIdempotentByMemo(t *testing.T) {
	t.Parallel()

	applier, client := newApplier(t)
	ctx := t.Context()

	first, err := applier.Apply(ctx, input("probe-0"))
	if err != nil {
		t.Fatalf("первый вызов не прошёл: %v", err)
	}

	if !first.Created {
		t.Fatal("первый вызов обязан создать запись")
	}

	second, err := applier.Apply(ctx, input("probe-0"))
	if err != nil {
		t.Fatalf("повтор не прошёл: %v", err)
	}

	if second.Created {
		t.Fatal("повтор создал вторую запись")
	}

	if second.Ref != first.Ref {
		t.Fatalf("повтор указал на другую запись: %+v против %+v", second.Ref, first.Ref)
	}

	list, err := client.Resource(probeGVR()).Namespace("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("список не прочитался: %v", err)
	}

	if len(list.Items) != 1 {
		t.Fatalf("в кластере %d записей, а прогон просил одну", len(list.Items))
	}
}

// Владение — это не пометка, а ссылка с UID. Имя прогона может быть
// переиспользовано следующим прогоном, и тогда старая ссылка отдала бы
// ему чужие машины.
func TestApplySetsOwnership(t *testing.T) {
	t.Parallel()

	applier, client := newApplier(t)
	ctx := t.Context()

	out, err := applier.Apply(ctx, input("probe-0"))
	if err != nil {
		t.Fatalf("не прошло: %v", err)
	}

	got, err := client.Resource(probeGVR()).Namespace("default").Get(ctx, out.Ref.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("запись не читается: %v", err)
	}

	owners := got.GetOwnerReferences()
	if len(owners) != 1 {
		t.Fatalf("ссылок на владельца %d, а нужна одна", len(owners))
	}

	if owners[0].Kind != "Run" || owners[0].Name != "perf-42" || string(owners[0].UID) == "" {
		t.Fatalf("владелец записан неверно: %+v", owners[0])
	}

	if got.GetLabels()[worker.LabelRun] != "perf-42" {
		t.Fatalf("нет метки прогона: %v", got.GetLabels())
	}

	if got.GetAnnotations()[worker.AnnotationMemo] != "probe-0" {
		t.Fatalf("памятка не сохранена: %v", got.GetAnnotations())
	}
}

// Спека доезжает до кластера как её написал пайплайн: мы в чужой вид не
// заглядываем и ничего в нём не правим.
func TestApplyKeepsTheManifestAsWritten(t *testing.T) {
	t.Parallel()

	applier, client := newApplier(t)
	ctx := t.Context()

	out, err := applier.Apply(ctx, input("probe-0"))
	if err != nil {
		t.Fatalf("не прошло: %v", err)
	}

	got, err := client.Resource(probeGVR()).Namespace("default").Get(ctx, out.Ref.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("запись не читается: %v", err)
	}

	after, found, err := unstructured.NestedString(got.Object, "spec", "after")
	if err != nil || !found || after != "1s" {
		t.Fatalf("спека поехала: %v найдено=%v ошибка=%v", after, found, err)
	}
}

// Разные памятки — разные записи, даже внутри одного прогона.
func TestApplyKeepsMemosApart(t *testing.T) {
	t.Parallel()

	applier, client := newApplier(t)
	ctx := t.Context()

	for _, memo := range []string{"probe-0", "probe-1"} {
		if _, err := applier.Apply(ctx, input(memo)); err != nil {
			t.Fatalf("%s не прошла: %v", memo, err)
		}
	}

	list, err := client.Resource(probeGVR()).Namespace("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("список не прочитался: %v", err)
	}

	if len(list.Items) != 2 {
		t.Fatalf("две памятки дали %d записей", len(list.Items))
	}
}

// Снос убирает то, что прогон создал, и не спотыкается о то, чего уже
// нет: до него могли добраться сборщик мусора или человек.
func TestTeardownRemovesWhatTheRunMade(t *testing.T) {
	t.Parallel()

	applier, client := newApplier(t)
	ctx := t.Context()

	out, err := applier.Apply(ctx, input("probe-0"))
	if err != nil {
		t.Fatalf("не прошло: %v", err)
	}

	gone := out.Ref
	gone.Name = "давно-удалённая"

	removed, err := applier.Teardown(ctx, agent.TeardownInput{
		Owner: input("probe-0").Owner,
		Refs:  []agent.ObjectRef{out.Ref, gone},
	})
	if err != nil {
		t.Fatalf("снос не прошёл: %v", err)
	}

	if len(removed.Removed) != 1 {
		t.Fatalf("снесено %d записей, а существовала одна", len(removed.Removed))
	}

	list, err := client.Resource(probeGVR()).Namespace("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("список не прочитался: %v", err)
	}

	if len(list.Items) != 0 {
		t.Fatalf("после сноса осталось %d записей", len(list.Items))
	}
}
