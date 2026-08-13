package cli_test

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "github.com/graphene-ci/graphene/api/v1"
	"github.com/graphene-ci/graphene/internal/cli"
)

const digest = "registry.example.com/perf@sha256:" +
	"0e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85"

type fixedBuilder struct {
	image string
	calls int
}

func (b *fixedBuilder) Build(_ context.Context, _ string) (string, error) {
	b.calls++

	return b.image, nil
}

func kube(t *testing.T) client.Client {
	t.Helper()

	sch := runtime.NewScheme()
	if err := v1.AddToScheme(sch); err != nil {
		t.Fatalf("схема не собралась: %v", err)
	}

	return fake.NewClientBuilder().WithScheme(sch).
		WithStatusSubresource(&v1.Pipeline{}, &v1.PipelineRevision{}, &v1.Run{}).Build()
}

// Ревизия держит дайджест, а не тег. Тег переставляют на другие байты, и
// «повторить прогон» перестало бы значить «выполнить тот же код».
func TestPushRecordsDigest(t *testing.T) {
	t.Parallel()

	kube := kube(t)
	builder := &fixedBuilder{image: digest}

	revision, err := cli.Push(t.Context(), cli.PushRequest{
		Kube:      kube,
		Builder:   builder,
		Namespace: "default",
		Pipeline:  "perf",
		Dir:       "./examples/perf",
	})
	if err != nil {
		t.Fatalf("push не прошёл: %v", err)
	}

	if revision.Spec.Image != digest {
		t.Fatalf("в ревизии %q, а не дайджест", revision.Spec.Image)
	}

	if err := revision.Spec.Validate(); err != nil {
		t.Fatalf("ревизия невалидна: %v", err)
	}

	var pipeline v1.Pipeline
	if err := kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "perf"}, &pipeline); err != nil {
		t.Fatalf("пайплайн не создан: %v", err)
	}

	if pipeline.Status.LatestRevision != revision.Name {
		t.Fatalf("последняя ревизия %q, а создана %q", pipeline.Status.LatestRevision, revision.Name)
	}
}

// Очередь у каждой ревизии своя: воркер старой ревизии не имеет права
// взять работу, предназначенную новой.
func TestPushGivesEveryRevisionItsOwnQueue(t *testing.T) {
	t.Parallel()

	kube := kube(t)

	other := "registry.example.com/perf@sha256:" +
		"1111111111111111111111111111111111111111111111111111111111111111"

	first, err := cli.Push(t.Context(), cli.PushRequest{
		Kube: kube, Builder: &fixedBuilder{image: digest},
		Namespace: "default", Pipeline: "perf", Dir: ".",
	})
	if err != nil {
		t.Fatalf("первый push: %v", err)
	}

	second, err := cli.Push(t.Context(), cli.PushRequest{
		Kube: kube, Builder: &fixedBuilder{image: other},
		Namespace: "default", Pipeline: "perf", Dir: ".",
	})
	if err != nil {
		t.Fatalf("второй push: %v", err)
	}

	if first.Spec.Queue == second.Spec.Queue {
		t.Fatalf("две ревизии делят очередь %q", first.Spec.Queue)
	}

	if first.Name == second.Name {
		t.Fatalf("две ревизии делят имя %q", first.Name)
	}
}

// Тот же образ — та же ревизия. Push дважды подряд не должен плодить
// записи: имя ревизии выводится из дайджеста.
func TestPushOfTheSameImageIsTheSameRevision(t *testing.T) {
	t.Parallel()

	kube := kube(t)

	request := cli.PushRequest{
		Kube: kube, Builder: &fixedBuilder{image: digest},
		Namespace: "default", Pipeline: "perf", Dir: ".",
	}

	first, err := cli.Push(t.Context(), request)
	if err != nil {
		t.Fatalf("первый push: %v", err)
	}

	second, err := cli.Push(t.Context(), request)
	if err != nil {
		t.Fatalf("повтор push: %v", err)
	}

	if first.Name != second.Name {
		t.Fatalf("тот же образ дал разные ревизии: %q и %q", first.Name, second.Name)
	}

	var list v1.PipelineRevisionList
	if err := kube.List(t.Context(), &list); err != nil {
		t.Fatalf("список ревизий: %v", err)
	}

	if len(list.Items) != 1 {
		t.Fatalf("ревизий %d, а образ один", len(list.Items))
	}
}

// Прогон прибит к ревизии, а не к пайплайну: иначе «повторить прогон»
// означало бы «выполнить то, что лежит там сейчас».
func TestStartRunPointsAtRevision(t *testing.T) {
	t.Parallel()

	kube := kube(t)

	revision, err := cli.Push(t.Context(), cli.PushRequest{
		Kube: kube, Builder: &fixedBuilder{image: digest},
		Namespace: "default", Pipeline: "perf", Dir: ".",
	})
	if err != nil {
		t.Fatalf("push: %v", err)
	}

	run, err := cli.Start(t.Context(), cli.StartRequest{
		Kube:      kube,
		Namespace: "default",
		Pipeline:  "perf",
		Params:    []byte(`{"nodes":3}`),
	})
	if err != nil {
		t.Fatalf("запуск: %v", err)
	}

	if run.Spec.RevisionRef.Name != revision.Name {
		t.Fatalf("прогон указывает на %q, а последняя ревизия %q", run.Spec.RevisionRef.Name, revision.Name)
	}

	if string(run.Spec.Params.Raw) != `{"nodes":3}` {
		t.Fatalf("параметры поехали: %s", run.Spec.Params.Raw)
	}
}

// Параметры больше предела отвергаются здесь, а не в кластере: человек,
// который их написал, ещё смотрит на терминал.
func TestStartRefusesOversizedParams(t *testing.T) {
	t.Parallel()

	kube := kube(t)

	if _, err := cli.Push(t.Context(), cli.PushRequest{
		Kube: kube, Builder: &fixedBuilder{image: digest},
		Namespace: "default", Pipeline: "perf", Dir: ".",
	}); err != nil {
		t.Fatalf("push: %v", err)
	}

	huge := make([]byte, v1.MaxParamsBytes+1)
	for i := range huge {
		huge[i] = 'x'
	}

	_, err := cli.Start(t.Context(), cli.StartRequest{
		Kube: kube, Namespace: "default", Pipeline: "perf", Params: huge,
	})
	if err == nil {
		t.Fatal("параметры больше предела приняты")
	}
}
