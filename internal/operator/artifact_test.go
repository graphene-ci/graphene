package operator_test

import (
	"context"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/graphene-ci/graphene/internal/operator"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

// errStorageDown is a bucket that will not answer.
var errStorageDown = errors.New("хранилище не отвечает")

// removed remembers what was taken out of storage.
type removed struct {
	keys []string
	fail error
}

func (r *removed) Remove(_ context.Context, key string) error {
	if r.fail != nil {
		return r.fail
	}

	r.keys = append(r.keys, key)

	return nil
}

// artifact builds one that a run left behind an hour ago.
func artifact(until *metav1.Time) *v1.Artifact {
	born := metav1.NewTime(time.Now().Add(-time.Hour))

	return &v1.Artifact{
		ObjectMeta: metav1.ObjectMeta{
			Name: "perf-42-report", Namespace: "default",
			CreationTimestamp: born,
		},
		Spec: v1.ArtifactSpec{
			RunRef: v1.LocalRef{Name: "perf-42"},
			Name:   "report",
			Key:    "perf-42/report",
			Until:  until,
		},
	}
}

func request() ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "perf-42-report"}}
}

// Срок наследуется от пайплайна, когда сам артефакт молчит.
func TestArtifactInheritsPipelinePolicy(t *testing.T) {
	t.Parallel()

	run, revision := fixtures()
	pipe := &v1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "perf", Namespace: "default"},
		Spec:       v1.PipelineSpec{ArtifactRetention: &metav1.Duration{Duration: 48 * time.Hour}},
	}

	made := artifact(nil)
	kube := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(made, run, revision, pipe).Build()

	reconciler := operator.NewArtifactReconciler(kube, &removed{}, 7*24*time.Hour)

	if _, err := reconciler.Reconcile(t.Context(), request()); err != nil {
		t.Fatalf("сверка не прошла: %v", err)
	}

	var settled v1.Artifact
	if err := kube.Get(t.Context(), request().NamespacedName, &settled); err != nil {
		t.Fatalf("артефакт пропал: %v", err)
	}

	if settled.Spec.Until == nil {
		t.Fatal("срок не записан — истечение некому заметить")
	}

	want := made.CreationTimestamp.Add(48 * time.Hour).Truncate(time.Second)
	if !settled.Spec.Until.Truncate(time.Second).Equal(want) {
		t.Fatalf("срок взят не у пайплайна: %v вместо %v", settled.Spec.Until, want)
	}
}

// Пайплайн молчит — срок берётся у установки, и он конечен.
func TestArtifactFallsBackToInstallation(t *testing.T) {
	t.Parallel()

	run, revision := fixtures()
	pipe := &v1.Pipeline{ObjectMeta: metav1.ObjectMeta{Name: "perf", Namespace: "default"}}

	made := artifact(nil)
	kube := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(made, run, revision, pipe).Build()

	reconciler := operator.NewArtifactReconciler(kube, &removed{}, 3*time.Hour)

	if _, err := reconciler.Reconcile(t.Context(), request()); err != nil {
		t.Fatalf("сверка не прошла: %v", err)
	}

	var settled v1.Artifact
	if err := kube.Get(t.Context(), request().NamespacedName, &settled); err != nil {
		t.Fatalf("артефакт пропал: %v", err)
	}

	want := made.CreationTimestamp.Add(3 * time.Hour).Truncate(time.Second)
	if settled.Spec.Until == nil || !settled.Spec.Until.Truncate(time.Second).Equal(want) {
		t.Fatalf("срок взят не у установки: %v вместо %v", settled.Spec.Until, want)
	}
}

// Артефакт сказал сам за себя — никто его не переписывает, и пересверка
// назначена на его собственный срок.
func TestArtifactKeepsItsOwnUntil(t *testing.T) {
	t.Parallel()

	run, revision := fixtures()
	pipe := &v1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "perf", Namespace: "default"},
		Spec:       v1.PipelineSpec{ArtifactRetention: &metav1.Duration{Duration: time.Minute}},
	}

	own := metav1.NewTime(time.Now().Add(2 * time.Hour))
	made := artifact(&own)
	kube := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(made, run, revision, pipe).Build()

	reconciler := operator.NewArtifactReconciler(kube, &removed{}, time.Minute)

	result, err := reconciler.Reconcile(t.Context(), request())
	if err != nil {
		t.Fatalf("сверка не прошла: %v", err)
	}

	if result.RequeueAfter <= time.Hour {
		t.Fatalf("пересверка назначена не на срок артефакта: %v", result.RequeueAfter)
	}

	var kept v1.Artifact
	if err := kube.Get(t.Context(), request().NamespacedName, &kept); err != nil {
		t.Fatalf("артефакт убрали раньше срока: %v", err)
	}

	if !kept.Spec.Until.Truncate(time.Second).Equal(own.Truncate(time.Second)) {
		t.Fatal("чужая политика переписала срок, который артефакт назвал сам")
	}
}

// Срок вышел — уходят ОБЕ половины: запись без байтов указывает в пустоту,
// байты без записи — счёт, который никто не объяснит.
func TestArtifactTakesItsBytesWithIt(t *testing.T) {
	t.Parallel()

	past := metav1.NewTime(time.Now().Add(-time.Minute))
	made := artifact(&past)
	made.Finalizers = []string{v1.Group + "/bytes"}

	bucket := &removed{}
	kube := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(made).Build()

	reconciler := operator.NewArtifactReconciler(kube, bucket, time.Hour)

	// Первый заход помечает запись на удаление, второй — отпускает её,
	// сняв байты. Так же это выглядит и в кластере.
	if _, err := reconciler.Reconcile(t.Context(), request()); err != nil {
		t.Fatalf("сверка не прошла: %v", err)
	}

	if _, err := reconciler.Reconcile(t.Context(), request()); err != nil {
		t.Fatalf("вторая сверка не прошла: %v", err)
	}

	if len(bucket.keys) != 1 || bucket.keys[0] != "perf-42/report" {
		t.Fatalf("байты остались в хранилище: снято %v", bucket.keys)
	}

	var gone v1.Artifact
	if err := kube.Get(t.Context(), request().NamespacedName, &gone); err == nil {
		t.Fatal("запись пережила свой срок")
	}
}

// Байты не снялись — запись остаётся. Иначе мы бы забыли, что именно
// лежит в хранилище и почему за него платят.
func TestArtifactStaysWhileItsBytesDo(t *testing.T) {
	t.Parallel()

	made := artifact(nil)
	made.Finalizers = []string{v1.Group + "/bytes"}
	dying := metav1.Now()
	made.DeletionTimestamp = &dying

	bucket := &removed{fail: errStorageDown}
	kube := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(made).Build()

	reconciler := operator.NewArtifactReconciler(kube, bucket, time.Hour)

	if _, err := reconciler.Reconcile(t.Context(), request()); err == nil {
		t.Fatal("отказ хранилища сошёл за успех — запись ушла бы, а байты остались")
	}
}
