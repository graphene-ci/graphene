package operator_test

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/graphene-ci/graphene/internal/operator"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

func stand(until time.Time) *v1.Stand {
	return &v1.Stand{
		ObjectMeta: metav1.ObjectMeta{Name: "perf-42", Namespace: "default"},
		Spec: v1.StandSpec{
			Until:  metav1.NewTime(until),
			RunRef: v1.LocalRef{Name: "perf-42"},
			Reason: "деградация p99",
		},
	}
}

func reconcileStand(t *testing.T, kube client.Client, now time.Time) ctrl.Result {
	t.Helper()

	reconciler := operator.NewStandReconciler(kube)
	reconciler.Now = func() time.Time { return now }

	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "perf-42"}}

	result, err := reconciler.Reconcile(t.Context(), request)
	if err != nil {
		t.Fatalf("сверка не прошла: %v", err)
	}

	return result
}

func standExists(t *testing.T, kube client.Client) bool {
	t.Helper()

	var found v1.Stand

	err := kube.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "perf-42"}, &found)

	return err == nil
}

// Стенд, чьё время не вышло, стоит — и пересверка назначена на момент
// конца, потому что стенд, на который никто не смотрит, обязан истечь.
func TestStandStandsUntilItsTime(t *testing.T) {
	t.Parallel()

	now := time.Now()
	kube := fake.NewClientBuilder().WithScheme(machineScheme(t)).
		WithObjects(stand(now.Add(time.Hour))).Build()

	result := reconcileStand(t, kube, now)

	if !standExists(t, kube) {
		t.Fatal("время не вышло, а стенд снесён")
	}

	if result.RequeueAfter <= 0 {
		t.Fatal("пересверка не назначена — истечение некому заметить")
	}
}

// Время вышло — стенд уходит, и с ним уходит то, чем он владел.
//
// Ради этого стенду и позволено существовать: «оставить на сутки»
// разрешено только потому, что есть кто-то, кто уберёт на следующие.
func TestStandEndsWhenItsTimeIsUp(t *testing.T) {
	t.Parallel()

	now := time.Now()
	kube := fake.NewClientBuilder().WithScheme(machineScheme(t)).
		WithObjects(stand(now.Add(-time.Minute))).Build()

	reconcileStand(t, kube, now)

	if standExists(t, kube) {
		t.Fatal("время вышло, а стенд стоит: Keep стал вежливым способом течь")
	}
}
