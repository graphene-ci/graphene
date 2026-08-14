package operator_test

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/graphene-ci/graphene/internal/operator"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

func intent() *v1.MachineIntent {
	return &v1.MachineIntent{
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-1", Namespace: "default"},
		Spec: v1.MachineIntentSpec{
			Address: "10.0.0.7:22",
			User:    "ubuntu",
			Key:     v1.SecretRef{Name: "fleet"},
			HostKey: "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJ0ZKPTWaW2Vg1p3wJhLmC8ZQxLNRhkOZq4Xn6VTn1qE",
			Script:  "#!/bin/sh\necho ставим\n",
		},
	}
}

func keySecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "fleet", Namespace: "default"},
		Data:       map[string][]byte{"id_ed25519": []byte("ключ")},
	}
}

func reconcileIntent(t *testing.T, kube client.Client, install operator.Installer) {
	t.Helper()

	reconciler := operator.NewMachineIntentReconciler(kube, install)

	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "legacy-1"}}
	if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
		t.Fatalf("сверка не прошла: %v", err)
	}
}

func loadIntent(t *testing.T, kube client.Client) *v1.MachineIntent {
	t.Helper()

	var loaded v1.MachineIntent

	key := types.NamespacedName{Namespace: "default", Name: "legacy-1"}
	if err := kube.Get(t.Context(), key, &loaded); err != nil {
		t.Fatalf("установка не читается: %v", err)
	}

	return &loaded
}

func intentClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	return fake.NewClientBuilder().WithScheme(machineScheme(t)).
		WithObjects(objects...).WithStatusSubresource(&v1.MachineIntent{}).Build()
}

// Скрипт и ключ доезжают до машины, и после успеха запись готова.
func TestIntentInstallsOnce(t *testing.T) {
	t.Parallel()

	kube := intentClient(t, intent(), keySecret())

	trips := 0
	install := func(_ context.Context, req operator.InstallRequest) error {
		trips++

		if req.Address != "10.0.0.7:22" || req.User != "ubuntu" {
			return errors.New("пошли не туда")
		}

		if string(req.Key) != "ключ" || req.Script == "" {
			return errors.New("принесли не то")
		}

		return nil
	}

	reconcileIntent(t, kube, install)

	if !meta.IsStatusConditionTrue(loadIntent(t, kube).Status.Conditions, v1.ConditionReady) {
		t.Fatal("установка прошла, а запись не готова")
	}

	// Сверка идёт много раз; второй проход не имеет права ходить на
	// чужую машину снова.
	reconcileIntent(t, kube, install)

	if trips != 1 {
		t.Fatalf("ходили на машину %d раз вместо одного", trips)
	}
}

// Недостижимая машина обязана сказать об этом на своей же записи, а не
// молчать.
func TestUnreachableMachineSaysWhy(t *testing.T) {
	t.Parallel()

	kube := intentClient(t, intent(), keySecret())

	reconcileIntent(t, kube, func(context.Context, operator.InstallRequest) error {
		return errors.New("соединение отвергнуто")
	})

	after := loadIntent(t, kube)
	if meta.IsStatusConditionTrue(after.Status.Conditions, v1.ConditionReady) {
		t.Fatal("машина недостижима, а запись готова")
	}

	condition := meta.FindStatusCondition(after.Status.Conditions, v1.ConditionReady)
	if condition == nil || condition.Message == "" {
		t.Fatalf("отказ без причины: %+v", condition)
	}
}

// Нет ключа — не идём вообще: лезть на машину без ключа значит получить
// невнятный отказ вместо внятного.
func TestMissingKeyStopsBeforeTheTrip(t *testing.T) {
	t.Parallel()

	kube := intentClient(t, intent())

	trips := 0

	reconcileIntent(t, kube, func(context.Context, operator.InstallRequest) error {
		trips++

		return nil
	})

	if trips != 0 {
		t.Fatal("пошли на машину без ключа")
	}

	condition := meta.FindStatusCondition(loadIntent(t, kube).Status.Conditions, v1.ConditionReady)
	if condition == nil || condition.Status != metav1.ConditionFalse {
		t.Fatalf("нет ключа, а запись не отказала: %+v", condition)
	}
}

// Без ключа хоста не идём вовсе.
//
// Доверие при первом подключении — это то, что делает человек за
// терминалом. Здесь управляющий слой открывает на той стороне корневую
// оболочку и кормит её скриптом с токеном установки внутри: тот, кто
// ответил бы по этому адресу, получил бы и то, и другое.
func TestMissingHostKeyStopsBeforeTheTrip(t *testing.T) {
	t.Parallel()

	without := intent()
	without.Spec.HostKey = ""

	kube := intentClient(t, without, keySecret())

	trips := 0

	reconcileIntent(t, kube, func(_ context.Context, req operator.InstallRequest) error {
		trips++

		if req.HostKey == "" {
			return errors.New("пошли без ключа хоста")
		}

		return nil
	})

	if trips != 0 {
		t.Fatal("пошли на машину, не зная, кто там должен ответить")
	}
}
