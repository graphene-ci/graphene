//go:build e2e

package e2e_test

import (
	"context"
	"os"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/graphene-ci/graphene/internal/cli"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

const standPipeline = "stand"

func pushStand(ctx context.Context, t *testing.T, kube client.Client) {
	t.Helper()

	_, err := cli.Push(ctx, cli.PushRequest{
		Kube:      kube,
		Builder:   cli.Ko{Path: tool("ko"), Repo: registry, Out: os.Stderr},
		Namespace: namespace,
		Pipeline:  standPipeline,
		Dir:       "../../examples/stand",
	})
	if err != nil {
		t.Fatalf("push не прошёл: %v", err)
	}
}

func startStand(ctx context.Context, t *testing.T, kube client.Client, params string) *v1.Run {
	t.Helper()

	run, err := cli.Start(ctx, cli.StartRequest{
		Kube: kube, Namespace: namespace, Pipeline: standPipeline, Params: []byte(params),
	})
	if err != nil {
		t.Fatalf("прогон не создался: %v", err)
	}

	return run
}

// probeGone waits until the record the run made is really gone.
func probeGone(ctx context.Context, t *testing.T, kube client.Client, run string) bool {
	t.Helper()

	deadline := time.After(2 * time.Minute)

	for {
		if probesOf(ctx, t, kube, run) == 0 {
			return true
		}

		select {
		case <-deadline:
			return false
		case <-time.After(2 * time.Second):
		}
	}
}

// Отмена — это не убийство: отменённый пайплайн получает возможность
// прибрать за собой, и запись, которую он создал, исчезает.
func TestCancelTearsDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), patience)
	defer cancel()

	kube := connect(t)
	pushStand(ctx, t, kube)
	serve(ctx, t, kube, standPipeline, "../../examples/stand")

	run := startStand(ctx, t, kube, `{"after":"1s","sleep":"10m"}`)

	// Дать записи появиться и прогону дойти до сна.
	time.Sleep(20 * time.Second)

	if probesOf(ctx, t, kube, run.Name) != 1 {
		t.Fatal("запись не создана — отменять нечего")
	}

	if err := cli.Cancel(ctx, kube, namespace, run.Name); err != nil {
		t.Fatalf("отмена не запрошена: %v", err)
	}

	if phase := await(ctx, t, kube, run.Name); phase != v1.RunCanceled {
		t.Fatalf("прогон завершился как %s, а не отменён", phase)
	}

	if !probeGone(ctx, t, kube, run.Name) {
		t.Fatal("прогон отменён, а запись осталась: снос не выполнился")
	}
}

// Keep оставляет стенд, и он переживает прогон. Это не отсрочка сноса:
// записи меняют владельца, и у нового владельца свой конец.
func TestKeepLeavesAStand(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), patience)
	defer cancel()

	kube := connect(t)
	pushStand(ctx, t, kube)
	serve(ctx, t, kube, standPipeline, "../../examples/stand")

	run := startStand(ctx, t, kube, `{"after":"1s","keep":"1h"}`)

	if phase := await(ctx, t, kube, run.Name); phase != v1.RunSucceeded {
		t.Fatalf("прогон завершился как %s", phase)
	}

	var stand v1.Stand

	key := client.ObjectKey{Namespace: namespace, Name: run.Name}
	if err := kube.Get(ctx, key, &stand); err != nil {
		t.Fatalf("стенд не оставлен: %v", err)
	}

	if stand.Spec.Until.IsZero() {
		t.Fatal("у стенда нет конца — это и есть вечная аренда")
	}

	// Запись пережила прогон и принадлежит теперь стенду.
	var probes v1.ProbeList
	if err := kube.List(ctx, &probes, client.MatchingLabels{"graphene-ci.dev/stand": run.Name}); err != nil {
		t.Fatalf("записи стенда не читаются: %v", err)
	}

	if len(probes.Items) != 1 {
		t.Fatalf("стенду принадлежит %d записей вместо одной", len(probes.Items))
	}

	// Снос стенда уносит и её — тем же сборщиком, что унёс бы за прогоном.
	if err := kube.Delete(ctx, &stand); err != nil {
		t.Fatalf("стенд не снёсся: %v", err)
	}

	gone := time.After(time.Minute)

	for {
		var left v1.ProbeList
		if err := kube.List(ctx, &left, client.MatchingLabels{"graphene-ci.dev/stand": run.Name}); err == nil {
			if len(left.Items) == 0 {
				return
			}
		}

		select {
		case <-gone:
			t.Fatal("стенд снесён, а его записи остались")
		case <-time.After(2 * time.Second):
		}
	}
}

// Удаление записи прогона уносит с собой то, что он создал.
func TestDeletingARunTakesItsRecords(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), patience)
	defer cancel()

	kube := connect(t)
	pushStand(ctx, t, kube)
	serve(ctx, t, kube, standPipeline, "../../examples/stand")

	run := startStand(ctx, t, kube, `{"after":"1s","sleep":"10m"}`)

	time.Sleep(20 * time.Second)

	if err := kube.Delete(ctx, run); err != nil {
		t.Fatalf("прогон не удалился: %v", err)
	}

	if !probeGone(ctx, t, kube, run.Name) {
		t.Fatal("прогон удалён, а записи остались")
	}

	// Финализатор снимается на следующей сверке после того, как убирать
	// стало нечего: облако удаляет в своём темпе, и «записей нет» мы
	// узнаём не в тот же миг.
	key := client.ObjectKey{Namespace: namespace, Name: run.Name}
	deadline := time.After(2 * time.Minute)

	for {
		var gone v1.Run
		if err := kube.Get(ctx, key, &gone); apierrors.IsNotFound(err) {
			return
		}

		select {
		case <-deadline:
			t.Fatal("записи убраны, а финализатор всё держит прогон")
		case <-time.After(2 * time.Second):
		}
	}
}
