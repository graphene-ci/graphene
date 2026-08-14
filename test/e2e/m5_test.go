//go:build e2e

package e2e_test

import (
	"context"
	"os"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/graphene-ci/graphene/internal/cli"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

const claimPipeline = "claim"

// freeMachine finds a machine in the pool that nobody holds, and skips the
// test when the pool is empty — this check needs real iron with a real
// agent, and pretending otherwise would prove nothing.
func freeMachine(ctx context.Context, t *testing.T, kube client.Client) string {
	t.Helper()

	var machines v1.MachineList
	if err := kube.List(ctx, &machines, client.InNamespace(namespace)); err != nil {
		t.Fatalf("машины не читаются: %v", err)
	}

	for i := range machines.Items {
		machine := &machines.Items[i]
		if machine.Status.Claim != nil {
			continue
		}

		for _, condition := range machine.Status.Conditions {
			if condition.Type == v1.ConditionReady && condition.Status == "True" {
				return machine.Name
			}
		}
	}

	t.Skip("в парке нет ни одной живой машины — проверять аренду не на чем")

	return ""
}

func heldBy(ctx context.Context, t *testing.T, kube client.Client, machine string) string {
	t.Helper()

	var found v1.Machine
	if err := kube.Get(ctx, client.ObjectKey{Namespace: namespace, Name: machine}, &found); err != nil {
		return ""
	}

	if found.Status.Claim == nil {
		return ""
	}

	return found.Status.Claim.Name
}

// Два прогона, просящие одну машину, не получают её оба: один работает,
// другой ждёт — и дожидается, когда первый её отпустит.
func TestTwoRunsShareOneMachineInTurn(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), patience)
	defer cancel()

	kube := connect(t)
	machine := freeMachine(ctx, t, kube)

	_, err := cli.Push(ctx, cli.PushRequest{
		Kube:      kube,
		Builder:   cli.Ko{Path: tool("ko"), Repo: registry, Out: os.Stderr},
		Namespace: namespace,
		Pipeline:  claimPipeline,
		Dir:       "../../examples/claim",
	})
	if err != nil {
		t.Fatalf("push не прошёл: %v", err)
	}

	serve(ctx, t, kube, claimPipeline, "../../examples/claim")

	first, err := cli.Start(ctx, cli.StartRequest{
		Kube: kube, Namespace: namespace, Pipeline: claimPipeline, Params: []byte(`{"hold":"45s"}`),
	})
	if err != nil {
		t.Fatalf("первый прогон не создался: %v", err)
	}

	// Дать первому взять машину.
	time.Sleep(20 * time.Second)

	if holder := heldBy(ctx, t, kube, machine); holder != first.Name {
		t.Fatalf("машину держит %q, а брал её %q", holder, first.Name)
	}

	second, err := cli.Start(ctx, cli.StartRequest{
		Kube: kube, Namespace: namespace, Pipeline: claimPipeline, Params: []byte(`{"hold":"1s"}`),
	})
	if err != nil {
		t.Fatalf("второй прогон не создался: %v", err)
	}

	// Пока первый держит, второй не имеет права её забрать.
	time.Sleep(15 * time.Second)

	if holder := heldBy(ctx, t, kube, machine); holder != first.Name {
		t.Fatalf("второй отобрал машину у первого: держит %q", holder)
	}

	// Первый кончился — машина освободилась — второй дождался.
	if phase := await(ctx, t, kube, first.Name); phase != v1.RunSucceeded {
		t.Fatalf("первый завершился как %s", phase)
	}

	if phase := await(ctx, t, kube, second.Name); phase != v1.RunSucceeded {
		t.Fatalf("второй не дождался машины: %s", phase)
	}

	// И после обоих машина снова ничья.
	free := time.After(2 * time.Minute)

	for {
		if heldBy(ctx, t, kube, machine) == "" {
			return
		}

		select {
		case <-free:
			t.Fatal("оба прогона кончились, а машина всё занята")
		case <-time.After(3 * time.Second):
		}
	}
}
