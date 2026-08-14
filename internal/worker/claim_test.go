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

	"github.com/graphene-ci/graphene/internal/kube"
	"github.com/graphene-ci/graphene/internal/worker"
	"github.com/graphene-ci/graphene/sdk/agent"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

func machinesGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: v1.Group, Version: v1.Version, Resource: "machines"}
}

// pool builds n ready machines, optionally carrying labels and taints.
func pool(names []string, labels map[string]any, taints []any) []runtime.Object {
	made := make([]runtime.Object, 0, len(names))

	for _, name := range names {
		spec := map[string]any{}
		if taints != nil {
			spec["taints"] = taints
		}

		made = append(made, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": v1.GroupVersion.String(),
			"kind":       "Machine",
			"metadata":   map[string]any{"name": name, "namespace": "default", "labels": labels},
			"spec":       spec,
			"status": map[string]any{
				"conditions": []any{map[string]any{
					"type": "Ready", "status": "True", "reason": "AgentAnswering",
					"lastTransitionTime": "2026-08-14T00:00:00Z", "message": "",
				}},
			},
		}})
	}

	return made
}

func claimer(t *testing.T, objects ...runtime.Object) (*worker.Applier, *dynamicfake.FakeDynamicClient) {
	t.Helper()

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{machinesGVR(): "MachineList"}, objects...)

	resolve := func(_ context.Context, _ schema.GroupVersionKind) (schema.GroupVersionResource, bool, error) {
		return machinesGVR(), true, nil
	}

	return worker.NewApplier(client, kube.Resolver(resolve)), client
}

func ask(run string, count int, match agent.Match) agent.ClaimInput {
	return agent.ClaimInput{
		Owner: agent.OwnerRef{Namespace: "default", Name: run, UID: "uid-" + run},
		Memo:  "pool", Count: count, Match: match,
	}
}

func claimOf(t *testing.T, client *dynamicfake.FakeDynamicClient, name string) *v1.ClaimRef {
	t.Helper()

	record, err := client.Resource(machinesGVR()).Namespace("default").Get(t.Context(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("машина %s не читается: %v", name, err)
	}

	var machine v1.Machine
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(record.Object, &machine); err != nil {
		t.Fatalf("машина не разобралась: %v", err)
	}

	return machine.Status.Claim
}

// Двое не берут одну машину: второй видит, что она занята, и уходит ни
// с чем — а не делит её с первым.
func TestTwoRunsDoNotTakeOneMachine(t *testing.T) {
	t.Parallel()

	applier, client := claimer(t, pool([]string{"node-a"}, nil, nil)...)

	first, err := applier.Claim(t.Context(), ask("perf-1", 1, agent.Match{}))
	if err != nil {
		t.Fatalf("первый не взял: %v", err)
	}

	if len(first.Machines) != 1 {
		t.Fatalf("первый взял %d машин", len(first.Machines))
	}

	_, err = applier.Claim(t.Context(), ask("perf-2", 1, agent.Match{}))
	if !errors.Is(err, worker.ErrNotEnough) {
		t.Fatalf("второй тоже взял занятую: %v", err)
	}

	if held := claimOf(t, client, "node-a"); held == nil || held.Name != "perf-1" {
		t.Fatalf("машину держит не первый: %+v", held)
	}
}

// Всё или ничего: не хватило — отпускаем взятое. Держать половину значит
// занять парк и не сделать работу.
func TestShortfallReleasesEverything(t *testing.T) {
	t.Parallel()

	applier, client := claimer(t, pool([]string{"node-a", "node-b"}, nil, nil)...)

	_, err := applier.Claim(t.Context(), ask("perf-1", 3, agent.Match{}))
	if !errors.Is(err, worker.ErrNotEnough) {
		t.Fatalf("трёх не было, а отказа нет: %v", err)
	}

	for _, name := range []string{"node-a", "node-b"} {
		if held := claimOf(t, client, name); held != nil {
			t.Fatalf("машина %s осталась занятой после неудачи: %+v", name, held)
		}
	}
}

// Два прогона по три машины на пяти: один взял, другому не хватило — и
// захваченного пополам ни у кого нет.
func TestHalfClaimsCannotHappen(t *testing.T) {
	t.Parallel()

	names := []string{"node-a", "node-b", "node-c", "node-d", "node-e"}
	applier, client := claimer(t, pool(names, nil, nil)...)

	first, err := applier.Claim(t.Context(), ask("perf-1", 3, agent.Match{}))
	if err != nil {
		t.Fatalf("первый не взял: %v", err)
	}

	_, err = applier.Claim(t.Context(), ask("perf-2", 3, agent.Match{}))
	if !errors.Is(err, worker.ErrNotEnough) {
		t.Fatalf("второму хватило пяти минус три: %v", err)
	}

	held := 0

	for _, name := range names {
		claim := claimOf(t, client, name)
		if claim == nil {
			continue
		}

		held++

		if claim.Name != "perf-1" {
			t.Fatalf("машину %s держит %s, а взял её первый", name, claim.Name)
		}
	}

	if held != len(first.Machines) {
		t.Fatalf("занято %d машин, а взято %d", held, len(first.Machines))
	}
}

// Выбор по меткам: берётся подходящая, не берётся неподходящая.
func TestClaimPicksByLabels(t *testing.T) {
	t.Parallel()

	withDocker := pool([]string{"node-a"}, map[string]any{"graphene-ci.dev/fact-docker": "27.3.1"}, nil)
	without := pool([]string{"node-b"}, nil, nil)

	applier, client := claimer(t, append(withDocker, without...)...)

	out, err := applier.Claim(t.Context(), ask("perf-1", 1, agent.Match{
		Labels: map[string]string{"graphene-ci.dev/fact-docker": "27.3.1"},
	}))
	if err != nil {
		t.Fatalf("подходящая не взялась: %v", err)
	}

	if len(out.Machines) != 1 || out.Machines[0] != "node-a" {
		t.Fatalf("взяли не ту: %v", out.Machines)
	}

	if claimOf(t, client, "node-b") != nil {
		t.Fatalf("взяли машину без докера")
	}
}

// Отталкивание: машина с taint не достаётся тому, кто его не терпит, —
// так человек забирает машину, не убирая её из системы.
func TestTaintKeepsWorkAway(t *testing.T) {
	t.Parallel()

	taints := []any{map[string]any{"key": "dedicated", "value": "perf", "effect": "NoSchedule"}}
	applier, _ := claimer(t, pool([]string{"node-a"}, nil, taints)...)

	if _, err := applier.Claim(t.Context(), ask("perf-1", 1, agent.Match{})); !errors.Is(err, worker.ErrNotEnough) {
		t.Fatalf("машина с taint досталась тому, кто его не терпит: %v", err)
	}

	out, err := applier.Claim(t.Context(), ask("perf-2", 1, agent.Match{Tolerate: []string{"dedicated=perf"}}))
	if err != nil {
		t.Fatalf("терпящий не взял: %v", err)
	}

	if len(out.Machines) != 1 {
		t.Fatalf("терпящий взял %d машин", len(out.Machines))
	}
}
