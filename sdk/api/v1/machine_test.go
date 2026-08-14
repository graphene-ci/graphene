package v1_test

import (
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/graphene-ci/graphene/sdk/agent"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

func TestSchemeKnowsMachine(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := v1.AddToScheme(scheme); err != nil {
		t.Fatalf("схема не собралась: %v", err)
	}

	for _, kind := range []string{"Machine", "MachineList"} {
		if !scheme.Recognizes(v1.GroupVersion.WithKind(kind)) {
			t.Errorf("вид %s не зарегистрирован", kind)
		}
	}
}

// Факты пишет агент, и это произвольные строки: версия ядра, версия
// докера, что угодно. Они обязаны доезжать без потерь.
func TestMachineFactsSurviveJSON(t *testing.T) {
	t.Parallel()

	before := v1.Machine{
		ObjectMeta: metav1.ObjectMeta{Name: "node-0", Namespace: "default"},
		Spec: v1.MachineSpec{
			Taints: []v1.Taint{{Key: "dedicated", Value: "perf", Effect: v1.TaintNoSchedule}},
		},
		Status: v1.MachineStatus{
			Queue: agent.InstallationQueue("perf-42-node-0"),
			Facts: map[string]string{
				"os":     "linux",
				"arch":   "amd64",
				"kernel": "6.8.0-45-generic",
				"docker": "27.3.1",
			},
		},
	}

	encoded, err := json.Marshal(before)
	if err != nil {
		t.Fatalf("не закодировалось: %v", err)
	}

	var after v1.Machine
	if err := json.Unmarshal(encoded, &after); err != nil {
		t.Fatalf("не раскодировалось: %v", err)
	}

	for key, want := range before.Status.Facts {
		if after.Status.Facts[key] != want {
			t.Errorf("факт %s поехал: %q вместо %q", key, after.Status.Facts[key], want)
		}
	}

	if after.Status.Queue != before.Status.Queue {
		t.Fatalf("очередь поехала: %q", after.Status.Queue)
	}

	if len(after.Spec.Taints) != 1 || after.Spec.Taints[0].Key != "dedicated" {
		t.Fatalf("метки-отталкивания поехали: %+v", after.Spec.Taints)
	}
}

// Очередь принадлежит УСТАНОВКЕ, а не машине. Переустановка агента на том
// же железе обязана дать новую очередь: иначе шаг, поставленный в очередь
// прежней установки, доедет до зомби, а новый агент будет ждать работы,
// которая ушла не туда.
func TestQueueBelongsToInstallation(t *testing.T) {
	t.Parallel()

	first := agent.InstallationQueue("perf-42-node-0")
	again := agent.InstallationQueue("perf-42-node-0")
	other := agent.InstallationQueue("perf-43-node-0")

	if first != again {
		t.Fatalf("одна установка дала две очереди: %q и %q", first, again)
	}

	if first == other {
		t.Fatalf("две установки делят очередь %q", first)
	}

	if first == "" {
		t.Fatal("очередь пуста")
	}
}
