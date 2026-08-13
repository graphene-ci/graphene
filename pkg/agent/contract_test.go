package agent_test

import (
	"encoding/json"
	"testing"

	"github.com/graphene-ci/graphene/pkg/agent"
)

// Аргументы activity ездят через JSON: их пишет SDK внутри воркфлоу, а
// читает системный воркер в другом процессе. Сырой манифест обязан
// доехать байт в байт — это чужой ресурс, и мы в него не заглядываем.
func TestApplyInputSurvivesJSON(t *testing.T) {
	t.Parallel()

	manifest := []byte(`{"apiVersion":"compute.yandex.crossplane.io/v1beta1",` +
		`"kind":"Instance","spec":{"cores":4,"userData":"#cloud-config\n"}}`)

	before := agent.ApplyInput{
		Name:     "node-0",
		Manifest: manifest,
		Owner: agent.OwnerRef{
			Namespace: "default",
			Name:      "perf-42",
			UID:       "0f3d5b2a-1c44-4a0e-9f2b-7f1d9c8e5a31",
		},
	}

	encoded, err := json.Marshal(before)
	if err != nil {
		t.Fatalf("не закодировалось: %v", err)
	}

	var after agent.ApplyInput
	if err := json.Unmarshal(encoded, &after); err != nil {
		t.Fatalf("не раскодировалось: %v", err)
	}

	if string(after.Manifest) != string(manifest) {
		t.Fatalf("манифест поехал:\nбыло: %s\nстало: %s", manifest, after.Manifest)
	}

	if after.Name != before.Name || after.Owner != before.Owner {
		t.Fatalf("остальное поехало: %+v", after)
	}
}

// Имя, под которым запись создана, — это имя внутри прогона, а не имя в
// кластере. Второй вызов Apply с тем же именем обязан находить ту же
// запись, поэтому имя в кластере выводится из прогона и памятки, а не
// придумывается заново.
func TestObjectNameIsDerivedFromRunAndMemo(t *testing.T) {
	t.Parallel()

	owner := agent.OwnerRef{Namespace: "default", Name: "perf-42"}

	first := agent.ObjectName(owner, "node-0")
	again := agent.ObjectName(owner, "node-0")
	other := agent.ObjectName(owner, "node-1")

	if first != again {
		t.Fatalf("одно имя дало разные записи: %q и %q", first, again)
	}

	if first == other {
		t.Fatalf("разные памятки дали одну запись: %q", first)
	}

	if len(first) > 253 {
		t.Fatalf("имя длиннее, чем kubernetes принимает: %d", len(first))
	}
}

// Имя должно оставаться правильным именем kubernetes, что бы человек ни
// написал в памятке: пайплайн — это код, и туда попадёт что угодно.
func TestObjectNameStaysValidForAnyMemo(t *testing.T) {
	t.Parallel()

	memos := []string{
		"node-0",
		"Node_0",
		"очень длинная памятка с пробелами",
		"a/b/c",
		"",
	}

	seen := make(map[string]string, len(memos))

	for _, memo := range memos {
		name := agent.ObjectName(agent.OwnerRef{Namespace: "default", Name: "perf-42"}, memo)
		if !agent.ValidName(name) {
			t.Errorf("памятка %q дала недопустимое имя %q", memo, name)
		}

		if before, ok := seen[name]; ok {
			t.Errorf("памятки %q и %q дали одно имя %q", before, memo, name)
		}

		seen[name] = memo
	}
}

func TestReadySignalSurvivesJSON(t *testing.T) {
	t.Parallel()

	before := agent.ReadySignal{
		Name:   "node-0",
		Ready:  false,
		Reason: "ждём адреса",
		Status: []byte(`{"atProvider":{"id":"epd123"}}`),
	}

	encoded, err := json.Marshal(before)
	if err != nil {
		t.Fatalf("не закодировалось: %v", err)
	}

	var after agent.ReadySignal
	if err := json.Unmarshal(encoded, &after); err != nil {
		t.Fatalf("не раскодировалось: %v", err)
	}

	if after.Name != before.Name || after.Ready != before.Ready ||
		after.Reason != before.Reason || string(after.Status) != string(before.Status) {
		t.Fatalf("сигнал поехал: %+v", after)
	}
}
