package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/graphene-ci/graphene/api/v1"
	"github.com/graphene-ci/graphene/pkg/agent"
	"github.com/graphene-ci/graphene/pkg/pipeline"
)

type params struct {
	Nodes int `json:"nodes"`
}

func owner() agent.OwnerRef {
	return agent.OwnerRef{Namespace: "default", Name: "perf-42", UID: "0f3d5b2a"}
}

// Пайплайн просит запись, дожидается готовности и читает то, что мир в неё
// вписал. Это вся сшивка целиком, только вместо кластера — заглушка.
func TestApplyAndAwait(t *testing.T) {
	t.Parallel()

	pipe := func(run pipeline.Run, arg params) error {
		probe := pipeline.Apply(run, "probe-0", &v1.Probe{})
		ready := pipeline.Await(run, probe)

		if len(ready.Status.Conditions) == 0 {
			return errors.New("готовность пришла пустой")
		}

		if arg.Nodes != 3 {
			return errors.New("параметры не доехали")
		}

		return nil
	}

	flow, err := pipeline.Workflow(pipe, pipeline.Scheme(v1.AddToScheme))
	if err != nil {
		t.Fatalf("пайплайн не принят: %v", err)
	}

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()

	var applied agent.ApplyInput

	env.RegisterActivityWithOptions(
		func(_ context.Context, in agent.ApplyInput) (agent.ApplyOutput, error) {
			applied = in

			return agent.ApplyOutput{
				Ref:     agent.ObjectRef{APIVersion: "graphene-ci.dev/v1", Kind: "Probe", Name: "perf-42-probe-0"},
				Created: true,
			}, nil
		},
		activity.RegisterOptions{Name: agent.ActivityApply},
	)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(agent.SignalReady, agent.ReadySignal{
			Name:   "probe-0",
			Ready:  true,
			Status: []byte(`{"conditions":[{"type":"Ready","status":"True","reason":"Ok","lastTransitionTime":"2026-08-14T00:00:00Z","message":""}]}`),
		})
	}, time.Millisecond)

	env.ExecuteWorkflow(flow, agent.RunInput{Owner: owner(), Params: []byte(`{"nodes":3}`)})

	if !env.IsWorkflowCompleted() {
		t.Fatal("воркфлоу не завершился")
	}

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("воркфлоу упал: %v", err)
	}

	if applied.Name != "probe-0" || applied.Owner != owner() {
		t.Fatalf("activity получила не то: %+v", applied)
	}

	// Манифест обязан нести apiVersion и kind: сгенерированный тип их не
	// несёт, и без них это не запись, а безымянный объект.
	var manifest map[string]json.RawMessage
	if err := json.Unmarshal(applied.Manifest, &manifest); err != nil {
		t.Fatalf("манифест не разобрался: %v", err)
	}

	if got := string(manifest["apiVersion"]); got != `"graphene-ci.dev/v1"` {
		t.Fatalf("apiVersion в манифесте: %s", got)
	}

	if got := string(manifest["kind"]); got != `"Probe"` {
		t.Fatalf("kind в манифесте: %s", got)
	}
}

// Без схемы и без TypeMeta вид определить нечем, и это обязано быть
// внятным отказом, а не записью без apiVersion, о которой узнают в
// кластере.
func TestApplyRefusesUnknownKind(t *testing.T) {
	t.Parallel()

	pipe := func(run pipeline.Run, _ params) error {
		pipeline.Apply(run, "probe-0", &v1.Probe{})

		return nil
	}

	flow, err := pipeline.Workflow(pipe)
	if err != nil {
		t.Fatalf("пайплайн не принят: %v", err)
	}

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(flow, agent.RunInput{Owner: owner()})

	err = env.GetWorkflowError()
	if err == nil {
		t.Fatal("объект без вида обязан быть отвергнут")
	}

	if !contains(err.Error(), "pipeline.Scheme") {
		t.Fatalf("отказ не говорит, что делать: %v", err)
	}
}

// Пайплайн неверной формы отвергается при старте воркера, а не на первом
// прогоне: воркер, который ничего не сможет выполнить, обязан сказать об
// этом, пока на него ещё кто-то смотрит.
func TestWorkflowRefusesWrongShape(t *testing.T) {
	t.Parallel()

	wrong := []any{
		"не функция",
		func() error { return nil },
		func(pipeline.Run, params) {},
		func(int, params) error { return nil },
	}

	for _, fn := range wrong {
		if _, err := pipeline.Workflow(fn); !errors.Is(err, pipeline.ErrNotPipeline) {
			t.Errorf("%T принят как пайплайн: %v", fn, err)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) &&
		(haystack == needle || indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}

	return -1
}
