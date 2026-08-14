package pipeline_test

import (
	"errors"
	"testing"

	"go.temporal.io/sdk/testsuite"

	"github.com/graphene-ci/graphene/sdk/agent"
	"github.com/graphene-ci/graphene/sdk/pipeline"
)

// Вычисление возвращает ответ и выполняется ровно столько раз, сколько
// прогон его просил, — а не заново на каждом переигрывании истории.
func TestDoComputesOnceAndReturns(t *testing.T) {
	t.Parallel()

	calls := 0

	pipe := func(run pipeline.Run, _ params) error {
		got := pipeline.Do(run, "сложить", func() (int, error) {
			calls++

			return 2 + 2, nil
		})

		if got != 4 {
			return errors.New("вычисление вернуло не то")
		}

		return nil
	}

	flow, err := pipeline.Workflow(pipe)
	if err != nil {
		t.Fatalf("пайплайн не принят: %v", err)
	}

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(flow, agent.RunInput{Owner: owner()})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("воркфлоу упал: %v", err)
	}

	if calls != 1 {
		t.Fatalf("функция выполнена %d раз вместо одного", calls)
	}
}

// Отказ вычисления — отказ прогона, и в нём видно, какое именно
// вычисление отказало.
func TestDoFailureNamesTheComputation(t *testing.T) {
	t.Parallel()

	pipe := func(run pipeline.Run, _ params) error {
		pipeline.Do(run, "разобрать-отчёт", func() (string, error) {
			return "", errors.New("отчёт не разобрался")
		})

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
		t.Fatal("отказ вычисления не уронил прогон")
	}

	if !contains(err.Error(), "разобрать-отчёт") {
		t.Fatalf("отказ не называет вычисление: %v", err)
	}
}
