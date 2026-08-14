package pipeline_test

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	v1 "github.com/graphene-ci/graphene/api/v1"
	"github.com/graphene-ci/graphene/pkg/pipeline"
)

// Требования — это то, что пайплайн МОЖЕТ применить, а не то, что
// применит: какие виды понадобятся конкретному прогону, зависит от его
// параметров, и отказать заранее можно только по широкому ответу.
func TestRequirementsAreEveryKindTheSchemeKnows(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := v1.AddToScheme(scheme); err != nil {
		t.Fatalf("схема не собралась: %v", err)
	}

	kinds := pipeline.Requirements(scheme)

	found := map[string]bool{}
	for _, kind := range kinds {
		found[kind.Kind] = true

		if kind.Group != v1.Group || kind.Version != v1.Version {
			t.Errorf("вид %s из чужой группы: %+v", kind.Kind, kind)
		}
	}

	for _, want := range []string{"Machine", "Probe", "Run", "Pipeline", "PipelineRevision"} {
		if !found[want] {
			t.Errorf("вид %s не попал в требования", want)
		}
	}

	// Списки не применяют, и внутренние виды machinery — тоже.
	for _, kind := range kinds {
		if len(kind.Kind) > 4 && kind.Kind[len(kind.Kind)-4:] == "List" {
			t.Errorf("список попал в требования: %s", kind.Kind)
		}
	}
}

// Пайплайн без схемы — это пайплайн, который ничего не применяет.
// Он обязан работать, а не падать.
func TestRequirementsWithoutSchemeAreEmpty(t *testing.T) {
	t.Parallel()

	if kinds := pipeline.Requirements(nil); len(kinds) != 0 {
		t.Fatalf("без схемы требований %d", len(kinds))
	}
}

// Один и тот же набор видов даёт один и тот же список: порядок в схеме не
// определён, а требования сравнивают и пишут в запись.
func TestRequirementsAreStable(t *testing.T) {
	t.Parallel()

	build := func() []pipelineKind {
		scheme := runtime.NewScheme()
		if err := v1.AddToScheme(scheme); err != nil {
			t.Fatalf("схема не собралась: %v", err)
		}

		var kinds []pipelineKind
		for _, one := range pipeline.Requirements(scheme) {
			kinds = append(kinds, pipelineKind{one.Group, one.Version, one.Kind})
		}

		return kinds
	}

	first, again := build(), build()
	if len(first) != len(again) {
		t.Fatalf("два прохода дали %d и %d видов", len(first), len(again))
	}

	for i := range first {
		if first[i] != again[i] {
			t.Fatalf("порядок поехал на %d: %+v против %+v", i, first[i], again[i])
		}
	}
}

type pipelineKind struct{ group, version, kind string }
