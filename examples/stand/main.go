// Command stand is the pipeline that proves ownership and teardown.
//
// It asks for a record, waits for it, and then does whatever the run was
// told: sleep long enough to be canceled, or leave a stand behind.
//
// Unlike the probe example it DOES tear down after itself, because that is
// the thing under test here.
package main

import (
	"fmt"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
	"github.com/graphene-ci/graphene/sdk/pipeline"
)

// Params is what this pipeline takes.
type Params struct {
	// After is how long the record takes to become ready.
	After metav1.Duration `json:"after"`
	// Sleep keeps the run going, so that something can cancel it.
	Sleep metav1.Duration `json:"sleep"`
	// Keep leaves the record standing after the run, for this long.
	Keep metav1.Duration `json:"keep"`
}

func main() {
	if err := pipeline.Serve(Stand, pipeline.Scheme(v1.AddToScheme)); err != nil {
		fmt.Fprintln(os.Stderr, "stand:", err)
		os.Exit(1)
	}
}

// Stand asks for a record and decides what becomes of it.
func Stand(run pipeline.Run, params Params) error {
	// Снос при любом исходе, включая отмену: на отвязанном контексте
	// внутри, потому что при отмене обычный контекст уже мёртв.
	defer run.Teardown()

	ref := pipeline.Apply(run, "probe-0", &v1.Probe{
		Spec: v1.ProbeSpec{After: params.After},
	})

	pipeline.Await(run, ref)

	if params.Keep.Duration > 0 {
		run.Keep(params.Keep.Duration, "проверка стенда")
	}

	if params.Sleep.Duration > 0 {
		run.Sleep(params.Sleep.Duration)
	}

	return nil
}
