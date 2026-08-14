// Command claim is the pipeline that proves the pool.
//
// It asks for a machine by description rather than by name, holds it for a
// while, and lets it go. Two of these running at once is the whole of the
// milestone: one gets the machine, the other waits.
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
	// Hold is how long to keep the machine before letting it go.
	Hold metav1.Duration `json:"hold"`
	// Os is what the machine must be, as a fact turned label.
	Os string `json:"os"`
}

func main() {
	if err := pipeline.Serve(Claim, pipeline.Scheme(v1.AddToScheme)); err != nil {
		fmt.Fprintln(os.Stderr, "claim:", err)
		os.Exit(1)
	}
}

// Claim takes one machine out of the pool and holds it.
func Claim(run pipeline.Run, params Params) error {
	// Освобождение — часть сноса: захват принадлежит прогону, и прогон
	// кончился — машина свободна. Здесь ничего отпускать руками не надо.
	defer run.Teardown()

	system := params.Os
	if system == "" {
		system = "linux"
	}

	nodes := pipeline.Claim(run, "pool", 1, pipeline.Match{
		Labels: map[string]string{"graphene-ci.dev/fact-os": system},
	})

	if len(nodes) != 1 {
		return fmt.Errorf("просили одну машину, дали %d", len(nodes))
	}

	if params.Hold.Duration > 0 {
		run.Sleep(params.Hold.Duration)
	}

	return nil
}
