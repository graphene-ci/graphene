// Command probe is the smallest pipeline there is: it asks for one record
// and waits for it to become ready.
//
// It exists to check the wiring — record created, workflow woken by the
// readiness signal, run finished — without a cloud provider or an agent in
// the way. When something breaks later, this answers "is it the wiring or
// is it the provider" in one step.
//
// It deliberately does NOT tear down what it made: M1 is about the wiring,
// and the run's records are what the check looks at afterwards. Teardown
// gets its own proof at M4.
package main

import (
	"fmt"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
	"github.com/graphene-ci/graphene/sdk/pipeline"
)

// Params is what this pipeline takes.
//
// metav1.Duration rather than time.Duration: it reads and writes as "2s"
// in JSON, which is what a person types on the command line, while Go's
// own duration would arrive as a count of nanoseconds.
type Params struct {
	// After is how long the probe should take to become ready.
	After metav1.Duration `json:"after"`
}

func main() {
	// pipeline.Scheme is what tells the SDK the kinds this pipeline
	// applies. A generated type carries no apiVersion and no kind of its
	// own, and Go has no global registry to look them up in — so the one
	// place that knows is the program that imported the package.
	err := pipeline.Serve(Probe, pipeline.Scheme(v1.AddToScheme))
	if err != nil {
		fmt.Fprintln(os.Stderr, "probe:", err)
		os.Exit(1)
	}
}

// Probe asks for one record and waits for it.
func Probe(run pipeline.Run, params Params) error {
	after := params.After.Duration
	if after == 0 {
		after = time.Second
	}

	ref := pipeline.Apply(run, "probe-0", &v1.Probe{
		Spec: v1.ProbeSpec{After: metav1.Duration{Duration: after}},
	})

	pipeline.Await(run, ref)

	return nil
}
