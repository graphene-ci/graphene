package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

// watchEvery is how often the run's record is re-read. A run is minutes to
// hours long; a second between looks is not a cost anyone can measure.
const watchEvery = time.Second

// WatchRequest is one follow.
type WatchRequest struct {
	Kube      client.Client
	Namespace string
	Name      string
	Out       io.Writer
	// Every overrides the interval between looks. Zero means watchEvery.
	Every time.Duration
}

// Watch follows a run until it finishes and reports its final phase.
//
// It reads the record rather than the workflow: the record is what the
// system promises, and anything a person can see here they can also see
// with kubectl. A CLI that knew more than the records would be a second
// source of truth.
func Watch(ctx context.Context, req WatchRequest) (v1.RunPhase, error) {
	every := req.Every
	if every == 0 {
		every = watchEvery
	}

	var last v1.RunPhase

	for {
		var run v1.Run

		key := client.ObjectKey{Namespace: req.Namespace, Name: req.Name}
		if err := req.Kube.Get(ctx, key, &run); err != nil {
			return "", fmt.Errorf("прогон не читается: %w", err)
		}

		if run.Status.Phase != last {
			last = run.Status.Phase
			report(req.Out, &run)
		}

		if run.Status.Phase.Terminal() {
			return run.Status.Phase, nil
		}

		select {
		case <-ctx.Done():
			return run.Status.Phase, fmt.Errorf("слежение прервано: %w", ctx.Err())
		case <-time.After(every):
		}
	}
}

func report(out io.Writer, run *v1.Run) {
	if out == nil {
		return
	}

	phase := run.Status.Phase
	if phase == "" {
		phase = v1.RunPending
	}

	if run.Status.Reason == "" {
		fmt.Fprintf(out, "%-10s %s\n", run.Name, phase)

		return
	}

	fmt.Fprintf(out, "%-10s %s: %s\n", run.Name, phase, run.Status.Reason)
}
