// Package env answers one question about the local control plane: is it
// ready to run anything. It knows which pieces must be up and how to wait
// for them; it does not install them — that is what `make up` is for.
package env

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// ErrNotReady is returned when at least one component is not ready. Callers
// compare against it; the message names which ones and why.
var ErrNotReady = errors.New("окружение не готово")

// defaultEvery is how often Wait re-asks when told to wait. Slow enough not
// to hammer the API server, fast enough that a person watching does not
// wonder whether it hung.
const defaultEvery = 2 * time.Second

// Component is one piece of the control plane whose readiness `graphene up`
// reports. Deployment is the workload whose availability stands for it.
type Component struct {
	Name       string
	Namespace  string
	Deployment string
}

// Status is one component's answer. Reason is filled only when it is not
// ready, and says what is missing in the words of whoever knows.
type Status struct {
	Ready  bool
	Reason string
}

// Probe answers for one component. It exists so that Wait can be tested
// without a cluster.
type Probe interface {
	Status(ctx context.Context, comp Component) (Status, error)
}

// Options controls how Wait behaves. The zero value makes one pass and
// reports what it found.
type Options struct {
	// Wait keeps re-asking until everything is ready or ctx is done.
	Wait bool
	// Every is the interval between passes. Zero means defaultEvery.
	Every time.Duration
}

// Wait reports readiness of every component, writing one line per component
// per pass to out. Without Options.Wait it makes a single pass and returns
// ErrNotReady if anything is missing. With it, it keeps asking until
// everything is ready or ctx is done.
func Wait(ctx context.Context, probe Probe, comps []Component, out io.Writer, opts Options) error {
	every := opts.Every
	if every == 0 {
		every = defaultEvery
	}

	for {
		pending, err := pass(ctx, probe, comps, out)
		if err != nil {
			return err
		}

		if len(pending) == 0 {
			return nil
		}

		if !opts.Wait {
			return fmt.Errorf("%w: %s", ErrNotReady, strings.Join(pending, ", "))
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %s", ErrNotReady, strings.Join(pending, ", "))
		case <-time.After(every):
		}
	}
}

// pass asks every component once and returns those that are not ready.
func pass(ctx context.Context, probe Probe, comps []Component, out io.Writer) ([]string, error) {
	pending := make([]string, 0, len(comps))

	for _, comp := range comps {
		status, err := probe.Status(ctx, comp)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", comp.Name, err)
		}

		if status.Ready {
			fmt.Fprintf(out, "%-12s готов\n", comp.Name)

			continue
		}

		fmt.Fprintf(out, "%-12s ждём: %s\n", comp.Name, status.Reason)
		pending = append(pending, describe(comp, status))
	}

	return pending, nil
}

// describe names one component and why it is not ready.
func describe(comp Component, status Status) string {
	if status.Reason == "" {
		return comp.Name
	}

	return comp.Name + " (" + status.Reason + ")"
}
