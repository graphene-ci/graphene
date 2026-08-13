package env_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/graphene-ci/graphene/internal/env"
)

// scripted answers a fixed sequence of statuses per component name, holding
// the last one once the sequence runs out.
type scripted struct {
	answers map[string][]env.Status
	calls   map[string]int
}

func (s *scripted) Status(_ context.Context, comp env.Component) (env.Status, error) {
	seq := s.answers[comp.Name]
	if len(seq) == 0 {
		return env.Status{}, errors.New("нет ответов для " + comp.Name)
	}

	idx := s.calls[comp.Name]
	if idx >= len(seq) {
		idx = len(seq) - 1
	}

	s.calls[comp.Name]++

	return seq[idx], nil
}

func components() []env.Component {
	return []env.Component{
		{Name: "temporal", Namespace: "temporal", Deployment: "temporal"},
		{Name: "crossplane", Namespace: "crossplane-system", Deployment: "crossplane"},
	}
}

func TestWaitPassesWhenEverythingIsReady(t *testing.T) {
	t.Parallel()

	probe := &scripted{
		answers: map[string][]env.Status{
			"temporal":   {{Ready: true}},
			"crossplane": {{Ready: true}},
		},
		calls: map[string]int{},
	}

	err := env.Wait(t.Context(), probe, components(), io.Discard, env.Options{})
	if err != nil {
		t.Fatalf("готовое окружение отвергнуто: %v", err)
	}
}

func TestWaitWithoutWaitingFailsOnFirstPass(t *testing.T) {
	t.Parallel()

	probe := &scripted{
		answers: map[string][]env.Status{
			"temporal":   {{Ready: true}},
			"crossplane": {{Ready: false, Reason: "0/1 реплик"}},
		},
		calls: map[string]int{},
	}

	err := env.Wait(t.Context(), probe, components(), io.Discard, env.Options{})
	if !errors.Is(err, env.ErrNotReady) {
		t.Fatalf("ожидали ErrNotReady, получили %v", err)
	}

	if probe.calls["crossplane"] != 1 {
		t.Fatalf("без --wait должен быть ровно один проход, было %d", probe.calls["crossplane"])
	}
}

func TestWaitPollsUntilReady(t *testing.T) {
	t.Parallel()

	probe := &scripted{
		answers: map[string][]env.Status{
			"temporal":   {{Ready: true}},
			"crossplane": {{Ready: false, Reason: "0/1 реплик"}, {Ready: false}, {Ready: true}},
		},
		calls: map[string]int{},
	}

	opts := env.Options{Wait: true, Every: time.Millisecond}

	err := env.Wait(t.Context(), probe, components(), io.Discard, opts)
	if err != nil {
		t.Fatalf("ожидание не дождалось: %v", err)
	}

	if probe.calls["crossplane"] != 3 {
		t.Fatalf("ожидали три опроса crossplane, было %d", probe.calls["crossplane"])
	}
}

func TestWaitStopsWhenContextIsDone(t *testing.T) {
	t.Parallel()

	probe := &scripted{
		answers: map[string][]env.Status{
			"temporal":   {{Ready: true}},
			"crossplane": {{Ready: false, Reason: "не поднялся"}},
		},
		calls: map[string]int{},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	err := env.Wait(ctx, probe, components(), io.Discard, env.Options{Wait: true, Every: time.Millisecond})
	if !errors.Is(err, env.ErrNotReady) {
		t.Fatalf("ожидали ErrNotReady по исчерпании времени, получили %v", err)
	}
}
