package process

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/graphene-ci/graphene/internal/types/resource"
)

// restartDelay keeps a crash loop from becoming a busy loop. Backing off
// further with each failure is a policy, and policy lives in controllers
// above; this is the floor that stops a broken binary eating the machine.
const restartDelay = time.Second

// supervisor owns one process for as long as its record says it should
// exist: fetch the bytes, open the door, start, watch it end, and — when
// the record asks — start it again.
//
// It runs on its own goroutine because the watch must not block. A fetch
// crosses a link and a process may run for days; a loop that waited for
// either would stop noticing everything else on the kernel.
type supervisor struct {
	agent      *Agent
	id         resource.Id
	name       string
	generation resource.Generation
	spec       spec

	mu      sync.Mutex
	current Started
	stopped bool
	done    chan struct{}
}

func (s *supervisor) run(ctx context.Context) {
	defer close(s.done)

	for starts := int64(1); ; starts++ {
		if s.isStopped() {
			return
		}

		code, err := s.once(ctx, starts)

		switch {
		case s.isStopped() || ctx.Err() != nil:
			return
		case err != nil:
			s.agent.setStatus(ctx, s.id, map[string]any{
				phaseField:  PhaseFailed,
				errorField:  err.Error(),
				startsField: starts,
			})
		default:
			s.agent.setStatus(ctx, s.id, map[string]any{
				phaseField:    phaseFor(code),
				exitCodeField: int64(code),
				startsField:   starts,
			})
		}

		if !s.spec.resident() {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(restartDelay):
		}
	}
}

// once starts the process and waits for it to end.
func (s *supervisor) once(ctx context.Context, starts int64) (int, error) {
	s.agent.setStatus(ctx, s.id, map[string]any{
		phaseField:  PhasePending,
		startsField: starts,
	})

	path, err := s.agent.Fetch.Fetch(ctx, s.spec.blob)
	if err != nil {
		return 0, fmt.Errorf("fetch %s: %w", s.spec.blob, err)
	}

	// The door is opened BEFORE the process starts: one that came up and
	// found nothing to talk to would have to be written to retry, and
	// every SDK in every language would carry that retry forever.
	door, err := s.agent.Doors.Open(s.name)
	if err != nil {
		return 0, fmt.Errorf("open door: %w", err)
	}

	defer func() { _ = door.Close() }()

	started, err := s.agent.Runner.Start(ctx, Spec{
		Path: path,
		Args: s.spec.args,
		Env:  merge(s.spec.env, door.Env()),
		Name: s.name,
	})
	if err != nil {
		return 0, fmt.Errorf("start: %w", err)
	}

	s.mu.Lock()
	// Stop may have arrived while the bytes were still being fetched; the
	// process must not survive it just because it started a moment late.
	if s.stopped {
		s.mu.Unlock()

		_ = started.Stop()

		return 0, context.Canceled
	}

	s.current = started
	s.mu.Unlock()

	s.agent.setStatus(ctx, s.id, map[string]any{
		phaseField:  PhaseRunning,
		startsField: starts,
	})

	code, err := started.Wait()
	if err != nil {
		return code, fmt.Errorf("wait: %w", err)
	}

	return code, nil
}

func (s *supervisor) stop() {
	s.mu.Lock()
	s.stopped = true
	current := s.current
	s.current = nil
	s.mu.Unlock()

	if current != nil {
		_ = current.Stop()
	}

	<-s.done
}

func (s *supervisor) isStopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.stopped
}

func phaseFor(code int) string {
	if code == 0 {
		return PhaseExited
	}

	return PhaseFailed
}

// merge lays the door's variables over the record's. The record cannot
// override them: where a process talks and what it is called are facts
// about how it was started, not preferences somebody gets to state.
func merge(base, over map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(over))
	for name, value := range base {
		out[name] = value
	}

	for name, value := range over {
		out[name] = value
	}

	return out
}
