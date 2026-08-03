package agent

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// supervisor owns one process for as long as its record says it should
// exist: fetch the bytes, start them, watch them end, and — when the
// record asks for it — start them again.
//
// It runs on its own goroutine because the watch loop must not block. A
// blob fetch crosses a link and a process may run for days; a loop that
// waited for either would stop noticing everything else on the kernel.
type supervisor struct {
	agent      *Agent
	name       string
	generation uint64
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
			s.agent.setStatus(ctx, s.name, func(fields map[string]any) {
				fields["phase"] = PhaseFailed
				fields["error"] = err.Error()
			})
		default:
			s.agent.setStatus(ctx, s.name, func(fields map[string]any) {
				fields["phase"] = phaseFor(code)
				fields["exit_code"] = code
				delete(fields, "error")
			})
		}

		// never — the record asked for one run, and it happened. The
		// record stays: what became of it is the answer someone is
		// waiting to read.
		if s.spec.restart != restartAlways {
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
	s.agent.setStatus(ctx, s.name, func(fields map[string]any) {
		fields["phase"] = PhasePending
		fields["starts"] = starts
	})

	path, err := s.agent.Fetch.Fetch(ctx, s.spec.blob)
	if err != nil {
		return 0, fmt.Errorf("fetch %s: %w", s.spec.blob, err)
	}

	started, err := s.agent.Runner.Start(ctx, Spec{
		Path:    path,
		Args:    s.spec.args,
		Env:     s.spec.env,
		Process: s.name,
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

	s.agent.setStatus(ctx, s.name, func(fields map[string]any) {
		fields["phase"] = PhaseRunning
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

const restartAlways = "always"
