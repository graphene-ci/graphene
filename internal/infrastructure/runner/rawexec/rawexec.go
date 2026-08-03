// Package rawexec runs a process by executing the bytes directly.
//
// This is the floor, not a fallback. A bare VM has no container runtime,
// and the kernel has to be able to start something there — otherwise the
// first thing anyone wants to run is the thing that would have made
// running possible.
package rawexec

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/graphene-ci/graphene/internal/core/agent"
)

const (
	dirMode = 0o700
	// stopGrace is how long a process gets to end on its own after being
	// asked. Past it, the choice is between killing it and never
	// reclaiming the machine.
	stopGrace = 5 * time.Second
)

// Runner executes prepared files under a working directory per process.
type Runner struct {
	// Root holds one directory per process: its working directory and the
	// files its output is written to.
	Root string
}

func New(root string) *Runner { return &Runner{Root: root} }

// Start executes the file and returns as soon as it is running; whether
// it then works is the process's own business, reported through Wait.
func (r *Runner) Start(ctx context.Context, spec agent.Spec) (agent.Started, error) {
	dir := filepath.Join(r.Root, spec.Process)
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, fmt.Errorf("rawexec: working directory: %w", err)
	}

	stdout, stderr, err := openOutputs(dir)
	if err != nil {
		return nil, err
	}

	// Deliberately detached from the caller's context: a process lives as
	// long as its record says it should, and stopping it is a decision the
	// agent makes from that record and says through Stop. Tying it to a
	// context would kill it abruptly at the same moment the agent was
	// asking it politely to end.
	//nolint:gosec // running the bytes it was given IS this component's job
	cmd := exec.CommandContext(context.WithoutCancel(ctx), spec.Path, spec.Args...)
	cmd.Dir = dir
	cmd.Env = environ(spec.Env)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	// Its own process group, so stopping reaches whatever it forked. A
	// child that outlives its parent holds the machine's resources with
	// nobody left to account for it.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		closeBoth(stdout, stderr)

		return nil, fmt.Errorf("rawexec: start %s: %w", spec.Path, err)
	}

	proc := &process{cmd: cmd, done: make(chan struct{}), outputs: []*os.File{stdout, stderr}}
	go proc.reap()

	return proc, nil
}

func openOutputs(dir string) (*os.File, *os.File, error) {
	stdout, err := os.Create(filepath.Join(dir, "stdout"))
	if err != nil {
		return nil, nil, fmt.Errorf("rawexec: stdout: %w", err)
	}

	stderr, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		_ = stdout.Close()

		return nil, nil, fmt.Errorf("rawexec: stderr: %w", err)
	}

	return stdout, stderr, nil
}

func closeBoth(files ...*os.File) {
	for _, file := range files {
		_ = file.Close()
	}
}

// environ renders the environment. The process inherits NOTHING from the
// kernel: a kernel's environment holds its own token and its own paths,
// and a process that inherited them would start out holding credentials
// nobody gave it.
func environ(env map[string]string) []string {
	out := make([]string, 0, len(env))
	for name, value := range env {
		out = append(out, name+"="+value)
	}

	return out
}

type process struct {
	cmd     *exec.Cmd
	done    chan struct{}
	outputs []*os.File

	mu   sync.Mutex
	code int
	err  error

	stopOnce sync.Once
}

// reap waits once, so Wait and Stop can both be called, from anywhere, as
// often as they like.
func (p *process) reap() {
	err := p.cmd.Wait()

	p.mu.Lock()
	p.code = p.cmd.ProcessState.ExitCode()

	// A non-zero exit is an outcome, not an error: the process ran and
	// said what it thought. Only a failure to run at all is ours.
	var exit *exec.ExitError
	if err != nil && !errors.As(err, &exit) {
		p.err = err
	}

	p.mu.Unlock()

	closeBoth(p.outputs...)
	close(p.done)
}

func (p *process) Wait() (int, error) {
	<-p.done

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.err != nil {
		return p.code, fmt.Errorf("rawexec: %w", p.err)
	}

	return p.code, nil
}

// Stop asks the whole process group to end, and insists if it will not.
func (p *process) Stop() error {
	p.stopOnce.Do(func() { p.signal(syscall.SIGTERM) })

	select {
	case <-p.done:
		return nil
	case <-time.After(stopGrace):
	}

	p.signal(syscall.SIGKILL)
	<-p.done

	return nil
}

func (p *process) signal(sig syscall.Signal) {
	if p.cmd.Process == nil {
		return
	}

	// Negative pid = the group. Falling back to the process alone keeps a
	// stop working even where the group could not be set up.
	if err := syscall.Kill(-p.cmd.Process.Pid, sig); err != nil {
		_ = p.cmd.Process.Signal(sig)
	}
}
