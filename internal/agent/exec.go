// Package agent is what runs on a machine. It knows five things at most —
// in M2, two: run a command and report what the machine has.
//
// It is deliberately thin. docker.Install, pg.Install and hundreds like
// them are NOT agent code: they are ordinary libraries composed from these
// primitives on the pipeline's side. Were they agent code, every new
// convenience would mean releasing the agent and walking the whole fleet.
package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/graphene-ci/graphene/sdk/agent"
)

// ErrNothingToRun means the request said neither argv nor script.
var ErrNothingToRun = errors.New("не сказано, что выполнять")

// shell is what a script is fed to. /bin/sh rather than bash: it is on
// every machine, and a step that needs bash can ask for it by argv.
const shell = "/bin/sh"

// Exec runs one command and reports how it went.
//
// A non-zero exit code is NOT an error here. The command ran and said no,
// which is an answer the pipeline is entitled to look at — errors are for
// when we could not find out.
func Exec(ctx context.Context, req agent.ExecInput) (agent.ExecOutput, error) {
	cmd, err := command(ctx, req)
	if err != nil {
		return agent.ExecOutput{}, err
	}

	var stdout, stderr bytes.Buffer

	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Dir = req.Dir
	cmd.Env = environment(req.Env)

	// Своя группа процессов: убить по таймауту надо именно её, иначе
	// умрёт оболочка, а запущенное ею останется.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	code, err := run(cmd, timeoutOf(req))

	outText, outCut := agent.Tail(stdout.String(), agent.MaxOutputBytes)
	errText, errCut := agent.Tail(stderr.String(), agent.MaxOutputBytes)

	return agent.ExecOutput{
		Code:      code,
		Stdout:    outText,
		Stderr:    errText,
		Truncated: outCut || errCut,
	}, err
}

// command builds the process to start.
func command(ctx context.Context, req agent.ExecInput) (*exec.Cmd, error) {
	switch {
	case len(req.Argv) > 0:
		//nolint:gosec // выполнять названное — это и есть работа агента
		return exec.CommandContext(ctx, req.Argv[0], req.Argv[1:]...), nil
	case req.Script != "":
		//nolint:gosec // выполнять названное — это и есть работа агента
		return exec.CommandContext(ctx, shell, "-c", req.Script), nil
	default:
		return nil, ErrNothingToRun
	}
}

// run starts the process, waits for it, and kills its whole group if the
// time runs out.
func run(cmd *exec.Cmd, timeout time.Duration) (int, error) {
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("не запустилось: %w", err)
	}

	done := make(chan error, 1)

	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		return exitCode(cmd, err)
	case <-time.After(timeout):
		killGroup(cmd)
		<-done

		return 0, fmt.Errorf("не уложилось в %s: %w", timeout, context.DeadlineExceeded)
	}
}

// killGroup kills the command and everything it started.
//
// The process group is the whole mechanism, and that was measured twice —
// the second time correctly. A first look said a background child survives
// a group kill; it did not. The check was `kill(pid, 0)`, which succeeds
// for a ZOMBIE — a process already dead and merely not yet reaped by init
// after its parent died. The child was dead all along.
//
// What genuinely survives is a process that leaves the group on purpose:
// setsid, a daemon detaching itself. That is what such a process is FOR,
// and chasing it through /proc would be undoing somebody's intent rather
// than cleaning up after them.
func killGroup(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}

	// Отрицательный pid — это группа.
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
		_ = cmd.Process.Kill()
	}
}

// exitCode turns the end of a process into a number.
func exitCode(cmd *exec.Cmd, err error) (int, error) {
	if err == nil {
		return 0, nil
	}

	var exited *exec.ExitError
	if errors.As(err, &exited) {
		return exited.ExitCode(), nil
	}

	return cmd.ProcessState.ExitCode(), fmt.Errorf("выполнение сорвалось: %w", err)
}

func timeoutOf(req agent.ExecInput) time.Duration {
	if req.Timeout > 0 {
		return req.Timeout
	}

	return agent.DefaultExecTimeout
}

// environment adds what the step asked for to what the agent already has.
func environment(extra map[string]string) []string {
	if len(extra) == 0 {
		return nil
	}

	env := os.Environ()
	for key, value := range extra {
		env = append(env, key+"="+value)
	}

	return env
}
