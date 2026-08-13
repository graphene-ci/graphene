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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/graphene-ci/graphene/pkg/agent"
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

	// Своя группа процессов — первый рубеж убийства по таймауту; почему
	// одного его мало, написано у killTree.
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
		killTree(cmd)
		<-done

		return 0, fmt.Errorf("не уложилось в %s: %w", timeout, context.DeadlineExceeded)
	}
}

// killTree kills the process and everything it started.
//
// The process group is not enough, and this was measured rather than
// assumed: `sh -c 'sleep 30 &'` puts the background child into a group of
// its own — that is what a shell does to protect background jobs from
// terminal signals — and a kill by group leaves it running. A machine that
// keeps such leftovers accumulates work nobody ordered and nobody will
// find.
//
// So: the group first, then every descendant found through /proc. The
// mechanism that does this properly is a cgroup per step, and it comes for
// free once the agent runs as a systemd unit — that is the right answer and
// it belongs with the installation work, not here.
func killTree(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}

	// Отрицательный pid — это группа.
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)

	for _, pid := range descendants(cmd.Process.Pid) {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}

	_ = cmd.Process.Kill()
}

// descendants lists every process below pid.
//
// Reading /proc is a snapshot and a snapshot can be stale: a process may
// have forked between the listing and the signal. The step is being killed
// either way, so what matters is that the common cases die, not that the
// scan is a proof.
func descendants(pid int) []int {
	children := childrenByParent()

	var found []int

	queue := []int{pid}
	for len(queue) > 0 {
		next := queue[0]
		queue = queue[1:]

		for _, child := range children[next] {
			found = append(found, child)
			queue = append(queue, child)
		}
	}

	return found
}

// childrenByParent reads /proc once and groups processes by their parent.
func childrenByParent() map[int][]int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}

	byParent := make(map[int][]int, len(entries))

	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}

		parent, ok := parentOf(pid)
		if !ok {
			continue
		}

		byParent[parent] = append(byParent[parent], pid)
	}

	return byParent
}

// parentOf reads a process's parent from /proc/<pid>/stat.
//
// The fields are positional and the second one is the command name in
// brackets, which may itself contain spaces and brackets. Everything after
// the LAST closing bracket splits safely: parent is the second field there.
func parentOf(pid int) (int, bool) {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, false
	}

	tail := strings.LastIndex(string(raw), ")")
	if tail < 0 {
		return 0, false
	}

	fields := strings.Fields(string(raw)[tail+1:])

	const parentField = 1
	if len(fields) <= parentField {
		return 0, false
	}

	parent, err := strconv.Atoi(fields[parentField])
	if err != nil {
		return 0, false
	}

	return parent, true
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
