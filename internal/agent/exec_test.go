package agent_test

import (
	"errors"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	internalagent "github.com/graphene-ci/graphene/internal/agent"
	"github.com/graphene-ci/graphene/sdk/agent"
)

// Ненулевой код возврата — это ОТВЕТ, а не отказ. Команда выполнилась и
// сказала «нет»; пайплайн имеет право на это посмотреть.
func TestExitCodeIsAnAnswer(t *testing.T) {
	t.Parallel()

	out, err := internalagent.Exec(t.Context(), agent.ExecInput{Script: "exit 7"})
	if err != nil {
		t.Fatalf("ненулевой код превратился в отказ: %v", err)
	}

	if out.Code != 7 {
		t.Fatalf("код возврата %d вместо 7", out.Code)
	}
}

func TestStreamsStayApart(t *testing.T) {
	t.Parallel()

	out, err := internalagent.Exec(t.Context(), agent.ExecInput{
		Script: "echo наружу; echo беда >&2",
	})
	if err != nil {
		t.Fatalf("не выполнилось: %v", err)
	}

	if !strings.Contains(out.Stdout, "наружу") || strings.Contains(out.Stdout, "беда") {
		t.Fatalf("stdout смешался: %q", out.Stdout)
	}

	if !strings.Contains(out.Stderr, "беда") {
		t.Fatalf("stderr потерялся: %q", out.Stderr)
	}
}

// Аргумент с пробелом остаётся одним аргументом: ради этого argv и
// существует рядом со script.
func TestArgvIsNotReparsed(t *testing.T) {
	t.Parallel()

	out, err := internalagent.Exec(t.Context(), agent.ExecInput{
		Argv: []string{"echo", "два слова"},
	})
	if err != nil {
		t.Fatalf("не выполнилось: %v", err)
	}

	if strings.TrimSpace(out.Stdout) != "два слова" {
		t.Fatalf("аргумент переразобрали: %q", out.Stdout)
	}
}

func TestEnvironmentAndDirectoryArrive(t *testing.T) {
	t.Parallel()

	out, err := internalagent.Exec(t.Context(), agent.ExecInput{
		Script: "echo $WHOM; pwd",
		Env:    map[string]string{"WHOM": "постгрес"},
		Dir:    "/tmp",
	})
	if err != nil {
		t.Fatalf("не выполнилось: %v", err)
	}

	if !strings.Contains(out.Stdout, "постгрес") {
		t.Fatalf("переменная не доехала: %q", out.Stdout)
	}

	if !strings.Contains(out.Stdout, "/tmp") {
		t.Fatalf("рабочий каталог не доехал: %q", out.Stdout)
	}
}

func TestTimeoutStops(t *testing.T) {
	t.Parallel()

	started := time.Now()

	_, err := internalagent.Exec(t.Context(), agent.ExecInput{
		Script:  "sleep 30",
		Timeout: 200 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("вышло время, а отказа нет")
	}

	if took := time.Since(started); took > 5*time.Second {
		t.Fatalf("убийство заняло %s", took)
	}
}

// Главное в таймауте: умирают ДЕТИ, а не только тот процесс, который мы
// запустили. Проверено отдельно и оказалось не тем, что ожидалось: shell
// уводит фоновое задание в собственную группу процессов, и убийство по
// группе оставляет его жить. Машина копила бы работу, которую никто не
// заказывал и никто не найдёт.
func TestTimeoutKillsTheChildrenToo(t *testing.T) {
	t.Parallel()

	out, err := internalagent.Exec(t.Context(), agent.ExecInput{
		Script:  "sleep 30 & echo $!; wait",
		Timeout: 300 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("вышло время, а отказа нет")
	}

	pid, convErr := strconv.Atoi(strings.TrimSpace(out.Stdout))
	if convErr != nil {
		t.Fatalf("не удалось узнать pid ребёнка из %q: %v", out.Stdout, convErr)
	}

	// Сигнал 0 ничего не делает, но отвечает, жив ли процесс.
	if err := syscall.Kill(pid, 0); err == nil {
		_ = syscall.Kill(pid, syscall.SIGKILL)

		t.Fatalf("ребёнок %d пережил таймаут", pid)
	}
}

func TestNothingToRunIsRefused(t *testing.T) {
	t.Parallel()

	if _, err := internalagent.Exec(t.Context(), agent.ExecInput{}); !errors.Is(err, internalagent.ErrNothingToRun) {
		t.Fatalf("пустой запрос принят: %v", err)
	}
}

// Длинный вывод обрезается на стороне агента, до отправки: он уезжает в
// историю Temporal и остаётся там на всю жизнь прогона.
func TestLongOutputIsCutBeforeItTravels(t *testing.T) {
	t.Parallel()

	out, err := internalagent.Exec(t.Context(), agent.ExecInput{
		Script: "head -c 200000 /dev/zero | tr '\\0' 'a'",
	})
	if err != nil {
		t.Fatalf("не выполнилось: %v", err)
	}

	if !out.Truncated {
		t.Fatal("вывод длиннее предела не помечен обрезанным")
	}

	if len(out.Stdout) > agent.MaxOutputBytes {
		t.Fatalf("вернулось %d байт при пределе %d", len(out.Stdout), agent.MaxOutputBytes)
	}
}
