package agent_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/graphene-ci/graphene/pkg/agent"
)

func TestExecSurvivesJSON(t *testing.T) {
	t.Parallel()

	before := agent.ExecInput{
		Argv:    []string{"psql", "-c", "select 1"},
		Env:     map[string]string{"PGUSER": "postgres"},
		Dir:     "/opt/pg",
		Timeout: 90 * time.Second,
	}

	encoded, err := json.Marshal(before)
	if err != nil {
		t.Fatalf("не закодировалось: %v", err)
	}

	var after agent.ExecInput
	if err := json.Unmarshal(encoded, &after); err != nil {
		t.Fatalf("не раскодировалось: %v", err)
	}

	// Аргумент с пробелом обязан остаться одним аргументом: ради этого
	// argv и существует рядом со script.
	if len(after.Argv) != 3 || after.Argv[2] != "select 1" {
		t.Fatalf("аргументы поехали: %q", after.Argv)
	}

	if after.Timeout != before.Timeout || after.Dir != before.Dir {
		t.Fatalf("остальное поехало: %+v", after)
	}
}

// Хвост, а не начало: когда команда падает, причина написана в конце.
func TestTailKeepsTheEnd(t *testing.T) {
	t.Parallel()

	text := strings.Repeat("a", 100) + "ВОТ ЗДЕСЬ ПРИЧИНА"

	cut, truncated := agent.Tail(text, 40)
	if !truncated {
		t.Fatal("обрезки не случилось")
	}

	if !strings.HasSuffix(cut, "ВОТ ЗДЕСЬ ПРИЧИНА") {
		t.Fatalf("конец потерялся: %q", cut)
	}

	if len(cut) > 40 {
		t.Fatalf("обрезали до %d байт, а предел 40", len(cut))
	}
}

func TestTailLeavesShortTextAlone(t *testing.T) {
	t.Parallel()

	cut, truncated := agent.Tail("коротко", agent.MaxOutputBytes)
	if truncated || cut != "коротко" {
		t.Fatalf("короткий вывод тронули: %q, обрезан=%v", cut, truncated)
	}
}

// Обрезка по байтам посреди символа даёт мусор в первом же символе —
// ровно там, куда человек смотрит раньше всего.
func TestTailDoesNotCutARuneInHalf(t *testing.T) {
	t.Parallel()

	text := strings.Repeat("щ", 50)

	for limit := 1; limit <= 20; limit++ {
		cut, _ := agent.Tail(text, limit)
		if !utf8.ValidString(cut) {
			t.Fatalf("предел %d дал невалидный UTF-8: %q", limit, cut)
		}
	}
}
