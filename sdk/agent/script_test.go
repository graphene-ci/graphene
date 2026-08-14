package agent_test

import (
	"strings"
	"testing"

	"github.com/graphene-ci/graphene/sdk/agent"
)

func install() agent.Install {
	return agent.Install{
		Control:   "http://10.0.0.1:18080",
		Address:   "temporal.temporal.svc:7233",
		Namespace: "graphene",
		Records:   "default",
		Machine:   "perf-42-node-0",
		Token:     "секрет",
	}
}

// Скрипт обязан быть чистой функцией установки: та же установка,
// заказанная дважды, даёт те же байты. Иначе всё, что его несёт —
// user-data ВМ например, — перестаёт быть идемпотентным, и повтор
// activity пересоздаёт машину.
func TestScriptIsPure(t *testing.T) {
	t.Parallel()

	first := install()
	again := install()

	if first.Script() != again.Script() {
		t.Fatal("одна установка дала два разных скрипта")
	}
}

func TestScriptCarriesTheInstallation(t *testing.T) {
	t.Parallel()

	script := install().Script()

	for _, want := range []string{"perf-42-node-0", "temporal.temporal.svc:7233", "http://10.0.0.1:18080"} {
		if !strings.Contains(script, want) {
			t.Errorf("в скрипте нет %q", want)
		}
	}
}

// Кавычка в значении не должна разрывать скрипт: значения приходят из
// кода пайплайна, то есть там может быть что угодно.
func TestScriptSurvivesAQuote(t *testing.T) {
	t.Parallel()

	awkward := install()
	awkward.Machine = "node'0"

	script := awkward.Script()
	if strings.Contains(script, "GRAPHENE_MACHINE='node'0'\n") {
		t.Fatalf("кавычка разорвала присваивание:\n%s", script)
	}

	if !strings.Contains(script, `'\''`) {
		t.Fatal("кавычка не экранирована")
	}
}

// cloud-init — это тот же скрипт, а не второй его вариант: два бы
// разошлись, и сломанным оказался бы тот, которым пользуются реже.
func TestCloudInitCarriesTheSameScript(t *testing.T) {
	t.Parallel()

	cloud := install().CloudInit()
	if !strings.HasPrefix(cloud, "#cloud-config\n") {
		t.Fatal("это не cloud-config")
	}

	for line := range strings.SplitSeq(install().Script(), "\n") {
		if line == "" {
			continue
		}

		if !strings.Contains(cloud, "      "+line) {
			t.Fatalf("строка скрипта не доехала до cloud-init: %q", line)
		}
	}
}
