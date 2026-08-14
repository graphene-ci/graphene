// Command exec is the pipeline that proves the agent.
//
// It names a machine, hands out the script that puts an agent on one, and
// runs a command there. Nothing waits for the agent: the step goes into the
// installation's queue and sits there until somebody comes to read it.
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/graphene-ci/graphene/sdk/agent"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
	"github.com/graphene-ci/graphene/sdk/pipeline"
)

// errNoDigest means the upload came back without saying what it uploaded.
var errNoDigest = errors.New("артефакт уехал без дайджеста")

// Params is what this pipeline takes.
type Params struct {
	// Say is echoed on the machine, so that the test can recognize its
	// own output rather than any output.
	Say string `json:"say"`
}

func main() {
	if err := pipeline.Serve(Exec, pipeline.Scheme(v1.AddToScheme)); err != nil {
		fmt.Fprintln(os.Stderr, "exec:", err)
		os.Exit(1)
	}
}

// Exec runs one command on one machine.
func Exec(run pipeline.Run, params Params) error {
	node := pipeline.Install(run, "node-0")

	// Скрипт кладётся в статус прогона через результат: в M2 машину
	// поднимает проверка, а не пайплайн. С M3 это же значение уедет в
	// user-data ресурса провайдера, и здесь ничего не поменяется.
	said := pipeline.On(run, node).Command("say", agent.ExecInput{
		Argv: []string{"echo", params.Say},
	})

	if said.Code != 0 {
		return fmt.Errorf("команда вернула %d: %s", said.Code, said.Stderr)
	}

	// Отчёт: шаг оставил файл, файл уехал в хранилище прямо с машины.
	// Байты через управляющий слой не шли — он выдал дверь, которая
	// открывается один раз, и машина в неё прошла.
	report := pipeline.On(run, node).Command("report", agent.ExecInput{
		Script: `printf %s "$SAY" > /tmp/report.txt`,
		Env:    map[string]string{"SAY": params.Say},
	})

	if report.Code != 0 {
		return fmt.Errorf("отчёт не составился: %s", report.Stderr)
	}

	digest := pipeline.Artifact(run, node, "report", "/tmp/report.txt")
	if digest == "" {
		return errNoDigest
	}

	facts := pipeline.On(run, node).Facts()
	if facts["os"] == "" {
		return fmt.Errorf("машина не сказала, какая на ней система: %v", facts)
	}

	return nil
}
