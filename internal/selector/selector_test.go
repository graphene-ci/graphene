package selector

import (
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)

func compile(t *testing.T, in string) string {
	t.Helper()
	q, err := Parse(in)
	if err != nil {
		t.Fatalf("Parse(%q): %v", in, err)
	}
	out, err := Compile(q, now)
	if err != nil {
		t.Fatalf("Compile(%q): %v", in, err)
	}
	return out
}

func compileErr(t *testing.T, in string) string {
	t.Helper()
	q, err := Parse(in)
	if err != nil {
		return err.Error()
	}
	if _, err := Compile(q, now); err != nil {
		return err.Error()
	}
	t.Fatalf("%q: expected an error", in)
	return ""
}

func TestRunQueries(t *testing.T) {
	cases := map[string]string{
		"kind=run":                                `WorkflowId STARTS_WITH "run/"`,
		"kind=run, phase=Running":                 `WorkflowId STARTS_WITH "run/" AND ExecutionStatus = 'Running'`,
		"kind=run, phase in (Running, Failed)":    `WorkflowId STARTS_WITH "run/" AND ExecutionStatus IN ('Running', 'Failed')`,
		"kind=run, pipeline=deploy":               `WorkflowId STARTS_WITH "run/" AND WorkflowType = 'deploy'`,
		"kind=run, pipeline=^dep":                 `WorkflowId STARTS_WITH "run/" AND WorkflowType STARTS_WITH "dep"`,
		"kind=run, id=abc":                        `WorkflowId STARTS_WITH "run/" AND WorkflowId = 'run/abc'`,
		"kind=run, id=^rel-":                      `WorkflowId STARTS_WITH "run/" AND WorkflowId STARTS_WITH "run/rel-"`,
		"kind=run, label.env=prod":                `WorkflowId STARTS_WITH "run/" AND EntityLabels IN ('env=prod')`,
		"kind=run, started>-2h":                   `WorkflowId STARTS_WITH "run/" AND StartTime > '2026-08-24T10:00:00Z'`,
		"kind=run, finished<2026-08-24T00:00:00Z": `WorkflowId STARTS_WITH "run/" AND CloseTime < '2026-08-24T00:00:00Z'`,
	}
	for in, want := range cases {
		if got := compile(t, in); got != want {
			t.Errorf("%q:\n got %s\nwant %s", in, got, want)
		}
	}
}

func TestEntityQueries(t *testing.T) {
	cases := map[string]string{
		"kind=agent":                             `EntityKind = 'agent' AND ExecutionStatus = 'Running'`,
		"kind in (agent, artifact)":              `EntityKind IN ('agent', 'artifact') AND ExecutionStatus = 'Running'`,
		"phase=ready":                            `EntityKind IS NOT NULL AND ExecutionStatus = 'Running' AND EntityPhase = 'ready'`,
		"kind=agent, owner=run/x":                `EntityKind = 'agent' AND ExecutionStatus = 'Running' AND EntityOwner = 'run/x'`,
		"kind=agent, label.env in (prod, stage)": `EntityKind = 'agent' AND ExecutionStatus = 'Running' AND EntityLabels IN ('env=prod', 'env=stage')`,
	}
	for in, want := range cases {
		if got := compile(t, in); got != want {
			t.Errorf("%q:\n got %s\nwant %s", in, got, want)
		}
	}
}

func TestErrors(t *testing.T) {
	cases := map[string]string{
		"":                          "empty selector",
		"kind=run, owner=run/x":     "owner does not apply",
		"pipeline=deploy":           "pipeline applies to kind=run only",
		"kind in (run, agent)":      "cannot be mixed",
		"bogus=1":                   "unknown field",
		"kind=run, phase~Running":   "no operator",
		`kind=run, label.a=b'c`:     "quotes",
		"kind=run, started=-2h":     "> and < only",
		"kind=run, started>xyz":     "RFC3339",
		"kind=run, label.env!=prod": "= and in only",
		"kind=run, kind=agent":      "once",
	}
	for in, want := range cases {
		if got := compileErr(t, in); !strings.Contains(got, want) {
			t.Errorf("%q: error %q does not mention %q", in, got, want)
		}
	}
}

func TestIsRunQuery(t *testing.T) {
	q, _ := Parse("kind=run, phase=Running")
	if !IsRunQuery(q) {
		t.Error("kind=run not detected")
	}
	q, _ = Parse("kind=agent")
	if IsRunQuery(q) {
		t.Error("kind=agent misdetected as run")
	}
}
