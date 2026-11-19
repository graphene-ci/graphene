package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	agentflow "github.com/graphene-ci/pipeline/pkg/flow/agent"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// leakClient answers the caller's describe with its queue and records the
// command sent to the agent record.
type leakClient struct {
	client.Client
	queue   string
	updates []client.UpdateWorkflowOptions
}

func (c *leakClient) DescribeWorkflowExecution(_ context.Context, _, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	if c.queue == "" {
		return nil, errors.New("no such workflow")
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{ExecutionConfig: &workflowpb.WorkflowExecutionConfig{
		TaskQueue: &taskqueue.TaskQueue{Name: c.queue},
	}}, nil
}

func (c *leakClient) UpdateWorkflow(_ context.Context, opts client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	c.updates = append(c.updates, opts)
	return transferHandle{}, nil
}

// The machine and the record come from Temporal, not from the request.
func TestReportLeak(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		queue     string
		wantAgent string
		fails     bool
	}{
		{name: "a record on a machine dirties that machine", queue: wire.AgentRunQueue("db", "run-1"), wantAgent: "agent/db"},
		{name: "a record off any machine is refused", queue: wire.RunQueue("run-1"), fails: true},
		{name: "an unknown caller is refused", queue: "", fails: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.SetTestTimeout(5 * time.Second)
			cl := &leakClient{queue: c.queue}
			w := &Worker{deps: Deps{Client: cl}, agentDef: agentflow.Definition(agentflow.Options{})}
			env.RegisterActivity(w.reportLeak)
			_, err := env.ExecuteActivity(w.reportLeak, wire.ReportLeakRequest{Error: "daemon gone"})
			if (err != nil) != c.fails {
				t.Fatalf("err = %v, want fails=%v", err, c.fails)
			}
			if c.fails {
				if len(cl.updates) != 0 {
					t.Fatalf("a refused report reached a record: %+v", cl.updates)
				}
				return
			}
			if len(cl.updates) != 1 || cl.updates[0].WorkflowID != c.wantAgent || cl.updates[0].UpdateName != "report-leak" {
				t.Fatalf("updates %+v", cl.updates)
			}
		})
	}
}
