package worker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	temporalworker "go.temporal.io/sdk/worker"

	"github.com/graphene-ci/graphene/internal/standflow"
	agentflow "github.com/graphene-ci/pipeline/pkg/flow/agent"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// standClient knows which stands exist and records every command sent.
type standClient struct {
	client.Client
	stands  map[string]entity.Phase
	updates []string
	starts  []string
}

func (c *standClient) DescribeWorkflowExecution(context.Context, string, string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return &workflowservice.DescribeWorkflowExecutionResponse{}, nil
}

type describeValue struct {
	out entity.DescribeOut[standflow.Spec, standflow.State]
}

func (describeValue) HasValue() bool { return true }

func (v describeValue) Get(ptr any) error {
	raw, err := json.Marshal(v.out)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, ptr)
}

func (c *standClient) QueryWorkflow(_ context.Context, workflowID, _ string, _ string, _ ...any) (converter.EncodedValue, error) {
	phase, ok := c.stands[workflowID]
	if !ok {
		return nil, errors.New("workflow not found")
	}
	return describeValue{out: entity.DescribeOut[standflow.Spec, standflow.State]{Phase: phase}}, nil
}

func (c *standClient) UpdateWorkflow(_ context.Context, opts client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	c.updates = append(c.updates, opts.WorkflowID+" "+opts.UpdateName)
	return transferHandle{}, nil
}

func (c *standClient) NewWithStartWorkflowOperation(opts client.StartWorkflowOptions, _ any, _ ...any) client.WithStartWorkflowOperation {
	c.starts = append(c.starts, opts.ID)
	return nil
}

func (c *standClient) UpdateWithStartWorkflow(_ context.Context, opts client.UpdateWithStartWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	c.updates = append(c.updates, "start "+opts.UpdateOptions.UpdateName)
	return transferHandle{}, nil
}

// A run creates only its OWN pipeline's stand; any other stand must have
// been applied, and a missing one is refused before the resource changes
// hands.
func TestTransferOntoStands(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		owner   string
		creator id.PipelineId
		stands  map[string]entity.Phase
		missing bool
		want    []string
	}{
		{
			name: "own stand is created on first use", owner: "stand/stroppy", creator: "stroppy",
			want: []string{"test.vm/db transfer-owner", "test.vm/db entity-set-labels", "start accept"},
		},
		{
			name: "named stand that exists", owner: "stand/lab", creator: "stroppy",
			stands: map[string]entity.Phase{"stand/lab": entity.PhaseReady},
			want:   []string{"test.vm/db transfer-owner", "test.vm/db entity-set-labels", "stand/lab accept"},
		},
		{name: "named stand never applied", owner: "stand/lab", creator: "stroppy", missing: true},
		{
			name: "named stand being deleted", owner: "stand/lab", creator: "stroppy", missing: true,
			stands: map[string]entity.Phase{"stand/lab": entity.PhaseDeleting},
		},
		{name: "operator cannot create a pipeline stand", owner: "stand/stroppy", missing: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			cl := &standClient{stands: c.stands}
			w := &Worker{deps: Deps{Client: cl}, standDef: standflow.New(0)}
			err := w.Transfer(context.Background(), wire.TransferResourceRequest{Resource: "test.vm/db", NewOwner: ref.OwnerRef(c.owner)}, c.creator)
			if c.missing {
				if !errors.Is(err, ErrStandMissing) {
					t.Fatalf("err = %v, want ErrStandMissing", err)
				}
				if len(cl.updates) != 0 {
					t.Fatalf("a refused transfer moved the resource: %v", cl.updates)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(cl.updates) != len(c.want) {
				t.Fatalf("commands %v, want %v", cl.updates, c.want)
			}
			for i := range c.want {
				if cl.updates[i] != c.want[i] {
					t.Fatalf("commands %v, want %v", cl.updates, c.want)
				}
			}
		})
	}
}

// A stand's values are checked at the door: a duration typo fails the
// apply instead of silently meaning "no TTL".
func TestStandSpecValidatedOnApply(t *testing.T) {
	t.Parallel()
	check := validated[standflow.Spec]()
	cases := []struct {
		spec string
		ok   bool
	}{
		{spec: ``, ok: true},
		{spec: `{}`, ok: true},
		{spec: `{"owner":"pipeline/stroppy-stand","ttl":"168h","idleTimeout":"72h"}`, ok: true},
		{spec: `{"ttl":"a week"}`},
		{spec: `{"idleTimeout":"-1h"}`},
		{spec: `{"pipelineId":"p","owner":"pipeline/q"}`},
	}
	for _, c := range cases {
		t.Run(c.spec, func(t *testing.T) {
			t.Parallel()
			if err := check(json.RawMessage(c.spec)); (err == nil) != c.ok {
				t.Fatalf("err = %v, want ok=%v", err, c.ok)
			}
		})
	}
}

// leaseClient answers run descriptions, record owners and stand leases.
type leaseClient struct {
	client.Client
	runs   map[string]enums.WorkflowExecutionStatus
	owners map[string]string
	stands map[string]*standflow.Lease
	broken bool
}

func (c *leaseClient) DescribeWorkflowExecution(_ context.Context, id, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	if c.broken {
		return nil, errors.New("visibility down")
	}
	if status, ok := c.runs[id]; ok {
		return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: status}}, nil
	}
	owner, ok := c.owners[id]
	if !ok {
		return nil, serviceerror.NewNotFound("no " + id)
	}
	payload, err := converter.GetDefaultDataConverter().ToPayload(owner)
	if err != nil {
		return nil, err
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
		SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{wire.SearchAttrOwner.GetName(): payload}},
	}}, nil
}

func (c *leaseClient) QueryWorkflow(_ context.Context, id, _, _ string, _ ...any) (converter.EncodedValue, error) {
	lease, ok := c.stands[id]
	if !ok {
		return nil, serviceerror.NewNotFound("no " + id)
	}
	return describeValue{out: entity.DescribeOut[standflow.Spec, standflow.State]{Phase: entity.PhaseReady, State: standflow.State{Lease: lease}}}, nil
}

func TestRunState(t *testing.T) {
	t.Parallel()
	cl := &leaseClient{runs: map[string]enums.WorkflowExecutionStatus{
		"run/live": enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
		"run/done": enums.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}}
	w := &Worker{deps: Deps{Client: cl}}
	for run, want := range map[string]string{"live": standflow.RunRunning, "done": standflow.RunClosed, "later": standflow.RunUnknown} {
		got, err := w.runState(context.Background(), run)
		if err != nil || got != want {
			t.Fatalf("%s: %q %v, want %q", run, got, err, want)
		}
	}
}

// A record is on a leased stand when its ownership chain ends at one; a
// lookup that fails refuses the delete instead of letting it through.
func TestLeasedStandOf(t *testing.T) {
	t.Parallel()
	owners := map[string]string{
		"yc.vm/db-1":    "stand/lab",
		"docker/pg":     "agent/db-1",
		"agent/db-1":    "yc.vm/db-1",
		"yc.vm/free-1":  "stand/idle",
		"artifact/mine": "run/r1",
		"loop/a":        "loop/b",
		"loop/b":        "loop/a",
	}
	stands := map[string]*standflow.Lease{
		"stand/lab":  {Run: "r1"},
		"stand/idle": nil,
	}
	cases := []struct {
		record string
		broken bool
		stand  string
		leased bool
		fails  bool
	}{
		{record: "docker/pg", stand: "lab", leased: true},
		{record: "stand/lab", stand: "lab", leased: true},
		{record: "yc.vm/free-1", stand: "idle"},
		{record: "artifact/mine"},
		{record: "nothing/here"},
		{record: "docker/pg", broken: true, fails: true},
		{record: "loop/a", fails: true},
	}
	for _, c := range cases {
		t.Run(c.record, func(t *testing.T) {
			t.Parallel()
			w := &Worker{deps: Deps{Client: &leaseClient{owners: owners, stands: stands, broken: c.broken}}, standDef: standflow.New(0)}
			stand, leased, err := w.LeasedStandOf(context.Background(), c.record)
			if (err != nil) != c.fails {
				t.Fatalf("err = %v", err)
			}
			if !c.fails && (stand != c.stand && c.leased || leased != c.leased) {
				t.Fatalf("got %q leased=%v, want %q leased=%v", stand, leased, c.stand, c.leased)
			}
		})
	}
}

// dirtyClient is a stand holding a tree, agents with or without leaks.
type dirtyClient struct {
	client.Client
	holdings []string
	children map[string][]string
	leaks    map[string]int
}

func (c *dirtyClient) ListWorkflow(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	resp := &workflowservice.ListWorkflowExecutionsResponse{}
	for owner, kids := range c.children {
		if !strings.Contains(req.GetQuery(), "'"+owner+"'") {
			continue
		}
		for _, kid := range kids {
			resp.Executions = append(resp.Executions, &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: kid}})
		}
	}
	return resp, nil
}

type agentValue struct{ leaks int }

func (agentValue) HasValue() bool { return true }

func (v agentValue) Get(ptr any) error {
	st := agentflow.State{LeaksOverflow: v.leaks}
	raw, err := json.Marshal(map[string]any{"phase": entity.PhaseReady, "state": st})
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, ptr)
}

func (c *dirtyClient) QueryWorkflow(_ context.Context, id, _, _ string, _ ...any) (converter.EncodedValue, error) {
	if id == "stand/lab" {
		h := map[string]standflow.Holding{}
		for _, held := range c.holdings {
			h[held] = standflow.Holding{}
		}
		return describeValue{out: entity.DescribeOut[standflow.Spec, standflow.State]{Phase: entity.PhaseReady, State: standflow.State{Holdings: h}}}, nil
	}
	return agentValue{leaks: c.leaks[id]}, nil
}

// Dirt anywhere in the stand's trees is found: an agent held directly or
// one under a held VM.
func TestStandDirty(t *testing.T) {
	t.Parallel()
	cl := &dirtyClient{
		holdings: []string{"yc.vm/db-1", "agent/manual-1"},
		children: map[string][]string{"yc.vm/db-1": {"agent/db-1"}, "agent/db-1": {"docker/pg"}},
		leaks:    map[string]int{"agent/db-1": 1},
	}
	w := &Worker{deps: Deps{Client: cl}, standDef: standflow.New(0), agentDef: agentflow.Definition(agentflow.Options{})}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.SetWorkerOptions(temporalworker.Options{})
	env.RegisterActivity(w.standDirty)
	if _, err := env.ExecuteActivity(w.standDirty); err == nil {
		t.Fatal("the activity must refuse a caller that is not a stand")
	}
	dirty, err := w.dirtyOf(context.Background(), "lab")
	if err != nil {
		t.Fatal(err)
	}
	if len(dirty) != 1 || dirty[0] != "agent/db-1" {
		t.Fatalf("dirty %v", dirty)
	}
}

// agentPhaseClient answers an agent record's describe with a phase.
type agentPhaseClient struct {
	client.Client
	phase entity.Phase
	gone  bool
}

func (c agentPhaseClient) QueryWorkflow(context.Context, string, string, string, ...any) (converter.EncodedValue, error) {
	if c.gone {
		return nil, serviceerror.NewNotFound("no record")
	}
	return phaseValue{phase: c.phase}, nil
}

type phaseValue struct{ phase entity.Phase }

func (phaseValue) HasValue() bool { return true }

func (v phaseValue) Get(ptr any) error {
	raw, err := json.Marshal(map[string]any{"phase": v.phase})
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, ptr)
}

// A deleted agent record answers for no machine: its agent is refused
// whatever token it holds. A record being deleted keeps the session so a
// decommission reaches the agent.
func TestAgentExists(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		client agentPhaseClient
		want   bool
	}{
		{name: "ready", client: agentPhaseClient{phase: entity.PhaseReady}, want: true},
		{name: "deleting", client: agentPhaseClient{phase: entity.PhaseDeleting}, want: true},
		{name: "deleted", client: agentPhaseClient{phase: entity.PhaseDeleted}},
		{name: "never declared", client: agentPhaseClient{gone: true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			w := &Worker{deps: Deps{Client: c.client}, agentDef: agentflow.Definition(agentflow.Options{})}
			if got := w.AgentExists(context.Background(), "db-1"); got != c.want {
				t.Fatalf("AgentExists = %v, want %v", got, c.want)
			}
		})
	}
}
