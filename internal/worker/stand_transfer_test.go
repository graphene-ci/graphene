package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/graphene-ci/graphene/internal/standflow"
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

type describeValue struct{ out entity.DescribeOut[standflow.Spec, standflow.State] }

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
