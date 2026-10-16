package worker

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/graphene-ci/pipeline/pkg/ref"
)

// treeClient is a tree of records whose finalizers each take `finalize`
// once the delete signal lands: a record is listed under its owner while
// running, and closes `finalize` after its signal.
type treeClient struct {
	client.Client
	t        *testing.T
	finalize time.Duration
	mu       sync.Mutex
	owner    map[string]string // record -> owner
	// failed are records that closed without a delete (record -> owner),
	// each in its run (failedRun).
	failed    map[string]string
	failedRun map[string]string
	signaled  map[string]time.Time // record -> when its delete landed
	order     []string             // records in the order they closed
}

var (
	ownerQuery = regexp.MustCompile(`EntityOwner = '([^']+)'`)
	runQuery   = regexp.MustCompile(`EntityLabels IN \('graphene.io/run=([^']+)'\)`)
)

func (c *treeClient) closed(id string, now time.Time) bool {
	at, ok := c.signaled[id]
	return ok && now.Sub(at) >= c.finalize
}

func (c *treeClient) ListWorkflow(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	owner := ownerQuery.FindStringSubmatch(req.GetQuery())[1]
	c.mu.Lock()
	defer c.mu.Unlock()
	out := &workflowservice.ListWorkflowExecutionsResponse{}
	if strings.Contains(req.GetQuery(), "ExecutionStatus != 'Running'") {
		require.Contains(c.t, req.GetQuery(), "ExecutionStatus != 'ContinuedAsNew'")
		run := runQuery.FindStringSubmatch(req.GetQuery())[1]
		for id, o := range c.failed {
			if o == owner && c.failedRun[id] == run {
				out.Executions = append(out.Executions, &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id}})
			}
		}
		return out, nil
	}
	for id, o := range c.owner {
		if o == owner && !c.closed(id, time.Now()) {
			out.Executions = append(out.Executions, &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id}})
		}
	}
	return out, nil
}

func (c *treeClient) SignalWorkflow(_ context.Context, id, _, _ string, _ any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.failed[id]; ok {
		panic("delete of " + id + ", a record already closed")
	}
	// The owner is deleted only after everything it owns is gone.
	for child, o := range c.owner {
		if o == id && !c.closed(child, time.Now()) {
			panic("delete of " + id + " before its child " + child)
		}
	}
	if _, ok := c.signaled[id]; !ok {
		c.signaled[id] = time.Now()
	}
	return nil
}

func (c *treeClient) DescribeWorkflowExecution(_ context.Context, id, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	status := enums.WORKFLOW_EXECUTION_STATUS_RUNNING
	if c.closed(id, time.Now()) {
		status = enums.WORKFLOW_EXECUTION_STATUS_COMPLETED
		if !contains(c.order, id) {
			c.order = append(c.order, id)
		}
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: status}}, nil
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// Four machines under one subnet go down together — the time of one,
// not of four — and the subnet, then the network, only after them.
func TestCascadeTakesSiblingsDownTogether(t *testing.T) {
	c := &treeClient{
		t:        t,
		finalize: 1200 * time.Millisecond,
		signaled: map[string]time.Time{},
		owner: map[string]string{
			"net/n":      "run/r",
			"subnet/s":   "net/n",
			"instance/a": "subnet/s", "instance/b": "subnet/s", "instance/c": "subnet/s", "instance/d": "subnet/s",
		},
	}
	w := &Worker{deps: Deps{Client: c}}
	start := time.Now()
	require.NoError(t, w.cascade(context.Background(), ref.OwnerRef("run/r")))
	took := time.Since(start)
	// The closure is polled each second, so a record takes ~2s to be seen
	// gone: three levels in parallel take ~6s, one record after another —
	// four machines, the subnet, the network — ~12s.
	require.Less(t, took, 9*time.Second, "four siblings took %s", took)
	require.GreaterOrEqual(t, took, 3*c.finalize)
	require.Len(t, c.order, 6)
	require.Equal(t, "subnet/s", c.order[4])
	require.Equal(t, "net/n", c.order[5])
}

// A network the cloud refused closed its record, but the subnet and the
// machine declared under it run on: the run's teardown walks the failed
// record and takes them down — the machine before the subnet — without
// deleting the closed record again. A closed record of the same name in
// ANOTHER run is not walked: names are reused run after run.
func TestCascadeWalksAFailedRecordsChildren(t *testing.T) {
	c := &treeClient{
		t:         t,
		finalize:  100 * time.Millisecond,
		signaled:  map[string]time.Time{},
		failed:    map[string]string{"net/n": "run/r", "net/old": "run/r"},
		failedRun: map[string]string{"net/n": "r", "net/old": "other"},
		owner: map[string]string{
			"subnet/s":   "net/n",
			"instance/a": "subnet/s",
			"subnet/x":   "net/old",
		},
	}
	w := &Worker{deps: Deps{Client: c}}
	require.NoError(t, w.cascade(context.Background(), ref.OwnerRef("run/r")))
	require.Equal(t, []string{"instance/a", "subnet/s"}, c.order, "children of the failed root, deepest first")
	_, touched := c.signaled["subnet/x"]
	require.False(t, touched, "another run's record is not this run's teardown")
}

// A wait on someone else's reply keeps the activity heartbeating and ends
// with that wait's own answer.
func TestHeartbeatWhile(t *testing.T) {
	err := heartbeatWhile(context.Background(), "x", func() error { return context.DeadlineExceeded })
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
