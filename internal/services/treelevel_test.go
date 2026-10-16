package services

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// pagedVisibility answers a query with fixed pages.
type pagedVisibility struct {
	client.Client
	pages [][]*workflowpb.WorkflowExecutionInfo
}

func (p *pagedVisibility) ListWorkflow(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	i := 0
	if len(req.GetNextPageToken()) > 0 {
		i = int(req.GetNextPageToken()[0])
	}
	resp := &workflowservice.ListWorkflowExecutionsResponse{Executions: p.pages[i]}
	if i+1 < len(p.pages) {
		resp.NextPageToken = []byte{byte(i + 1)}
	}
	return resp, nil
}

func execution(id string, status enums.WorkflowExecutionStatus, closed time.Time) *workflowpb.WorkflowExecutionInfo {
	e := &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id}, Status: status}
	if !closed.IsZero() {
		e.CloseTime = timestamppb.New(closed)
	}
	return e
}

// One row per record across every page: the live execution when there is
// one, the last to close otherwise — a record is shown, and walked, once.
func TestTreeLevelIsOneRowPerRecord(t *testing.T) {
	t0 := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	running := enums.WORKFLOW_EXECUTION_STATUS_RUNNING
	done := enums.WORKFLOW_EXECUTION_STATUS_COMPLETED
	failed := enums.WORKFLOW_EXECUTION_STATUS_FAILED
	c := &pagedVisibility{pages: [][]*workflowpb.WorkflowExecutionInfo{
		{execution("vm/db-1", done, t0), execution("vm/db-1", running, time.Time{}), execution("net/n", failed, t0)},
		{execution("vm/db-1", done, t0.Add(time.Hour)), execution("disk/d", done, t0), execution("disk/d", failed, t0.Add(time.Minute))},
	}}
	got, err := treeLevel(context.Background(), c, "q")
	require.NoError(t, err)
	require.Len(t, got, 3)
	require.Equal(t, "vm/db-1", got[0].GetExecution().GetWorkflowId())
	require.Equal(t, running, got[0].GetStatus(), "the live execution wins")
	require.Equal(t, "net/n", got[1].GetExecution().GetWorkflowId())
	require.Equal(t, "disk/d", got[2].GetExecution().GetWorkflowId(), "the second page is read")
	require.Equal(t, failed, got[2].GetStatus(), "the last to close")
}
