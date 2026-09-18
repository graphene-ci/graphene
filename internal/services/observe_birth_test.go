package services

import (
	"context"
	"errors"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeTemporal answers DescribeWorkflowExecution from a table keyed by run
// id; "" is the latest run of the workflow id.
type fakeTemporal map[string]*workflowpb.WorkflowExecutionInfo

func (f fakeTemporal) DescribeWorkflowExecution(_ context.Context, _, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	info, ok := f[runID]
	if !ok {
		return nil, errors.New("workflow execution not found")
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: info}, nil
}

func run(runID, firstRunID string, start time.Time) *workflowpb.WorkflowExecutionInfo {
	return &workflowpb.WorkflowExecutionInfo{
		Execution:  &commonpb.WorkflowExecution{WorkflowId: "agent/db-1", RunId: runID},
		FirstRunId: firstRunID,
		StartTime:  timestamppb.New(start),
	}
}

func TestRecordBirth(t *testing.T) {
	born := time.Date(2026, 9, 18, 11, 0, 0, 0, time.UTC)
	later := born.Add(36 * time.Hour)
	for name, tc := range map[string]struct {
		temporal fakeTemporal
		want     time.Time
	}{
		// A record that never continued-as-new: its only run is its birth.
		"single run": {fakeTemporal{"": run("r1", "r1", born)}, born},
		// A long-lived record: the latest run started a day and a half after
		// the record was born — the birth is the FIRST run's start.
		"continued as new": {fakeTemporal{"": run("r9", "r1", later), "r1": run("r1", "r1", born)}, born},
		// The first run is past retention: unknown beats a wrong bound that
		// would hide the record's own history.
		"first run gone": {fakeTemporal{"": run("r9", "r1", later)}, time.Time{}},
		"no such record": {fakeTemporal{}, time.Time{}},
	} {
		t.Run(name, func(t *testing.T) {
			if got := recordBirth(context.Background(), tc.temporal, "agent/db-1"); !got.Equal(tc.want) {
				t.Fatalf("birth = %v, want %v", got, tc.want)
			}
		})
	}
}
