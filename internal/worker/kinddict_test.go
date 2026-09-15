package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

type retirementClient struct {
	client.Client
	signalError error
	signals     int
	waits       int
}

func (c *retirementClient) SignalWorkflow(_ context.Context, workflowID, runID, signal string, value interface{}) error {
	if workflowID != "kind/test.database" || runID != "" || signal != entity.DeleteSignalName || value != nil {
		return errors.New("unexpected retirement signal")
	}
	c.signals++
	return c.signalError
}
func (c *retirementClient) DescribeWorkflowExecution(context.Context, string, string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	c.waits++
	return nil, errors.New("the calling workflow cannot close until its retirement activity returns")
}

// Retirement runs inside the target workflow's reconcile tick. Waiting for
// that workflow to close would wait for the activity's own completion.
func TestRetireKindDoesNotWaitForCallingWorkflow(t *testing.T) {
	denied := serviceerror.NewPermissionDenied("denied", "")
	for _, tc := range []struct {
		name                   string
		signalError, wantError error
	}{
		{name: "signal accepted"},
		{name: "already gone", signalError: serviceerror.NewNotFound("gone")},
		{name: "signal rejected", signalError: denied, wantError: denied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &retirementClient{signalError: tc.signalError}
			w := &Worker{deps: Deps{Client: c}}
			err := w.retireKind(context.Background(), "test.database")
			if !errors.Is(err, tc.wantError) {
				t.Fatalf("got %v, want %v", err, tc.wantError)
			}
			if c.signals != 1 || c.waits != 0 {
				t.Fatalf("signals=%d, closure waits=%d", c.signals, c.waits)
			}
		})
	}
}
