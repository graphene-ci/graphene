package worker

import (
	"errors"
	"testing"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestChildTerminalErrorNonRetryable guards the fan-out hole: a
// failed/cancelled/terminated child must fail its await in ONE attempt —
// as a NON-RETRYABLE application error — so the parent's handle surfaces it
// and the fan-out semaphore is released. Without this the default-retryable
// GetWorkflow error loops until the activity's own (168h) timeout, pinning
// a permit and starving siblings.
func TestChildTerminalErrorNonRetryable(t *testing.T) {
	cases := []struct {
		status   enums.WorkflowExecutionStatus
		wantType string
	}{
		{enums.WORKFLOW_EXECUTION_STATUS_FAILED, "ChildFailed"},
		{enums.WORKFLOW_EXECUTION_STATUS_TERMINATED, "ChildFailed"},
		{enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, "ChildFailed"},
		{enums.WORKFLOW_EXECUTION_STATUS_CANCELED, "ChildCancelled"},
	}
	for _, tc := range cases {
		err := childTerminalError(tc.status, "suite-cell-1")
		if err == nil {
			t.Fatalf("%s: expected an error, got nil", tc.status)
		}
		var appErr *temporal.ApplicationError
		if !errors.As(err, &appErr) {
			t.Fatalf("%s: expected *temporal.ApplicationError, got %T (%v)", tc.status, err, err)
		}
		if !appErr.NonRetryable() {
			t.Errorf("%s: error must be NON-RETRYABLE, else the await loops forever", tc.status)
		}
		if appErr.Type() != tc.wantType {
			t.Errorf("%s: want type %q, got %q", tc.status, tc.wantType, appErr.Type())
		}
	}
}

// TestPermanentStart keeps caller-fault start errors (bad params, unknown
// pipeline, policy) off the infinite-retry path while leaving transient
// infrastructure errors retryable.
func TestPermanentStart(t *testing.T) {
	permanent := []error{
		status.Error(codes.InvalidArgument, "bad params"),
		status.Error(codes.FailedPrecondition, "no active image"),
		status.Error(codes.NotFound, "no such pipeline"),
		status.Error(codes.PermissionDenied, "nope"),
	}
	for _, e := range permanent {
		if !permanentStart(e) {
			t.Errorf("expected permanent (non-retryable) for %v", e)
		}
	}
	transient := []error{
		status.Error(codes.Unavailable, "temporal down"),
		status.Error(codes.Internal, "blip"),
		errors.New("plain error"),
	}
	for _, e := range transient {
		if permanentStart(e) {
			t.Errorf("expected transient (retryable) for %v", e)
		}
	}
}
