package operator

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/graphene-ci/graphene/sdk/agent"
	graphenev1 "github.com/graphene-ci/graphene/sdk/api/v1"
	"github.com/graphene-ci/graphene/sdk/pipeline"
)

// Client is the operator's side of Temporal: start a workflow, ask how far
// it got, wake it when a record it waits for becomes ready.
type Client struct {
	temporal client.Client
}

// NewClient builds one over a connected Temporal client.
func NewClient(temporal client.Client) *Client {
	return &Client{temporal: temporal}
}

// Start begins the run's workflow. Starting one that already exists is the
// expected answer on a second pass, not a failure: the workflow id is the
// record's name, so the collision IS the idempotency.
func (c *Client) Start(ctx context.Context, req StartRequest) (string, error) {
	options := client.StartWorkflowOptions{
		ID:        req.WorkflowID,
		TaskQueue: req.Queue,
		// Two runs of the same record are never wanted. A record is one
		// run; repeating a run means a new record.
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}

	handle, err := c.temporal.ExecuteWorkflow(ctx, options, pipeline.WorkflowName, req.Input)
	if err != nil {
		var running *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &running) {
			return c.temporal.GetWorkflow(ctx, req.WorkflowID, "").GetRunID(), nil
		}

		return "", fmt.Errorf("воркфлоу не стартовал: %w", err)
	}

	return handle.GetRunID(), nil
}

// Phase reports how far the workflow got, in the record's vocabulary.
func (c *Client) Phase(ctx context.Context, workflowID string) (graphenev1.RunPhase, string, error) {
	described, err := c.temporal.DescribeWorkflowExecution(ctx, workflowID, "")
	if err != nil {
		return "", "", fmt.Errorf("воркфлоу не описался: %w", err)
	}

	info := described.GetWorkflowExecutionInfo()

	switch info.GetStatus() {
	case enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
		enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return graphenev1.RunRunning, "", nil
	case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return graphenev1.RunSucceeded, "", nil
	case enums.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enums.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return graphenev1.RunCanceled, "остановлен", nil
	case enums.WORKFLOW_EXECUTION_STATUS_FAILED:
		return graphenev1.RunFailed, c.failure(ctx, workflowID), nil
	case enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return graphenev1.RunFailed, "истекло время", nil
	case enums.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED:
		return graphenev1.RunPending, "", nil
	default:
		return graphenev1.RunPending, "", nil
	}
}

// failure asks the workflow what went wrong. The answer is what a person
// reads on `graphene watch`, so it is worth one extra call.
func (c *Client) failure(ctx context.Context, workflowID string) string {
	err := c.temporal.GetWorkflow(ctx, workflowID, "").Get(ctx, nil)
	if err == nil {
		return ""
	}

	return err.Error()
}

// Stop ends a workflow. A workflow that already ended is not an error:
// that is the normal case when a record is deleted after its run finished.
func (c *Client) Stop(ctx context.Context, workflowID string) error {
	err := c.temporal.TerminateWorkflow(ctx, workflowID, "", "запись прогона удалена")
	if err == nil {
		return nil
	}

	var gone *serviceerror.NotFound
	if errors.As(err, &gone) {
		return nil
	}

	return fmt.Errorf("воркфлоу не остановился: %w", err)
}

// Signal wakes a workflow waiting for a record.
func (c *Client) Signal(ctx context.Context, workflowID, name string, payload agent.ReadySignal) error {
	if err := c.temporal.SignalWorkflow(ctx, workflowID, "", name, payload); err != nil {
		return fmt.Errorf("сигнал не отправился: %w", err)
	}

	return nil
}
