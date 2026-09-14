package worker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/graphene-ci/pipeline/pkg/wire"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	temporalworker "go.temporal.io/sdk/worker"
)

type transferClient struct {
	client.Client
	update func(context.Context, client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error)
}

func (c transferClient) UpdateWorkflow(ctx context.Context, opts client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	return c.update(ctx, opts)
}

type transferHandle struct{ client.WorkflowUpdateHandle }

func (transferHandle) Get(context.Context, interface{}) error { return nil }

// A resource that cannot accept its transfer until after a heartbeat models
// a VM still being created. The actual activity must keep the wait alive and
// preserve both successful completion and a command failure.
func TestTransferResourceHeartbeat(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "command error"
		}
		t.Run(name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.SetTestTimeout(5 * time.Second)
			beat := make(chan struct{})
			var once sync.Once
			env.SetOnActivityHeartbeatListener(func(_ *activity.Info, _ converter.EncodedValues) {
				once.Do(func() { close(beat) })
			})
			w := &Worker{deps: Deps{Client: transferClient{update: func(ctx context.Context, opts client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
				if opts.WorkflowID != "test.vm/db" || opts.UpdateName != wire.TransferOwnerCmdName {
					return nil, errors.New("unexpected transfer target")
				}
				select {
				case <-beat:
					if fail {
						return nil, errors.New("transfer rejected by entity")
					}
					return transferHandle{}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}}}}
			env.RegisterActivity(w.transferResource)
			_, err := env.ExecuteActivity(w.transferResource, wire.TransferResourceRequest{Resource: "test.vm/db", NewOwner: "run/owner", From: "run/source"})
			if fail {
				if err == nil || !strings.Contains(err.Error(), "transfer rejected by entity") {
					t.Fatalf("command error lost: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			select {
			case <-beat:
			default:
				t.Fatal("transfer completed without heartbeat")
			}
		})
	}
}

func TestTransferResourceCancellation(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	env.SetWorkerOptions(temporalworker.Options{BackgroundActivityContext: ctx})
	env.SetTestTimeout(5 * time.Second)
	stopped := make(chan struct{})
	w := &Worker{deps: Deps{Client: transferClient{update: func(ctx context.Context, _ client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
		<-ctx.Done()
		close(stopped)
		return nil, ctx.Err()
	}}}}
	env.RegisterActivity(w.transferResource)
	_, err := env.ExecuteActivity(w.transferResource, wire.TransferResourceRequest{Resource: "test.vm/db", NewOwner: "run/owner", From: "run/source"})
	if err == nil {
		t.Fatal("canceled transfer succeeded")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("transfer request outlived cancellation")
	}
}
