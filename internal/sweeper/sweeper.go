// Package sweeper enforces stand TTLs: a resource transferred to a
// Stand with KeepFor carries its deadline in the EntityKeepUntil search
// attribute; when it expires, the sweeper deletes the resource with its
// subtree. Also the answer to "who deletes what nobody owns any more".
package sweeper

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/graphene-ci/pipeline/pkg/ref"
)

// Cascader deletes a resource subtree — implemented by the worker.
type Cascader interface {
	CascadeDelete(ctx context.Context, owner ref.OwnerRef) error
	DeleteOne(ctx context.Context, workflowId string) error
}

// Sweeper ticks over expired stand stays.
type Sweeper struct {
	temporal client.Client
	cascader Cascader
	log      *slog.Logger
}

// New builds the sweeper.
func New(temporal client.Client, cascader Cascader, log *slog.Logger) *Sweeper {
	return &Sweeper{temporal: temporal, cascader: cascader, log: log}
}

// Tick sweeps until ctx ends.
func (s *Sweeper) Tick(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

func (s *Sweeper) sweep(ctx context.Context) {
	query := fmt.Sprintf("EntityKeepUntil <= '%s' AND ExecutionStatus = 'Running'",
		time.Now().UTC().Format(time.RFC3339))
	resp, err := s.temporal.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Query: query})
	if err != nil {
		s.log.Error("sweep: list expired", "error", err)
		return
	}
	for _, e := range resp.GetExecutions() {
		workflowId := e.GetExecution().GetWorkflowId()
		s.log.Info("stand TTL expired, deleting", "resource", workflowId)
		// Subtree first, then the resource itself.
		if err := s.cascader.CascadeDelete(ctx, ref.OwnerRef(workflowId)); err != nil {
			s.log.Error("sweep cascade", "resource", workflowId, "error", err)
			continue
		}
		if err := s.cascader.DeleteOne(ctx, workflowId); err != nil {
			s.log.Error("sweep delete", "resource", workflowId, "error", err)
		}
	}
}
