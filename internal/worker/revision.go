package worker

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	v1 "github.com/graphene-ci/graphene/api/v1"
	"github.com/graphene-ci/graphene/pkg/agent"
)

// RecordRequirements writes what a revision needs into its record.
//
// It runs here rather than in the pipeline's own worker because that worker
// has no access to the cluster and must not get any — the same rule that
// keeps the agent out. What it knows travels; what it knows is not written
// by it.
func (a *Applier) RecordRequirements(ctx context.Context, req agent.RegisterRevisionInput) error {
	resource := schema.GroupVersionResource{
		Group: v1.Group, Version: v1.Version, Resource: "pipelinerevisions",
	}

	client := a.client.Resource(resource).Namespace(req.Namespace)

	existing, err := client.Get(ctx, req.Revision, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("ревизия %s не читается: %w", req.Revision, err)
	}

	requires := make([]any, 0, len(req.Requires))
	for _, kind := range req.Requires {
		requires = append(requires, map[string]any{
			"group": kind.Group, "version": kind.Version, "kind": kind.Kind,
		})
	}

	if err := unstructured.SetNestedSlice(existing.Object, requires, "status", "requires"); err != nil {
		return fmt.Errorf("требования не собрались: %w", err)
	}

	if _, err := client.UpdateStatus(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("требования ревизии %s не записались: %w", req.Revision, err)
	}

	return nil
}
