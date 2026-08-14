package worker

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/graphene-ci/graphene/sdk/agent"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

// LabelStand is the stand a record now belongs to.
const LabelStand = v1.Group + "/stand"

// Keep hands what a run made to a stand that outlives it.
//
// Ownership moves for real: the owner reference is repointed and the run's
// label is replaced by the stand's. After this the run's own teardown finds
// nothing of its own, which is the point — the records are not its any
// more, and when it is deleted they stay.
func (a *Applier) Keep(ctx context.Context, req agent.KeepInput) (agent.KeepOutput, error) {
	stand, err := a.stand(ctx, req)
	if err != nil {
		return agent.KeepOutput{}, err
	}

	for _, ref := range req.Refs {
		if err := a.handOver(ctx, ref, stand); err != nil {
			return agent.KeepOutput{}, err
		}
	}

	return agent.KeepOutput{Stand: stand.GetName()}, nil
}

// stand makes the record that will answer for these machines.
//
// Named after the run, so asking twice makes one stand rather than two —
// the activity runs at least once, like every other.
func (a *Applier) stand(ctx context.Context, req agent.KeepInput) (*unstructured.Unstructured, error) {
	resource := schema.GroupVersionResource{Group: v1.Group, Version: v1.Version, Resource: "stands"}
	client := a.client.Resource(resource).Namespace(req.Owner.Namespace)

	fresh := &unstructured.Unstructured{Object: map[string]any{
		fieldVersion:  v1.GroupVersion.String(),
		fieldKind:     "Stand",
		fieldMetadata: map[string]any{fieldName: req.Owner.Name},
		"spec": map[string]any{
			"until":  req.Until.UTC().Format(metav1.RFC3339Micro),
			"runRef": map[string]any{fieldName: req.Owner.Name},
			"reason": req.Reason,
		},
	}}

	created, err := client.Create(ctx, fresh, metav1.CreateOptions{})
	if err == nil {
		return created, nil
	}

	if !apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("стенд %s не завёлся: %w", req.Owner.Name, err)
	}

	existing, err := client.Get(ctx, req.Owner.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("стенд %s есть, но не читается: %w", req.Owner.Name, err)
	}

	return existing, nil
}

// handOver repoints one record at the stand.
func (a *Applier) handOver(ctx context.Context, ref agent.ObjectRef, stand *unstructured.Unstructured) error {
	gvk := schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind)

	resource, namespaced, err := a.resolve(ctx, gvk)
	if err != nil {
		return fmt.Errorf("вид %s не обслуживается кластером: %w", gvk, err)
	}

	client := a.forResource(resource, namespaced, ref.Namespace)

	record, err := client.Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		// Уже нет — оставлять нечего.
		return nil
	}

	if err != nil {
		return fmt.Errorf("запись %s не читается: %w", ref.Name, err)
	}

	record.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: v1.GroupVersion.String(),
		Kind:       "Stand",
		Name:       stand.GetName(),
		UID:        stand.GetUID(),
	}})

	labels := record.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	// Метка прогона снимается: по ней прогон ищет своё при сносе, и
	// оставить её значило бы отдать стенд и тут же его снести.
	delete(labels, LabelRun)
	labels[LabelStand] = stand.GetName()
	record.SetLabels(labels)

	if _, err := client.Update(ctx, record, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("запись %s не передалась стенду: %w", ref.Name, err)
	}

	return nil
}
