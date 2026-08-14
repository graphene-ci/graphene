package worker

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/graphene-ci/graphene/sdk/agent"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

// Presign issues one short-lived place to put one artifact.
func (a *Applier) Presign(ctx context.Context, req agent.PresignInput) (agent.PresignOutput, error) {
	// Ключ выводится из прогона и имени: спросить дважды — получить одно
	// место, а не два. То же правило, что у имён записей.
	key := req.Owner.Name + "/" + req.Name

	link, err := a.storage.Presign(ctx, key)
	if err != nil {
		return agent.PresignOutput{}, err
	}

	return agent.PresignOutput{URL: link, Key: key}, nil
}

// RecordArtifact writes down what was uploaded.
func (a *Applier) RecordArtifact(ctx context.Context, req agent.RecordArtifactInput) error {
	resource := schema.GroupVersionResource{Group: v1.Group, Version: v1.Version, Resource: "artifacts"}
	client := a.client.Resource(resource).Namespace(req.Owner.Namespace)

	name := agent.ObjectName(req.Owner, req.Name)

	fresh := &unstructured.Unstructured{Object: map[string]any{
		fieldVersion: v1.GroupVersion.String(),
		fieldKind:    "Artifact",
		fieldMetadata: map[string]any{
			fieldName: name,
			"ownerReferences": []any{map[string]any{
				fieldVersion: v1.GroupVersion.String(),
				fieldKind:    kindRun,
				fieldName:    req.Owner.Name,
				fieldUID:     req.Owner.UID,
			}},
			fieldLabels: map[string]any{LabelRun: req.Owner.Name, LabelManaged: yes},
		},
		"spec": map[string]any{
			"runRef":  map[string]any{fieldName: req.Owner.Name},
			fieldName: req.Name,
			"key":     req.Key,
		},
	}}

	created, err := client.Create(ctx, fresh, metav1.CreateOptions{})
	if err != nil {
		existing, getErr := client.Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("артефакт %s не записался: %w", name, err)
		}

		created = existing
	}

	status := map[string]any{"digest": req.Digest, "size": req.Size}
	if err := unstructured.SetNestedMap(created.Object, status, "status"); err != nil {
		return fmt.Errorf("статус артефакта не собрался: %w", err)
	}

	if _, err := client.UpdateStatus(ctx, created, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("дайджест артефакта не записался: %w", err)
	}

	return nil
}
