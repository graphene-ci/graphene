package worker

import (
	"context"
	"fmt"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/graphene-ci/graphene/sdk/agent"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

// leaseSeconds comes from the contract: the agent, this writer and the
// operator must all mean the same thing by silence.
const leaseSeconds = agent.LeaseSeconds

// Register records a machine and renews its mark.
//
// The agent cannot do this itself: it has no access to the cluster, and a
// token that let a machine write records would be a key to the cluster
// handed out with every installation. So the agent asks, through Temporal,
// and this — running in our own worker, with our own permissions — writes.
func (a *Applier) Register(ctx context.Context, req agent.RegisterInput) error {
	if err := a.upsertMachine(ctx, req); err != nil {
		return err
	}

	return a.renewLease(ctx, req)
}

// upsertMachine creates the machine record or refreshes what the agent
// reports about it. It never touches the spec: the spec is the person's,
// and an agent that overwrote it would undo a taint somebody set.
func (a *Applier) upsertMachine(ctx context.Context, req agent.RegisterInput) error {
	resource := schema.GroupVersionResource{Group: v1.Group, Version: v1.Version, Resource: "machines"}
	client := a.client.Resource(resource).Namespace(req.Namespace)

	status := map[string]any{"queue": req.Queue}
	if len(req.Facts) > 0 {
		status["facts"] = toAny(req.Facts)
	}

	existing, err := client.Get(ctx, req.Machine, metav1.GetOptions{})
	if err == nil {
		if err := unstructured.SetNestedMap(existing.Object, status, "status"); err != nil {
			return fmt.Errorf("статус машины не собрался: %w", err)
		}

		if _, err := client.UpdateStatus(ctx, existing, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("машина %s не обновилась: %w", req.Machine, err)
		}

		return nil
	}

	fresh := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1.GroupVersion.String(),
		fieldKind:    "Machine",
		"metadata":   map[string]any{fieldName: req.Machine},
		"status":     status,
	}}

	created, err := client.Create(ctx, fresh, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("машина %s не завелась: %w", req.Machine, err)
	}

	// Create ignores the status on the way in when the kind has a status
	// subresource, so what the agent said has to be written a second time.
	if err := unstructured.SetNestedMap(created.Object, status, "status"); err != nil {
		return fmt.Errorf("статус машины не собрался: %w", err)
	}

	if _, err := client.UpdateStatus(ctx, created, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("статус машины %s не записался: %w", req.Machine, err)
	}

	return nil
}

// renewLease marks the machine as still answering.
func (a *Applier) renewLease(ctx context.Context, req agent.RegisterInput) error {
	resource := schema.GroupVersionResource{Group: "coordination.k8s.io", Version: "v1", Resource: "leases"}
	client := a.client.Resource(resource).Namespace(req.Namespace)

	seconds := int32(leaseSeconds)
	renew := metav1.NewMicroTime(time.Now())

	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: req.Machine, Namespace: req.Namespace},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &req.Queue,
			LeaseDurationSeconds: &seconds,
			RenewTime:            &renew,
		},
	}

	fields, err := runtime.DefaultUnstructuredConverter.ToUnstructured(lease)
	if err != nil {
		return fmt.Errorf("аренда не собралась: %w", err)
	}

	fields["apiVersion"] = "coordination.k8s.io/v1"
	fields["kind"] = "Lease"

	obj := &unstructured.Unstructured{Object: fields}

	existing, err := client.Get(ctx, req.Machine, metav1.GetOptions{})
	if err == nil {
		obj.SetResourceVersion(existing.GetResourceVersion())

		if _, err := client.Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("аренда %s не продлилась: %w", req.Machine, err)
		}

		return nil
	}

	if _, err := client.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("аренда %s не завелась: %w", req.Machine, err)
	}

	return nil
}

func toAny(from map[string]string) map[string]any {
	to := make(map[string]any, len(from))
	for key, value := range from {
		to[key] = value
	}

	return to
}
