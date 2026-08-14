package worker

import (
	"context"
	"fmt"
	"strings"
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
		if err := setOurs(existing, req); err != nil {
			return err
		}

		if _, err := client.UpdateStatus(ctx, existing, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("машина %s не обновилась: %w", req.Machine, err)
		}

		return nil
	}

	metadata := map[string]any{fieldName: req.Machine}
	if owner := a.ironOf(ctx, req); owner != nil {
		metadata["ownerReferences"] = []any{owner}
	}

	fresh := &unstructured.Unstructured{Object: map[string]any{
		fieldVersion: v1.GroupVersion.String(),
		fieldKind:    "Machine",
		"metadata":   metadata,
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

// ironOf finds the record that brought this machine's iron into being, if
// anything did.
//
// This narrows a decision from M2 that turned out too wide. "A Machine has
// no owner, because the iron outlives the run that used it" is true of a
// machine somebody else built and lent us. It is false of a cloud VM the
// run itself created: that iron dies with the run, and a record outliving
// its iron is a record about nothing.
//
// So: the owner is whatever made the iron, when we can see it. An agent
// that came up on somebody's laptop belongs to nobody and is never
// collected — losing the labels and taints a person put there would be
// worse than keeping a row that says "not ready".
//
// The link is the installation's name: a pipeline calls the machine and
// the record it applies by the same memo, so `<run>-<memo>` names both. A
// run name has dashes of its own, so the split is not guessed — each
// candidate is checked against a Run that actually exists.
func (a *Applier) ironOf(ctx context.Context, req agent.RegisterInput) map[string]any {
	for run, memo := range splits(req.Machine) {
		kinds := a.kindsOf(ctx, req.Namespace, run)
		if len(kinds) == 0 {
			continue
		}

		if owner := a.recordOf(ctx, req.Namespace, run, memo, kinds); owner != nil {
			return owner
		}
	}

	return nil
}

// splits yields every way the installation's name could divide into a run
// and a memo, shortest run first.
func splits(installation string) map[string]string {
	ways := map[string]string{}
	parts := strings.Split(installation, "-")

	for i := 1; i < len(parts); i++ {
		ways[strings.Join(parts[:i], "-")] = strings.Join(parts[i:], "-")
	}

	return ways
}

// kindsOf is what that run could have created — the revision's
// requirements once more, now as the list of places to look for the iron.
func (a *Applier) kindsOf(ctx context.Context, namespace, run string) []schema.GroupVersionResource {
	runs := schema.GroupVersionResource{Group: v1.Group, Version: v1.Version, Resource: "runs"}

	record, err := a.client.Resource(runs).Namespace(namespace).Get(ctx, run, metav1.GetOptions{})
	if err != nil {
		return nil
	}

	revisionName, found, err := unstructured.NestedString(record.Object, "spec", "revisionRef", fieldName)
	if err != nil || !found {
		return nil
	}

	revisions := schema.GroupVersionResource{Group: v1.Group, Version: v1.Version, Resource: "pipelinerevisions"}

	revision, err := a.client.Resource(revisions).Namespace(namespace).Get(ctx, revisionName, metav1.GetOptions{})
	if err != nil {
		return nil
	}

	required, found, err := unstructured.NestedSlice(revision.Object, "status", "requires")
	if err != nil || !found {
		return nil
	}

	resources := make([]schema.GroupVersionResource, 0, len(required))

	for _, one := range required {
		kind, ok := one.(map[string]any)
		if !ok {
			continue
		}

		gvk := schema.GroupVersionKind{
			Group:   fmt.Sprint(kind["group"]),
			Version: fmt.Sprint(kind["version"]),
			Kind:    fmt.Sprint(kind[fieldKind]),
		}

		resource, _, err := a.resolve(ctx, gvk)
		if err != nil {
			continue
		}

		resources = append(resources, resource)
	}

	return resources
}

// recordOf looks for the record this run made under this memo.
func (a *Applier) recordOf(
	ctx context.Context, namespace, run, memo string, kinds []schema.GroupVersionResource,
) map[string]any {
	ours := metav1.ListOptions{LabelSelector: LabelRun + "=" + run}

	for _, resource := range kinds {
		list, err := a.client.Resource(resource).Namespace(namespace).List(ctx, ours)
		if err != nil {
			continue
		}

		for i := range list.Items {
			record := &list.Items[i]
			if record.GetAnnotations()[AnnotationMemo] != memo {
				continue
			}

			return map[string]any{
				fieldVersion: record.GetAPIVersion(),
				fieldKind:    record.GetKind(),
				fieldName:    record.GetName(),
				"uid":        string(record.GetUID()),
			}
		}
	}

	return nil
}

// setOurs writes only what the agent knows, leaving the rest of the status
// alone.
//
// Writing the whole status here was a real bug and a costly one: the agent
// marks itself every few seconds, and each mark wiped the conditions the
// operator wrote and the claim another run had just taken. A machine could
// not stay claimed for longer than one heartbeat.
//
// The lesson is the same one the operator learned an hour earlier: when
// two processes write one object, each writes its own fields, never the
// whole of it.
func setOurs(record *unstructured.Unstructured, req agent.RegisterInput) error {
	if err := unstructured.SetNestedField(record.Object, req.Queue, "status", "queue"); err != nil {
		return fmt.Errorf("очередь не записалась: %w", err)
	}

	if len(req.Facts) == 0 {
		return nil
	}

	if err := unstructured.SetNestedStringMap(record.Object, req.Facts, "status", "facts"); err != nil {
		return fmt.Errorf("факты не записались: %w", err)
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
