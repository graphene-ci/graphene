// Package worker performs what a pipeline asks for: it puts records into
// the cluster and takes them away again. This is the side that knows about
// kubernetes; the SDK on the other side of pkg/agent does not.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/graphene-ci/graphene/internal/kube"
	"github.com/graphene-ci/graphene/sdk/agent"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

// How a record says who made it and why. The labels are for selecting, the
// annotation is for a person reading `kubectl describe` and wondering which
// line of which pipeline produced this machine.
const (
	// LabelRun is the run that owns the record.
	LabelRun = v1.Group + "/run"
	// LabelManaged marks everything we made, whatever kind it is.
	LabelManaged = v1.Group + "/managed"
	// AnnotationMemo is what the pipeline called this thing.
	AnnotationMemo = v1.Group + "/memo"
)

// Поля манифеста, которые мы собираем руками. Строками они встречаются
// в трёх местах, и линтер прав: одно написание вместо трёх.
const (
	fieldKind    = "kind"
	fieldName    = "name"
	fieldVersion = "apiVersion"
)

// ErrNoKind means the manifest did not say what it is.
var ErrNoKind = errors.New("в манифесте нет apiVersion или kind")

// Applier performs the activities a pipeline schedules.
type Applier struct {
	client  dynamic.Interface
	resolve kube.Resolver
}

// NewApplier builds one.
func NewApplier(client dynamic.Interface, resolve kube.Resolver) *Applier {
	return &Applier{client: client, resolve: resolve}
}

// Apply makes the record exist, owned by the run, and reports where it is.
//
// It is called at least once for every record a pipeline asks for, and may
// be called many times for the same one — Temporal retries an activity
// until it succeeds, and a worker can die after writing but before
// answering. So the name is derived from the run and the memo rather than
// generated, and a record that is already there is the expected answer.
func (a *Applier) Apply(ctx context.Context, req agent.ApplyInput) (agent.ApplyOutput, error) {
	obj := &unstructured.Unstructured{}
	if err := json.Unmarshal(req.Manifest, &obj.Object); err != nil {
		return agent.ApplyOutput{}, fmt.Errorf("манифест не разобрался: %w", err)
	}

	gvk := obj.GroupVersionKind()
	if gvk.Kind == "" || gvk.Version == "" {
		return agent.ApplyOutput{}, ErrNoKind
	}

	resource, namespaced, err := a.resolve(ctx, gvk)
	if err != nil {
		return agent.ApplyOutput{}, fmt.Errorf("вид %s не обслуживается кластером: %w", gvk, err)
	}

	name := agent.ObjectName(req.Owner, req.Name)
	obj.SetName(name)
	obj.SetNamespace(req.Owner.Namespace)
	mark(obj, req)

	client := a.forResource(resource, namespaced, req.Owner.Namespace)

	created, err := client.Create(ctx, obj, metav1.CreateOptions{})
	if err == nil {
		return agent.ApplyOutput{Ref: refOf(created), Created: true}, nil
	}

	if !apierrors.IsAlreadyExists(err) {
		return agent.ApplyOutput{}, fmt.Errorf("запись %s не создалась: %w", name, err)
	}

	// Already there: this attempt is not the first. What exists is what a
	// previous attempt made, and it is the answer.
	existing, err := client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return agent.ApplyOutput{}, fmt.Errorf("запись %s есть, но не читается: %w", name, err)
	}

	return agent.ApplyOutput{Ref: refOf(existing), Created: false}, nil
}

// Teardown removes what the run made. Records that are already gone are not
// a failure: the garbage collector or a person may have got there first,
// and teardown's promise is that nothing is left, not that it did the
// leaving.
func (a *Applier) Teardown(ctx context.Context, in agent.TeardownInput) (agent.TeardownOutput, error) {
	out := agent.TeardownOutput{Removed: make([]agent.ObjectRef, 0, len(in.Refs))}

	for _, ref := range in.Refs {
		gvk := schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind)

		resource, namespaced, err := a.resolve(ctx, gvk)
		if err != nil {
			return out, fmt.Errorf("вид %s не обслуживается кластером: %w", gvk, err)
		}

		client := a.forResource(resource, namespaced, ref.Namespace)

		err = client.Delete(ctx, ref.Name, metav1.DeleteOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}

		if err != nil {
			return out, fmt.Errorf("запись %s не удалилась: %w", ref.Name, err)
		}

		out.Removed = append(out.Removed, ref)
	}

	return out, nil
}

func (a *Applier) forResource(
	resource schema.GroupVersionResource, namespaced bool, namespace string,
) dynamic.ResourceInterface {
	if namespaced {
		return a.client.Resource(resource).Namespace(namespace)
	}

	return a.client.Resource(resource)
}

// mark writes who owns the record and what the pipeline called it.
//
// The owner reference carries the UID, and that is the point: a name can be
// reused by a later run, and a reference by name alone would hand somebody
// else's machines to it. It is also what makes the cluster's own garbage
// collector the safety net under teardown.
func mark(obj *unstructured.Unstructured, req agent.ApplyInput) {
	obj.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: v1.GroupVersion.String(),
		Kind:       "Run",
		Name:       req.Owner.Name,
		UID:        types.UID(req.Owner.UID),
	}})

	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	labels[LabelRun] = req.Owner.Name
	labels[LabelManaged] = "true"
	obj.SetLabels(labels)

	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}

	annotations[AnnotationMemo] = req.Name
	obj.SetAnnotations(annotations)
}

func refOf(obj *unstructured.Unstructured) agent.ObjectRef {
	return agent.ObjectRef{
		APIVersion: obj.GetAPIVersion(),
		Kind:       obj.GetKind(),
		Namespace:  obj.GetNamespace(),
		Name:       obj.GetName(),
		UID:        string(obj.GetUID()),
	}
}
