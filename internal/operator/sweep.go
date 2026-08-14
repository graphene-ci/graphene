package operator

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/graphene-ci/graphene/internal/kube"
	"github.com/graphene-ci/graphene/internal/worker"
	"github.com/graphene-ci/graphene/sdk/agent"
)

// Sweeper removes what a run owns and says how much is left.
//
// "How much is left" rather than "done": deleting a cloud resource asks for
// deletion, it does not perform it. A virtual machine goes away in its own
// time, and until it has, the run is not finished being cleaned up. That is
// the whole reason the finalizer exists.
type Sweeper interface {
	Sweep(ctx context.Context, owner agent.OwnerRef, kinds []agent.Kind) (int, error)
}

// DynamicSweeper removes records of any kind, including kinds that did not
// exist when this was compiled.
type DynamicSweeper struct {
	client  dynamic.Interface
	resolve kube.Resolver
}

// NewSweeper builds one.
func NewSweeper(client dynamic.Interface, resolve kube.Resolver) *DynamicSweeper {
	return &DynamicSweeper{client: client, resolve: resolve}
}

// Sweep asks for everything the run owns to go away and reports how many
// records are still there.
//
// It works by label rather than by a remembered list: the run's own memory
// lives in Temporal's history, and a control plane cleaning up after a run
// whose workflow is already gone must not depend on it.
func (s *DynamicSweeper) Sweep(ctx context.Context, owner agent.OwnerRef, kinds []agent.Kind) (int, error) {
	ours := metav1.ListOptions{LabelSelector: worker.LabelRun + "=" + owner.Name}
	left := 0

	for _, kind := range kinds {
		gvk := schema.GroupVersionKind{Group: kind.Group, Version: kind.Version, Kind: kind.Kind}

		resource, namespaced, err := s.resolve(ctx, gvk)
		if err != nil {
			// Вида больше нет в кластере — значит, и записей его нет.
			// Провайдер могли снять, пока прогон стоял.
			continue
		}

		client := s.forResource(resource, namespaced, owner.Namespace)

		list, err := client.List(ctx, ours)
		if err != nil {
			return left, fmt.Errorf("записи вида %s не читаются: %w", gvk, err)
		}

		for i := range list.Items {
			left++

			record := &list.Items[i]
			if record.GetDeletionTimestamp() != nil {
				// Уже уходит. Торопить нечем: облако удаляет в своём
				// темпе, и повторное удаление его не ускорит.
				continue
			}

			err := client.Delete(ctx, record.GetName(), metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				return left, fmt.Errorf("запись %s не удалилась: %w", record.GetName(), err)
			}
		}
	}

	return left, nil
}

func (s *DynamicSweeper) forResource(
	resource schema.GroupVersionResource, namespaced bool, namespace string,
) dynamic.ResourceInterface {
	if namespaced {
		return s.client.Resource(resource).Namespace(namespace)
	}

	return s.client.Resource(resource)
}
