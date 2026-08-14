package cli

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

// Cancel asks a run to stop.
//
// It sets the wish on the record and returns. Carrying it to Temporal is
// the operator's job, and that is deliberate: the person who asked may
// close their laptop a second later, and the run must still stop.
func Cancel(ctx context.Context, kube client.Client, namespace, name string) error {
	var run v1.Run

	key := client.ObjectKey{Namespace: namespace, Name: name}
	if err := kube.Get(ctx, key, &run); err != nil {
		return fmt.Errorf("прогон не читается: %w", err)
	}

	if run.Status.Phase.Terminal() {
		return fmt.Errorf("%w: %s", ErrAlreadyOver, run.Status.Phase)
	}

	if run.Spec.Cancel {
		return nil
	}

	run.Spec.Cancel = true
	if err := kube.Update(ctx, &run); err != nil {
		return fmt.Errorf("отмена не записалась: %w", err)
	}

	return nil
}
