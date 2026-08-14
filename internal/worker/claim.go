package worker

import (
	"context"
	"errors"
	"fmt"
	"sort"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/graphene-ci/graphene/sdk/agent"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

// ErrNotEnough means the pool did not have as many machines as were asked
// for. It is not a failure of the run: the run waits and asks again.
var ErrNotEnough = errors.New("свободных машин меньше, чем нужно")

// Claim takes machines out of the pool for a run.
//
// All of them or none, and that is the whole of it. Taking two of three
// and waiting for the last one is how a fleet gets occupied by runs that
// cannot do any work: two runs each holding half of what they need wait
// for each other forever, and nobody is there to arbitrate.
//
// The order is fixed — by name — and that is what makes the deadlock
// impossible rather than unlikely. Two runs walking the same list in the
// same order cannot each get the half the other needs.
func (a *Applier) Claim(ctx context.Context, req agent.ClaimInput) (agent.ClaimOutput, error) {
	client := a.client.Resource(machinesResource()).Namespace(req.Owner.Namespace)

	free, err := a.candidates(ctx, req)
	if err != nil {
		return agent.ClaimOutput{}, err
	}

	taken := make([]string, 0, req.Count)

	for _, name := range free {
		if len(taken) == req.Count {
			break
		}

		ok, err := a.take(ctx, client, name, req)
		if err != nil {
			a.release(ctx, client, taken, req)

			return agent.ClaimOutput{}, err
		}

		if ok {
			taken = append(taken, name)
		}
	}

	if len(taken) < req.Count {
		// Отпускаем всё взятое. Держать половину — это занять парк и не
		// сделать работу.
		a.release(ctx, client, taken, req)

		return agent.ClaimOutput{}, fmt.Errorf("%w: нужно %d, взято %d", ErrNotEnough, req.Count, len(taken))
	}

	return agent.ClaimOutput{Machines: taken}, nil
}

// candidates lists free machines that suit, in a fixed order.
func (a *Applier) candidates(ctx context.Context, req agent.ClaimInput) ([]string, error) {
	options := metav1.ListOptions{}
	if len(req.Match.Labels) > 0 {
		options.LabelSelector = labels.Set(req.Match.Labels).String()
	}

	list, err := a.client.Resource(machinesResource()).Namespace(req.Owner.Namespace).List(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("машины не читаются: %w", err)
	}

	names := make([]string, 0, len(list.Items))

	for i := range list.Items {
		machine, err := asMachine(&list.Items[i])
		if err != nil {
			continue
		}

		if suits(machine, req) {
			names = append(names, machine.Name)
		}
	}

	// Порядок фиксирован, и это не косметика: два прогона, идущие по
	// одному списку в одном порядке, не могут взять по половине.
	sort.Strings(names)

	return names, nil
}

// suits answers whether this machine can be given to this run.
func suits(machine *v1.Machine, req agent.ClaimInput) bool {
	if machine.Status.Claim != nil && machine.Status.Claim.Name != req.Owner.Name {
		return false
	}

	if !ready(machine) {
		return false
	}

	for _, taint := range machine.Spec.Taints {
		if !tolerated(taint, req.Match.Tolerate) {
			return false
		}
	}

	return true
}

// ready answers whether the machine is answering right now. A machine that
// stopped answering is not one to hand out.
func ready(machine *v1.Machine) bool {
	for _, condition := range machine.Status.Conditions {
		if condition.Type == v1.ConditionReady {
			return condition.Status == metav1.ConditionTrue
		}
	}

	return false
}

// tolerated answers whether this work can live with this taint.
func tolerated(taint v1.Taint, tolerations []string) bool {
	wanted := taint.Key
	if taint.Value != "" {
		wanted += "=" + taint.Value
	}

	for _, one := range tolerations {
		if one == wanted || one == taint.Key {
			return true
		}
	}

	return false
}

// take writes the claim onto one machine, and says whether it won.
//
// The write carries the version it read, so two runs reaching for the same
// machine cannot both succeed: the loser is told the record moved, reads
// again, and goes on to the next one. No lock, and no scheduler — a
// scheduler is what you need when somebody must decide who deserves the
// scarce thing, and all we need is that two do not take one.
func (a *Applier) take(
	ctx context.Context, client dynamic.ResourceInterface, name string, req agent.ClaimInput,
) (bool, error) {
	record, err := client.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("машина %s не читается: %w", name, err)
	}

	machine, err := asMachine(record)
	if err != nil {
		// Запись есть, но не разбирается: не наше дело чинить чужой
		// мусор посреди захвата — просто не берём её.
		return false, nil //nolint:nilerr // неразбираемая машина это ответ «не эта», а не отказ
	}

	if !suits(machine, req) {
		return false, nil
	}

	now := metav1.Now()
	claim := map[string]any{
		fieldKind: "Run",
		fieldName: req.Owner.Name,
		"uid":     req.Owner.UID,
		"since":   now.UTC().Format(metav1.RFC3339Micro),
	}

	if err := unstructured.SetNestedMap(record.Object, claim, "status", "claim"); err != nil {
		return false, fmt.Errorf("захват не собрался: %w", err)
	}

	_, err = client.UpdateStatus(ctx, record, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		// Кто-то успел раньше. Это не отказ, это ответ.
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("машина %s не захватилась: %w", name, err)
	}

	return true, nil
}

// release gives machines back.
func (a *Applier) release(ctx context.Context, client dynamic.ResourceInterface, names []string, req agent.ClaimInput) {
	for _, name := range names {
		record, err := client.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			continue
		}

		machine, err := asMachine(record)
		if err != nil || machine.Status.Claim == nil || machine.Status.Claim.Name != req.Owner.Name {
			continue
		}

		unstructured.RemoveNestedField(record.Object, "status", "claim")

		_, _ = client.UpdateStatus(ctx, record, metav1.UpdateOptions{})
	}
}

func machinesResource() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: v1.Group, Version: v1.Version, Resource: "machines"}
}

func asMachine(record *unstructured.Unstructured) (*v1.Machine, error) {
	var machine v1.Machine

	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(record.Object, &machine); err != nil {
		return nil, fmt.Errorf("машина не разобралась: %w", err)
	}

	return &machine, nil
}
