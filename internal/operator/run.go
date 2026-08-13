// Package operator is what makes a record happen. It is the seam between
// the two halves of the system: the cluster holds what exists, Temporal
// holds how far things got, and this is the only place that knows both.
package operator

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/graphene-ci/graphene/api/v1"
	"github.com/graphene-ci/graphene/pkg/agent"
)

// StartRequest is everything needed to put a run in motion.
type StartRequest struct {
	// WorkflowID is always the run record's own name. That is what makes
	// starting safe to repeat: a second attempt collides with the first
	// instead of starting a second run of the same pipeline.
	WorkflowID string
	// Queue belongs to the revision, so that a worker of an old revision
	// never picks up work meant for a new one.
	Queue string
	Input agent.RunInput
}

// Temporal is the half of the world this operator does not own. It is an
// interface so that the reconciler can be checked without one running.
type Temporal interface {
	// Start begins the workflow and reports its attempt id. Starting one
	// that already exists is not an error: it returns the existing one.
	Start(ctx context.Context, req StartRequest) (string, error)
	// Phase reports how far a workflow got.
	Phase(ctx context.Context, workflowID string) (v1.RunPhase, string, error)
}

// ErrNoRevision means the run points at a revision that is not there.
var ErrNoRevision = errors.New("ревизия не найдена")

// RunReconciler turns a Run record into a running workflow, and a running
// workflow back into the record's phase.
type RunReconciler struct {
	kube     client.Client
	temporal Temporal
}

// NewRunReconciler builds one.
func NewRunReconciler(kube client.Client, temporal Temporal) *RunReconciler {
	return &RunReconciler{kube: kube, temporal: temporal}
}

// Reconcile brings one run to where it should be.
//
// It runs many times for the same record — on every change, and again after
// every restart of this process. Everything it does must therefore be safe
// to do again, which is why the workflow id is the record's name and why a
// finished run is left alone.
func (r *RunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var run v1.Run
	if err := r.kube.Get(ctx, req.NamespacedName, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if run.Status.Phase.Terminal() {
		return ctrl.Result{}, nil
	}

	if run.Status.WorkflowID != "" {
		return ctrl.Result{}, r.follow(ctx, &run)
	}

	return ctrl.Result{}, r.start(ctx, &run)
}

// start puts the run in motion, or records why it cannot be.
func (r *RunReconciler) start(ctx context.Context, run *v1.Run) error {
	var revision v1.PipelineRevision

	key := types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.RevisionRef.Name}
	if err := r.kube.Get(ctx, key, &revision); err != nil {
		if apierrors.IsNotFound(err) {
			return r.refuse(ctx, run, fmt.Errorf("%w: %s", ErrNoRevision, run.Spec.RevisionRef.Name))
		}

		return fmt.Errorf("ревизия не читается: %w", err)
	}

	if err := run.Spec.Validate(); err != nil {
		return r.refuse(ctx, run, err)
	}

	temporalRunID, err := r.temporal.Start(ctx, StartRequest{
		WorkflowID: run.Name,
		Queue:      revision.Spec.Queue,
		Input: agent.RunInput{
			Owner:  ownerOf(run),
			Params: paramsOf(run),
		},
	})
	if err != nil {
		return fmt.Errorf("воркфлоу не стартовал: %w", err)
	}

	now := metav1.Now()
	run.Status.Phase = v1.RunRunning
	run.Status.WorkflowID = run.Name
	run.Status.TemporalRunID = temporalRunID
	run.Status.StartedAt = &now

	if err := r.kube.Status().Update(ctx, run); err != nil {
		return fmt.Errorf("статус прогона не записался: %w", err)
	}

	return nil
}

// follow copies how far the workflow got back into the record.
func (r *RunReconciler) follow(ctx context.Context, run *v1.Run) error {
	phase, reason, err := r.temporal.Phase(ctx, run.Status.WorkflowID)
	if err != nil {
		return fmt.Errorf("состояние воркфлоу не прочиталось: %w", err)
	}

	if phase == run.Status.Phase {
		return nil
	}

	run.Status.Phase = phase
	run.Status.Reason = reason

	if phase.Terminal() {
		now := metav1.Now()
		run.Status.FinishedAt = &now
	}

	if err := r.kube.Status().Update(ctx, run); err != nil {
		return fmt.Errorf("статус прогона не записался: %w", err)
	}

	return nil
}

// refuse records why the run will not happen. It is not an error returned
// upward: nothing about retrying will make a missing revision appear, and a
// controller that keeps retrying a settled refusal only fills the log.
func (r *RunReconciler) refuse(ctx context.Context, run *v1.Run, why error) error {
	now := metav1.Now()
	run.Status.Phase = v1.RunFailed
	run.Status.Reason = why.Error()
	run.Status.FinishedAt = &now

	if err := r.kube.Status().Update(ctx, run); err != nil {
		return fmt.Errorf("статус прогона не записался: %w", err)
	}

	return nil
}

// SetupWithManager wires the reconciler to the records it answers for.
func (r *RunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := ctrl.NewControllerManagedBy(mgr).For(&v1.Run{}).Complete(r); err != nil {
		return fmt.Errorf("контроллер прогонов не собрался: %w", err)
	}

	return nil
}

func ownerOf(run *v1.Run) agent.OwnerRef {
	return agent.OwnerRef{
		Namespace: run.Namespace,
		Name:      run.Name,
		UID:       string(run.UID),
	}
}

func paramsOf(run *v1.Run) []byte {
	if run.Spec.Params == nil {
		return nil
	}

	return run.Spec.Params.Raw
}
