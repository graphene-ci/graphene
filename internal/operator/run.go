// Package operator is what makes a record happen. It is the seam between
// the two halves of the system: the cluster holds what exists, Temporal
// holds how far things got, and this is the only place that knows both.
package operator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

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

// followEvery is how often a running workflow is asked how far it got.
//
// Polling, because nothing else wakes this controller: the record stops
// changing the moment the workflow starts, and Temporal does not write to
// the cluster. A workflow could report its own end through one last
// activity — but a terminated workflow never runs one, so the poll would
// still have to exist as the safety net. One mechanism, not two.
const followEvery = 5 * time.Second

// Refusals a run can meet before it starts.
var (
	// ErrNoRevision means the run points at a revision that is not there.
	ErrNoRevision = errors.New("ревизия не найдена")
	// ErrKindMissing means the cluster does not serve a kind the pipeline
	// needs.
	ErrKindMissing = errors.New("в кластере нет вида, который нужен пайплайну")
)

// Known answers whether the cluster serves a kind.
//
// A function rather than a discovery client so that the reconciler can be
// checked without a cluster, and so that the cache — with its own failure
// modes — stays at the edge.
type Known func(ctx context.Context, kind agent.Kind) (bool, error)

// RunReconciler turns a Run record into a running workflow, and a running
// workflow back into the record's phase.
type RunReconciler struct {
	kube     client.Client
	temporal Temporal
	// Known is consulted before a run starts. Nil means nothing is
	// checked, which is what a test that does not care about
	// requirements wants.
	Known Known
}

// NewRunReconciler builds one.
func NewRunReconciler(kube client.Client, temporal Temporal, known Known) *RunReconciler {
	return &RunReconciler{kube: kube, temporal: temporal, Known: known}
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
		return ctrl.Result{RequeueAfter: followEvery}, r.follow(ctx, &run)
	}

	return ctrl.Result{RequeueAfter: followEvery}, r.start(ctx, &run)
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

	// Отказ ДО старта, а не на середине. Половина построенного стенда
	// стоит денег и требует уборки; отказ до первого шага не стоит
	// ничего и говорит, что именно поставить.
	if missing := r.missing(ctx, &revision); len(missing) > 0 {
		return r.refuse(ctx, run, fmt.Errorf("%w: %s — поставьте провайдер, который их обслуживает",
			ErrKindMissing, strings.Join(missing, ", ")))
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

// missing lists the kinds the revision needs and the cluster does not
// serve.
//
// Whether the provider serving them is HEALTHY is not checked, and that is
// deliberate: without an installed provider the refusal is already clear,
// and "the provider is alive" has no single answer — a provider can be up
// and unable to reach its cloud, or down and about to come back.
func (r *RunReconciler) missing(ctx context.Context, revision *v1.PipelineRevision) []string {
	if r.Known == nil {
		return nil
	}

	var missing []string

	for _, required := range revision.Status.Requires {
		kind := agent.Kind{Group: required.Group, Version: required.Version, Kind: required.Kind}

		known, err := r.Known(ctx, kind)
		if err != nil || known {
			// Ошибка discovery — не повод отказать прогону: тогда
			// икота кэша останавливала бы работу, а не сообщала о
			// пропаже.
			continue
		}

		missing = append(missing, required.Kind+"."+required.Group)
	}

	return missing
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
