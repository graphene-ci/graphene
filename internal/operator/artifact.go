package operator

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

// bytesFinalizer keeps an artifact record alive until its bytes are gone.
//
// Nothing else can do this. The record is owned by its run, so the
// cluster's own collector removes it the moment the run goes — and the
// collector knows nothing about a bucket. Without the finalizer, every
// swept run would leave its megabytes behind: a bill nobody can explain,
// because the record that said what they were is already gone.
const bytesFinalizer = v1.Group + "/bytes"

// defaultArtifactRetention is how long bytes stay when nobody said.
//
// Finite, and at the top of the chain, for the same reason run retention
// is: storage that only grows is storage that one day stops.
const defaultArtifactRetention = 7 * 24 * time.Hour

// Bytes is the half of an artifact this operator does not own. An
// interface so the sweep can be checked without a bucket running.
type Bytes interface {
	// Remove takes the bytes away. Removing what is not there is not an
	// error: that is the normal answer when the sweep runs twice.
	Remove(ctx context.Context, key string) error
}

// ArtifactReconciler decides when an artifact stops existing — both halves
// of it, the record and the bytes, because either one alone is a lie: a
// record without bytes points at nothing, bytes without a record are a
// charge nobody can account for.
type ArtifactReconciler struct {
	kube  client.Client
	bytes Bytes
	// now is injectable so a test does not have to wait.
	now func() time.Time
	// fallback is what the installation says when nobody else did.
	fallback time.Duration
}

// NewArtifactReconciler builds one.
func NewArtifactReconciler(kube client.Client, bytes Bytes, fallback time.Duration) *ArtifactReconciler {
	if fallback <= 0 {
		fallback = defaultArtifactRetention
	}

	return &ArtifactReconciler{kube: kube, bytes: bytes, now: time.Now, fallback: fallback}
}

// Reconcile writes down when this artifact ends if nobody said, and ends it
// when that moment comes.
func (r *ArtifactReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var artifact v1.Artifact
	if err := r.kube.Get(ctx, req.NamespacedName, &artifact); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !artifact.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.release(ctx, &artifact)
	}

	if err := r.hold(ctx, &artifact); err != nil {
		return ctrl.Result{}, err
	}

	// Срок наследуется один раз и записывается. Вычислять его каждый раз
	// заново значило бы, что смена политики пайплайна задним числом
	// продлевает или обрубает то, что уже лежит, — а срок принадлежит
	// артефакту с момента, когда он появился.
	if artifact.Spec.Until == nil {
		return ctrl.Result{}, r.settle(ctx, &artifact)
	}

	if left := artifact.Spec.Until.Sub(r.now()); left > 0 {
		return ctrl.Result{RequeueAfter: left}, nil
	}

	if err := r.kube.Delete(ctx, &artifact); err != nil {
		return ctrl.Result{}, fmt.Errorf("артефакт %s не убрался: %w", artifact.Name, err)
	}

	return ctrl.Result{}, nil
}

// hold puts the finalizer on, so the bytes get their turn to go.
func (r *ArtifactReconciler) hold(ctx context.Context, artifact *v1.Artifact) error {
	if controllerutil.AddFinalizer(artifact, bytesFinalizer) {
		if err := r.kube.Update(ctx, artifact); err != nil {
			return fmt.Errorf("артефакт %s не удержался: %w", artifact.Name, err)
		}
	}

	return nil
}

// release removes the bytes and then lets the record go.
func (r *ArtifactReconciler) release(ctx context.Context, artifact *v1.Artifact) error {
	if !controllerutil.ContainsFinalizer(artifact, bytesFinalizer) {
		return nil
	}

	if r.bytes != nil && artifact.Spec.Key != "" {
		if err := r.bytes.Remove(ctx, artifact.Spec.Key); err != nil {
			return err
		}
	}

	controllerutil.RemoveFinalizer(artifact, bytesFinalizer)

	if err := r.kube.Update(ctx, artifact); err != nil {
		return fmt.Errorf("артефакт %s не отпустился: %w", artifact.Name, err)
	}

	return nil
}

// settle writes down when this artifact ends.
func (r *ArtifactReconciler) settle(ctx context.Context, artifact *v1.Artifact) error {
	until := metav1.NewTime(artifact.CreationTimestamp.Add(r.retention(ctx, artifact)))
	artifact.Spec.Until = &until

	if err := r.kube.Update(ctx, artifact); err != nil {
		return fmt.Errorf("срок артефакта %s не записался: %w", artifact.Name, err)
	}

	return nil
}

// retention is what the pipeline says, and otherwise what the installation
// says. The artifact's own field is not consulted here: if it were set,
// this is not called at all.
func (r *ArtifactReconciler) retention(ctx context.Context, artifact *v1.Artifact) time.Duration {
	var run v1.Run

	key := types.NamespacedName{Namespace: artifact.Namespace, Name: artifact.Spec.RunRef.Name}
	if err := r.kube.Get(ctx, key, &run); err != nil {
		return r.fallback
	}

	var revision v1.PipelineRevision

	key = types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.RevisionRef.Name}
	if err := r.kube.Get(ctx, key, &revision); err != nil {
		return r.fallback
	}

	var pipeline v1.Pipeline

	key = types.NamespacedName{Namespace: run.Namespace, Name: revision.Spec.PipelineRef.Name}
	if err := r.kube.Get(ctx, key, &pipeline); err != nil {
		return r.fallback
	}

	if pipeline.Spec.ArtifactRetention == nil || pipeline.Spec.ArtifactRetention.Duration <= 0 {
		return r.fallback
	}

	return pipeline.Spec.ArtifactRetention.Duration
}

// SetupWithManager wires the reconciler to artifacts.
func (r *ArtifactReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := ctrl.NewControllerManagedBy(mgr).For(&v1.Artifact{}).Complete(r); err != nil {
		return fmt.Errorf("контроллер артефактов не собрался: %w", err)
	}

	return nil
}
