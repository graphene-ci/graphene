package operator

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1 "github.com/graphene-ci/graphene/api/v1"
	"github.com/graphene-ci/graphene/pkg/pipeline"
)

// RevisionReconciler runs the worker of a pushed revision.
//
// This is the piece that was missing and had to be named: pushing a
// pipeline produces an image, and somebody has to run it. Nobody else can —
// a run's workflow executes inside that image, so the image must already be
// listening on its queue before the first run is asked for.
//
// One deployment per revision, and the queue is the revision's, so a worker
// of an old revision never picks up work meant for a new one. That is also
// what lets versioning stay out of the pipeline's code: old runs drain on
// old workers instead of hitting a `patched()` branch.
type RevisionReconciler struct {
	kube client.Client
	// Temporal is where the worker is told to connect.
	Address   string
	Namespace string
	// Control is where machines fetch the agent from.
	Control string
}

// NewRevisionReconciler builds one.
func NewRevisionReconciler(kube client.Client, address, namespace, control string) *RevisionReconciler {
	return &RevisionReconciler{kube: kube, Address: address, Namespace: namespace, Control: control}
}

// Reconcile makes the revision's worker exist.
func (r *RevisionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var revision v1.PipelineRevision
	if err := r.kube.Get(ctx, req.NamespacedName, &revision); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: revision.Name, Namespace: revision.Namespace},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.kube, deployment, func() error {
		r.shape(&revision, deployment)

		return controllerutil.SetControllerReference(&revision, deployment, r.kube.Scheme())
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("воркер ревизии %s не поднялся: %w", revision.Name, err)
	}

	return ctrl.Result{}, nil
}

// shape fills in the deployment. It is separate so that what the worker is
// told is readable in one piece.
func (r *RevisionReconciler) shape(revision *v1.PipelineRevision, deployment *appsv1.Deployment) {
	labels := map[string]string{
		"app.kubernetes.io/name":      "graphene-pipeline",
		"app.kubernetes.io/instance":  revision.Name,
		"app.kubernetes.io/component": "worker",
	}

	replicas := int32(1)
	deployment.Spec.Replicas = &replicas
	deployment.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
	deployment.Spec.Template.Labels = labels
	deployment.Spec.Template.Spec.Containers = []corev1.Container{{
		Name:  "pipeline",
		Image: revision.Spec.Image,
		Env: []corev1.EnvVar{
			{Name: pipeline.EnvAddress, Value: r.Address},
			{Name: pipeline.EnvNamespace, Value: r.Namespace},
			{Name: pipeline.EnvQueue, Value: revision.Spec.Queue},
			// Откуда машина возьмёт агента. Пайплайн кладёт этот адрес
			// в скрипт установки, который сам же и порождает.
			{Name: pipeline.EnvControl, Value: r.Control},
			// Чтобы воркер знал, чью ревизию он обслуживает, и мог
			// сообщить, какие виды ей нужны.
			{Name: pipeline.EnvRevision, Value: revision.Name},
			{Name: pipeline.EnvRecords, Value: revision.Namespace},
		},
	}}
}

// SetupWithManager wires the reconciler to revisions.
func (r *RevisionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		For(&v1.PipelineRevision{}).
		Owns(&appsv1.Deployment{}).
		Complete(r)
	if err != nil {
		return fmt.Errorf("контроллер ревизий не собрался: %w", err)
	}

	return nil
}
