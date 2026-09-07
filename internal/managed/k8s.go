package managed

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gopherex/xlog"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"go.temporal.io/sdk/client"

	"github.com/graphene-ci/graphene/internal/probes"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// K8sConfig configures the Kubernetes managed backend.
type K8sConfig struct {
	// PodNamespace is the Kubernetes namespace the run Deployments are
	// created in.
	PodNamespace string
	// ExternalGRPC is the server address a run worker dials (the in-cluster
	// Service address of the door).
	ExternalGRPC string
	// PullSecret, if set, is referenced as imagePullSecrets on run pods —
	// the docker-config Secret that authenticates against the image's
	// registry (the graphene door). Empty means no pull secret.
	PullSecret string
	// PullRegistry, if set, rewrites the HOST of the run image ref so pods
	// pull from it instead of the ref's own (door) host — for pulling from an
	// in-cluster registry. Empty leaves the ref untouched (pull via the door).
	PullRegistry string
	// Insecure passes GRAPHENE_INSECURE=1 to the run worker (h2c door).
	Insecure bool
}

// k8sRunner is the Kubernetes backend: each managed run worker is a
// Deployment (replicas=1) — a controller keeps it alive across crashes and
// node moves until the run is over, then Reap deletes it. Run records
// survive a server restart because the Deployments do; Reap and Ensure
// rediscover them by label, exactly like the docker backend does by daemon
// label.
type k8sRunner struct {
	namespace string // the graphene namespace this runner serves
	clients   kubernetes.Interface
	temporal  client.Client
	log       *xlog.Logger
	cfg       K8sConfig
	runToken  string // installation-wide fallback token
}

// NewK8s builds the Kubernetes managed backend over a clientset.
func NewK8s(namespace string, temporal client.Client, clients kubernetes.Interface, runToken string, cfg K8sConfig, log *xlog.Logger) Runner {
	return &k8sRunner{namespace: namespace, clients: clients, temporal: temporal, log: log, cfg: cfg, runToken: runToken}
}

func (r *k8sRunner) Ping(ctx context.Context) error {
	if r.clients == nil {
		return probes.ErrDisabled
	}
	_, err := r.clients.Discovery().ServerVersion()
	return err
}

func (r *k8sRunner) Start(ctx context.Context, runId id.RunId, imageRef, runToken string) error {
	if r.clients == nil {
		return fmt.Errorf("managed runs need a Kubernetes client")
	}
	if runToken == "" {
		runToken = r.runToken
	}
	dep := r.deployment(runId, imageRef, runToken)
	_, err := r.clients.AppsV1().Deployments(r.cfg.PodNamespace).Create(ctx, dep, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil // idempotent — the worker is already there
	}
	if err != nil {
		return fmt.Errorf("create run deployment: %w", err)
	}
	r.log.Info("managed run deployment started", xlog.Any("run", runId), xlog.String("image", imageRef))
	return nil
}

func (r *k8sRunner) Ensure(ctx context.Context, runId id.RunId, imageRef, runToken string) (bool, error) {
	if r.clients == nil {
		return false, fmt.Errorf("managed runs need a Kubernetes client")
	}
	name := runName(r.namespace, runId)
	_, err := r.clients.AppsV1().Deployments(r.cfg.PodNamespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return false, nil // the Deployment (hence the worker) is alive
	}
	if !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("ensure run deployment: %w", err)
	}
	if err := r.Start(ctx, runId, imageRef, runToken); err != nil {
		return false, err
	}
	r.log.Info("managed run deployment resurrected", xlog.Any("run", runId))
	return true, nil
}

func (r *k8sRunner) Reap(ctx context.Context) {
	if r.clients == nil {
		return
	}
	list, err := r.clients.AppsV1().Deployments(r.cfg.PodNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelNamespace + "=" + r.namespace,
	})
	if err != nil {
		r.log.Error("reap: list run deployments", xlog.Err(err))
		return
	}
	for i := range list.Items {
		dep := &list.Items[i]
		runId := id.RunId(dep.Labels[labelRun])
		if runId == "" {
			continue
		}
		over, err := runIsOver(ctx, r.temporal, runId)
		if err != nil || !over {
			continue
		}
		policy := metav1.DeletePropagationForeground
		if err := r.clients.AppsV1().Deployments(r.cfg.PodNamespace).Delete(ctx, dep.Name, metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
			r.log.Error("reap run deployment", xlog.Any("run", runId), xlog.Err(err))
			continue
		}
		r.log.Info("managed run deployment reaped", xlog.Any("run", runId))
	}
}

func (r *k8sRunner) Tick(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.Reap(ctx)
		}
	}
}

// deployment builds the run worker's Deployment.
func (r *k8sRunner) deployment(runId id.RunId, imageRef, runToken string) *appsv1.Deployment {
	name := runName(r.namespace, runId)
	labels := map[string]string{labelNamespace: r.namespace, labelRun: string(runId)}
	env := []corev1.EnvVar{
		{Name: wire.EnvRole, Value: "run"},
		{Name: wire.EnvAddress, Value: r.cfg.ExternalGRPC},
		{Name: wire.EnvNamespace, Value: r.namespace},
		{Name: wire.EnvRunId, Value: string(runId)},
		{Name: wire.EnvToken, Value: runToken},
		{Name: wire.EnvImage, Value: imageRef},
	}
	if r.cfg.Insecure {
		env = append(env, corev1.EnvVar{Name: wire.EnvInsecure, Value: "1"})
	}
	var pullSecrets []corev1.LocalObjectReference
	if r.cfg.PullSecret != "" {
		pullSecrets = []corev1.LocalObjectReference{{Name: r.cfg.PullSecret}}
	}
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: r.cfg.PodNamespace, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:    corev1.RestartPolicyAlways,
					ImagePullSecrets: pullSecrets,
					Containers: []corev1.Container{{
						Name:  "run",
						Image: r.imageFor(imageRef),
						Env:   env,
					}},
				},
			},
		},
	}
}

// imageFor rewrites the image ref's registry host to PullRegistry when set —
// so pods can pull from an in-cluster registry instead of the door. Empty
// leaves the ref as is (pull via the door host in the ref).
func (r *k8sRunner) imageFor(ref string) string {
	if r.cfg.PullRegistry == "" {
		return ref
	}
	// ref is "host[:port]/path:tag"; replace the leading host component.
	if i := strings.IndexByte(ref, '/'); i >= 0 {
		return r.cfg.PullRegistry + ref[i:]
	}
	return ref
}

// runName is the Deployment/pod name for a run: RFC1123, <=63 chars.
func runName(namespace string, runId id.RunId) string {
	name := "graphene-run-" + k8sSanitize(namespace) + "-" + k8sSanitize(string(runId))
	if len(name) > 63 {
		name = name[:63]
	}
	return strings.Trim(name, "-.")
}

func k8sSanitize(s string) string {
	out := make([]rune, 0, len(s))
	for _, c := range strings.ToLower(s) {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			out = append(out, c)
		} else {
			out = append(out, '-')
		}
	}
	return string(out)
}
