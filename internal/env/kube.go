package env

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// temporal is the component, the namespace and the deployment: one process
// installed under its own name, so the word appears three times on purpose.
const temporal = "temporal"

// Control names the pieces of the control plane `graphene up` reports on.
// The names match what deploy/local puts into the cluster; changing one
// here without changing the manifest makes `up` lie.
func Control() []Component {
	return []Component{
		{Name: temporal, Namespace: temporal, Deployment: temporal},
		{Name: "crossplane", Namespace: "crossplane-system", Deployment: "crossplane"},
	}
}

// KubeProbe answers for a component by looking at its Deployment.
type KubeProbe struct {
	client kubernetes.Interface
}

// NewKubeProbe builds a probe over the caller's current kubeconfig context.
// kubeconfig may be empty, in which case the usual loading rules apply
// (KUBECONFIG, then ~/.kube/config, then in-cluster).
func NewKubeProbe(kubeconfig string) (*KubeProbe, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}

	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("кластер не настроен: %w", err)
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("клиент кластера не собрался: %w", err)
	}

	return &KubeProbe{client: client}, nil
}

// Status reports whether the component's Deployment has an available
// replica. A missing Deployment is not an error: it is the answer "not
// installed yet", which is exactly what someone running `up` wants to hear.
func (p *KubeProbe) Status(ctx context.Context, comp Component) (Status, error) {
	dep, err := p.client.AppsV1().Deployments(comp.Namespace).Get(ctx, comp.Deployment, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return Status{Reason: "не установлен"}, nil
	}

	if err != nil {
		return Status{}, fmt.Errorf("не удалось прочитать %s/%s: %w", comp.Namespace, comp.Deployment, err)
	}

	if dep.Status.AvailableReplicas > 0 {
		return Status{Ready: true}, nil
	}

	reason := fmt.Sprintf("%d/%d реплик", dep.Status.ReadyReplicas, dep.Status.Replicas)

	return Status{Reason: reason}, nil
}
