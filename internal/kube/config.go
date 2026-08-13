// Package kube is how everything of ours reaches the cluster: one place
// that knows where the configuration comes from, so that a binary running
// in a pod and a command run by a person do not answer that differently.
package kube

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
)

// Config loads the cluster configuration. An explicit path wins; otherwise
// the usual rules apply — KUBECONFIG, then ~/.kube/config, then the
// service account of the pod we are running in.
func Config(kubeconfig string) (*rest.Config, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}

	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("кластер не настроен: %w", err)
	}

	return cfg, nil
}

// Dynamic builds a client that works with any kind, including ones that did
// not exist when this binary was compiled. That is the whole point: a
// person installs a new provider and our binaries stay as they are.
func Dynamic(cfg *rest.Config) (dynamic.Interface, error) {
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("динамический клиент не собрался: %w", err)
	}

	return client, nil
}

// Resolver turns a kind into the resource the API server serves it at, and
// says whether that resource lives in a namespace. It is a function rather
// than a RESTMapper so that whatever uses it can be tested without a
// cluster, and so that discovery — a cache with its own failure modes —
// stays at the edge.
type Resolver func(ctx context.Context, gvk schema.GroupVersionKind) (schema.GroupVersionResource, bool, error)

// Resolve builds a Resolver against the cluster.
//
// The mapping is cached, and the cache is dropped when a kind is not found:
// providers are installed while we are running, and "I have never heard of
// this kind" is very often "I last looked before it existed".
func Resolve(cfg *rest.Config) (Resolver, error) {
	client, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("клиент discovery не собрался: %w", err)
	}

	cached := memory.NewMemCacheClient(client)
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(cached)

	return func(_ context.Context, gvk schema.GroupVersionKind) (schema.GroupVersionResource, bool, error) {
		mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if meta.IsNoMatchError(err) {
			cached.Invalidate()

			mapping, err = mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		}

		if err != nil {
			return schema.GroupVersionResource{}, false, fmt.Errorf("вид %s не найден: %w", gvk, err)
		}

		return mapping.Resource, mapping.Scope.Name() == meta.RESTScopeNameNamespace, nil
	}, nil
}
