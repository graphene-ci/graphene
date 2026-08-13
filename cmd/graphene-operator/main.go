// Command graphene-operator is the seam between the two halves of the
// system. Records say what should exist; Temporal says how far things got.
// This binary is the only place that knows both, and it is deliberately the
// only place.
package main

import (
	"fmt"
	"os"

	"go.temporal.io/sdk/client"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	v1 "github.com/graphene-ci/graphene/api/v1"
	"github.com/graphene-ci/graphene/internal/operator"
)

const (
	envAddress   = "GRAPHENE_TEMPORAL_ADDRESS"
	envNamespace = "GRAPHENE_TEMPORAL_NAMESPACE"
	envMetrics   = "GRAPHENE_METRICS_ADDRESS"
)

func main() {
	os.Exit(run())
}

func run() int {
	if err := serve(); err != nil {
		fmt.Fprintln(os.Stderr, "graphene-operator:", err)

		return 1
	}

	return 0
}

func serve() error {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		clientgoscheme.AddToScheme,
		appsv1.AddToScheme,
		v1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			return fmt.Errorf("схема не собралась: %w", err)
		}
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: os.Getenv(envMetrics)},
	})
	if err != nil {
		return fmt.Errorf("менеджер не собрался: %w", err)
	}

	address := os.Getenv(envAddress)
	namespace := os.Getenv(envNamespace)

	temporal, err := client.Dial(client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		return fmt.Errorf("не подключиться к Temporal: %w", err)
	}
	defer temporal.Close()

	if err := wire(mgr, operator.NewClient(temporal), address, namespace); err != nil {
		return err
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		return fmt.Errorf("менеджер остановился: %w", err)
	}

	return nil
}

// wire attaches every controller this binary carries.
func wire(mgr ctrl.Manager, bridge *operator.Client, address, namespace string) error {
	kube := mgr.GetClient()

	setups := []func(ctrl.Manager) error{
		operator.NewRunReconciler(kube, bridge).SetupWithManager,
		operator.NewProbeReconciler(kube).SetupWithManager,
		operator.NewReadinessReconciler(kube, bridge, &v1.Probe{}).SetupWithManager,
		operator.NewRevisionReconciler(kube, address, namespace).SetupWithManager,
	}

	for _, setup := range setups {
		if err := setup(mgr); err != nil {
			return err
		}
	}

	return nil
}
