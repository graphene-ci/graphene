// Command graphene-operator is the seam between the two halves of the
// system. Records say what should exist; Temporal says how far things got.
// This binary is the only place that knows both, and it is deliberately the
// only place.
package main

import (
	"context"
	"fmt"
	"os"

	"go.temporal.io/sdk/client"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/graphene-ci/graphene/internal/kube"
	"github.com/graphene-ci/graphene/internal/operator"
	"github.com/graphene-ci/graphene/sdk/agent"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

const (
	envAddress      = "GRAPHENE_TEMPORAL_ADDRESS"
	envNamespace    = "GRAPHENE_TEMPORAL_NAMESPACE"
	envMetrics      = "GRAPHENE_METRICS_ADDRESS"
	envDist         = "GRAPHENE_DIST_ADDRESS"
	envControl      = "GRAPHENE_CONTROL"
	envAgentAddress = "GRAPHENE_AGENT_TEMPORAL"
)

// distAddress is where the agent binary is served from.
func distAddress() string {
	if value := os.Getenv(envDist); value != "" {
		return value
	}

	return ":8080"
}

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

	cfg := ctrl.GetConfigOrDie()

	serves, err := kube.Serves(cfg)
	if err != nil {
		return err
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
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

	known := func(ctx context.Context, kind agent.Kind) (bool, error) {
		return serves(ctx, schema.GroupVersionKind{Group: kind.Group, Version: kind.Version, Kind: kind.Kind})
	}

	source, err := kube.Dynamic(cfg)
	if err != nil {
		return err
	}

	resolve, err := kube.Resolve(cfg)
	if err != nil {
		return err
	}

	bridge := operator.NewClient(temporal)
	readiness := operator.NewReadiness(mgr.GetClient(), source, resolve, bridge)

	if err := mgr.Add(readiness); err != nil {
		return fmt.Errorf("наблюдение за готовностью не встало: %w", err)
	}

	sweeper := operator.NewSweeper(source, resolve)

	if err := wire(mgr, bridge, known, readiness, sweeper); err != nil {
		return err
	}

	// Раздача бинаря агента живёт в менеджере: тогда она поднимается и
	// гаснет вместе со всем остальным, а не отдельной жизнью.
	if err := mgr.Add(operator.NewDistributor(distAddress())); err != nil {
		return fmt.Errorf("раздача агента не встала: %w", err)
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		return fmt.Errorf("менеджер остановился: %w", err)
	}

	return nil
}

// wire attaches every controller this binary carries.
func wire(
	mgr ctrl.Manager, bridge *operator.Client,
	known operator.Known, watch operator.Watcher, sweep operator.Sweeper,
) error {
	records := mgr.GetClient()

	setups := []func(ctrl.Manager) error{
		operator.NewRunReconciler(records, bridge, known, watch, sweep).SetupWithManager,
		operator.NewProbeReconciler(records).SetupWithManager,
		operator.NewMachineReconciler(records).SetupWithManager,
		operator.NewMachineIntentReconciler(records, operator.SSH).SetupWithManager,
	}

	for _, setup := range setups {
		if err := setup(mgr); err != nil {
			return err
		}
	}

	return nil
}
