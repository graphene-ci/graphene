// Command graphene-worker performs what pipelines ask for. It is the only
// thing that writes records on a pipeline's behalf: a pipeline's own image
// carries no kubernetes client and no permission to write, and does not
// need to.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/graphene/internal/kube"
	"github.com/graphene-ci/graphene/internal/worker"
	"github.com/graphene-ci/graphene/pkg/agent"
)

// Where this binary is told to find things. Everything comes from the
// environment: it runs as a pod, and a pod's configuration is its
// environment.
const (
	envAddress    = "GRAPHENE_TEMPORAL_ADDRESS"
	envNamespace  = "GRAPHENE_TEMPORAL_NAMESPACE"
	envKubeconfig = "KUBECONFIG"
)

func main() {
	os.Exit(run())
}

func run() int {
	if err := serve(); err != nil {
		fmt.Fprintln(os.Stderr, "graphene-worker:", err)

		return 1
	}

	return 0
}

func serve() error {
	cfg, err := kube.Config(os.Getenv(envKubeconfig))
	if err != nil {
		return err
	}

	dynamic, err := kube.Dynamic(cfg)
	if err != nil {
		return err
	}

	resolve, err := kube.Resolve(cfg)
	if err != nil {
		return err
	}

	applier := worker.NewApplier(dynamic, resolve)

	temporal, err := client.Dial(client.Options{
		HostPort:  os.Getenv(envAddress),
		Namespace: os.Getenv(envNamespace),
	})
	if err != nil {
		return fmt.Errorf("не подключиться к Temporal: %w", err)
	}
	defer temporal.Close()

	w := temporalworker.New(temporal, agent.SystemQueue, temporalworker.Options{})

	w.RegisterActivityWithOptions(
		func(ctx context.Context, in agent.ApplyInput) (agent.ApplyOutput, error) {
			return applier.Apply(ctx, in)
		},
		activity.RegisterOptions{Name: agent.ActivityApply},
	)

	w.RegisterActivityWithOptions(
		func(ctx context.Context, in agent.TeardownInput) (agent.TeardownOutput, error) {
			return applier.Teardown(ctx, in)
		},
		activity.RegisterOptions{Name: agent.ActivityTeardown},
	)

	w.RegisterActivityWithOptions(
		func(ctx context.Context, in agent.RegisterInput) error {
			return applier.Register(ctx, in)
		},
		activity.RegisterOptions{Name: agent.ActivityRegister},
	)

	// Регистрация — воркфлоу, потому что агент это воркер, а не воркфлоу:
	// поставить activity ему нечем.
	w.RegisterWorkflowWithOptions(worker.RegisterMachine,
		workflow.RegisterOptions{Name: agent.WorkflowRegister})

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	stop := make(chan any, 1)

	go func() {
		<-signals
		close(stop)
	}()

	if err := w.Run(stop); err != nil {
		return fmt.Errorf("воркер остановился: %w", err)
	}

	return nil
}
