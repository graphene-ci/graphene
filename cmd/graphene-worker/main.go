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
	"github.com/graphene-ci/graphene/sdk/agent"
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

	applier := worker.NewApplier(dynamic, resolve).WithStorage(worker.StorageFrom(os.Getenv))

	temporal, err := client.Dial(client.Options{
		HostPort:  os.Getenv(envAddress),
		Namespace: os.Getenv(envNamespace),
	})
	if err != nil {
		return fmt.Errorf("не подключиться к Temporal: %w", err)
	}
	defer temporal.Close()

	w := temporalworker.New(temporal, agent.SystemQueue, temporalworker.Options{})
	register(w, applier)

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

// register attaches everything this worker answers for. Activities write to
// the cluster; the workflows exist because the things that ask — an agent
// and a pipeline's worker — are workers themselves and cannot schedule an
// activity on their own.
func register(w temporalworker.Worker, applier *worker.Applier) {
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
		func(ctx context.Context, in agent.PresignInput) (agent.PresignOutput, error) {
			return applier.Presign(ctx, in)
		},
		activity.RegisterOptions{Name: agent.ActivityPresign},
	)

	w.RegisterActivityWithOptions(
		func(ctx context.Context, in agent.RecordArtifactInput) error {
			return applier.RecordArtifact(ctx, in)
		},
		activity.RegisterOptions{Name: agent.ActivityRecordArtifact},
	)

	w.RegisterActivityWithOptions(
		func(ctx context.Context, in agent.ClaimInput) (agent.ClaimOutput, error) {
			return applier.Claim(ctx, in)
		},
		activity.RegisterOptions{Name: agent.ActivityClaim},
	)

	w.RegisterActivityWithOptions(
		func(ctx context.Context, in agent.KeepInput) (agent.KeepOutput, error) {
			return applier.Keep(ctx, in)
		},
		activity.RegisterOptions{Name: agent.ActivityKeep},
	)

	w.RegisterActivityWithOptions(
		func(ctx context.Context, in agent.RegisterInput) error {
			return applier.Register(ctx, in)
		},
		activity.RegisterOptions{Name: agent.ActivityRegister},
	)

	// Регистрация — воркфлоу, потому что агент это воркер, а не воркфлоу:
	// поставить activity ему нечем.
	w.RegisterActivityWithOptions(
		func(ctx context.Context, in agent.RegisterRevisionInput) error {
			return applier.RecordRequirements(ctx, in)
		},
		activity.RegisterOptions{Name: agent.ActivityRegisterRevision},
	)

	w.RegisterWorkflowWithOptions(worker.RegisterMachine,
		workflow.RegisterOptions{Name: agent.WorkflowRegister})
	w.RegisterWorkflowWithOptions(worker.RegisterRevision,
		workflow.RegisterOptions{Name: agent.WorkflowRegisterRevision})
}
