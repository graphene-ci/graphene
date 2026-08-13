// Command graphene-agent runs on a machine and does what a run asks of it.
//
// It is a Temporal worker reading the queue of its own installation, and
// nothing else. There is no inbound port and no protocol of ours: the
// machine reaches out, and the queue it reads is the whole of its world.
// A step scheduled into that queue before the agent exists simply waits
// there — the queue IS the waiting, which is why nothing ever has to check
// whether an agent has come up yet.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	internalagent "github.com/graphene-ci/graphene/internal/agent"
	"github.com/graphene-ci/graphene/pkg/agent"
)

// What the install script sets. Everything comes from the environment
// because the script is what configures this, and a script's way of
// configuring a service is its environment.
const (
	envAddress   = "GRAPHENE_TEMPORAL_ADDRESS"
	envNamespace = "GRAPHENE_TEMPORAL_NAMESPACE"
	envRecords   = "GRAPHENE_NAMESPACE"
	envMachine   = "GRAPHENE_MACHINE"
)

// errNoName means the install script did not say what this machine is
// called, and without a name there is no queue to read.
var errNoName = errors.New("не сказано, как зовут машину")

func main() {
	os.Exit(run())
}

func run() int {
	if err := serve(); err != nil {
		fmt.Fprintln(os.Stderr, "graphene-agent:", err)

		return 1
	}

	return 0
}

func serve() error {
	machine := os.Getenv(envMachine)
	if machine == "" {
		return fmt.Errorf("%w: %s", errNoName, envMachine)
	}

	temporal, err := client.Dial(client.Options{
		HostPort:  os.Getenv(envAddress),
		Namespace: os.Getenv(envNamespace),
	})
	if err != nil {
		return fmt.Errorf("не подключиться к Temporal: %w", err)
	}
	defer temporal.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	queue := agent.InstallationQueue(machine)

	// Факты читаются один раз при старте, и это правда о голой системе.
	// Дальше их меняют обёртки — docker.Install ставит докер и просит
	// перечитать; сам агент ничего о докере не знает.
	facts := internalagent.Facts(ctx).Facts

	registration := internalagent.Registration{
		Temporal:  temporal,
		Machine:   machine,
		Namespace: records(),
		Queue:     queue,
		Facts:     facts,
	}

	// Представиться до того, как начать брать работу: иначе первый шаг
	// выполнится на машине, которой в кластере ещё нет.
	if err := registration.Beat(ctx); err != nil {
		return err
	}

	go registration.Keep(ctx, func(err error) {
		fmt.Fprintln(os.Stderr, "graphene-agent: отметка не прошла:", err)
	})

	return work(ctx, temporal, queue)
}

func work(ctx context.Context, temporal client.Client, queue string) error {
	w := temporalworker.New(temporal, queue, temporalworker.Options{})

	w.RegisterActivityWithOptions(
		internalagent.Exec,
		activity.RegisterOptions{Name: agent.ActivityExec},
	)

	w.RegisterActivityWithOptions(
		func(ctx context.Context) (agent.FactsOutput, error) {
			return internalagent.Facts(ctx), nil
		},
		activity.RegisterOptions{Name: agent.ActivityFacts},
	)

	stop := make(chan any, 1)

	go func() {
		<-ctx.Done()
		close(stop)
	}()

	if err := w.Run(stop); err != nil {
		return fmt.Errorf("агент остановился: %w", err)
	}

	return nil
}

// records is where the machine's record goes. It is not the Temporal
// namespace and the two are unrelated; conflating them would tie the
// cluster's layout to Temporal's.
func records() string {
	if value := os.Getenv(envRecords); value != "" {
		return value
	}

	return "default"
}
