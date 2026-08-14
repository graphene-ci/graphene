package commands

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/graphene-ci/graphene/internal/cli"
	"github.com/graphene-ci/graphene/internal/kube"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

// kubeClient builds a typed client that knows our kinds.
func kubeClient(cmd *cobra.Command) (client.Client, error) {
	kubeconfig, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return nil, err
	}

	cfg, err := kube.Config(kubeconfig)
	if err != nil {
		return nil, err
	}

	scheme := runtime.NewScheme()
	if err := v1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("схема не собралась: %w", err)
	}

	built, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("клиент кластера не собрался: %w", err)
	}

	return built, nil
}

func newPush() *cobra.Command {
	var (
		name string
		repo string
	)

	cmd := &cobra.Command{
		Use:   "push <каталог>",
		Short: "Собрать пайплайн и записать его ревизию",
		Long: "Собирает каталог с кодом пайплайна в образ и записывает Pipeline\n" +
			"и PipelineRevision. В ревизии стоит дайджест, а не тег: тег\n" +
			"переставляют, и «повторить прогон» перестало бы значить\n" +
			"«выполнить тот же код».",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			kubeClient, err := kubeClient(cmd)
			if err != nil {
				return err
			}

			namespace, err := cmd.Flags().GetString("namespace")
			if err != nil {
				return err
			}

			if name == "" {
				name = defaultName(args[0])
			}

			revision, err := cli.Push(cmd.Context(), cli.PushRequest{
				Kube: kubeClient,
				Builder: cli.Ko{
					Path: toolPath("ko"),
					Repo: repo,
					Out:  os.Stderr,
				},
				Namespace: namespace,
				Pipeline:  name,
				Dir:       args[0],
			})
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "%s\n%s\n", revision.Name, revision.Spec.Image)

			return nil
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "имя пайплайна; пусто — имя каталога")
	cmd.Flags().StringVar(&repo, "repo", "localhost:5555/graphene",
		"реестр, в который класть образ; кластер обязан читать его по тому же имени")

	return cmd
}

func newRun() *cobra.Command {
	var (
		revision string
		params   string
		wait     bool
	)

	cmd := &cobra.Command{
		Use:   "run <пайплайн>",
		Short: "Запустить прогон",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			kubeClient, err := kubeClient(cmd)
			if err != nil {
				return err
			}

			namespace, err := cmd.Flags().GetString("namespace")
			if err != nil {
				return err
			}

			run, err := cli.Start(cmd.Context(), cli.StartRequest{
				Kube:      kubeClient,
				Namespace: namespace,
				Pipeline:  args[0],
				Revision:  revision,
				Params:    []byte(params),
			})
			if err != nil {
				return err
			}

			fmt.Fprintln(cmd.OutOrStdout(), run.Name)

			if !wait {
				return nil
			}

			return follow(cmd, kubeClient, namespace, run.Name, 0)
		},
	}

	cmd.Flags().StringVar(&revision, "revision", "", "какую ревизию исполнять; пусто — последнюю")
	cmd.Flags().StringVar(&params, "params", "", "параметры прогона в JSON")
	cmd.Flags().BoolVar(&wait, "wait", false, "дождаться конца прогона")

	return cmd
}

// newServe builds `graphene serve` — the pipeline's worker, run HERE.
//
// It exists because the control plane does not run pipelines: a pipeline is
// arbitrary code written by anybody, and executing it inside the control
// plane would turn "put a pipeline into the system" into "run whatever you
// like where our credentials live".
func newServe() *cobra.Command {
	var (
		name        string
		revision    string
		temporal    string
		control     string
		agent       string
		space       string
		traces      string
		agentTraces string
	)

	cmd := &cobra.Command{
		Use:   "serve <каталог>",
		Short: "Выполнять пайплайн здесь, на этой машине",
		Long: `Поднимает воркер пайплайна на машине того, кто набрал команду.
Управляющий слой чужой код не выполняет: пайплайн пишет кто угодно, и
внутри него можно сделать что угодно.

Пока ревизию никто не обслуживает, её прогоны не двигаются — история
цела, работа продолжится, когда воркер вернётся.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			kubeClient, err := kubeClient(cmd)
			if err != nil {
				return err
			}

			namespace, err := cmd.Flags().GetString("namespace")
			if err != nil {
				return err
			}

			if name == "" {
				name = defaultName(args[0])
			}

			return cli.Serve(cmd.Context(), cli.ServeRequest{
				Kube:         kubeClient,
				Namespace:    namespace,
				Pipeline:     name,
				Revision:     revision,
				Dir:          args[0],
				Address:      space,
				Temporal:     temporal,
				Control:      control,
				AgentAddress: agent,
				Traces:       traces,
				AgentTraces:  agentTraces,
				Out:          cmd.OutOrStdout(),
			})
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "имя пайплайна; пусто — имя каталога")
	cmd.Flags().StringVar(&revision, "revision", "", "какую ревизию обслуживать; пусто — последнюю")
	cmd.Flags().StringVar(&temporal, "temporal", "127.0.0.1:7233", "адрес Temporal")
	cmd.Flags().StringVar(&space, "temporal-namespace", "graphene", "пространство имён Temporal")
	cmd.Flags().StringVar(&control, "control", "http://127.0.0.1:18080", "откуда машины берут агента")
	cmd.Flags().StringVar(&agent, "agent-temporal", "", "адрес Temporal, каким его видит машина; пусто — как здесь")
	cmd.Flags().StringVar(&traces, "otlp", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		"приёмник трасс OTLP; пусто — не записывать")
	cmd.Flags().StringVar(&agentTraces, "agent-otlp", "",
		"приёмник трасс, каким его видит машина; пусто — как здесь")

	return cmd
}

// newCancel builds `graphene cancel`.
//
// It writes a wish onto the record rather than reaching for Temporal
// itself: stopping a run is a decision about the world, and decisions
// about the world are records. A person, a UI and kubectl all say it the
// same way, and the wish outlives whoever expressed it.
func newCancel() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cancel <прогон>",
		Short: "Остановить прогон",
		Long: `Просит прогон остановиться. Это отмена, а не убийство:
пайплайн получает возможность прибрать за собой, и снос выполняется.

Убить всё вместе с записью — это kubectl delete run, и тогда убирает
управляющий слой.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			kubeClient, err := kubeClient(cmd)
			if err != nil {
				return err
			}

			namespace, err := cmd.Flags().GetString("namespace")
			if err != nil {
				return err
			}

			if err := cli.Cancel(cmd.Context(), kubeClient, namespace, args[0]); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "%s: отмена запрошена\n", args[0])

			return nil
		},
	}

	return cmd
}

func newWatch() *cobra.Command {
	var every time.Duration

	cmd := &cobra.Command{
		Use:   "watch <прогон>",
		Short: "Следить за прогоном до конца",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			kubeClient, err := kubeClient(cmd)
			if err != nil {
				return err
			}

			namespace, err := cmd.Flags().GetString("namespace")
			if err != nil {
				return err
			}

			return follow(cmd, kubeClient, namespace, args[0], every)
		},
	}

	cmd.Flags().DurationVar(&every, "every", 0, "как часто перечитывать запись")

	return cmd
}

// follow watches a run and turns its outcome into the command's outcome: a
// failed run means a failed command, which is what a CI system reads.
func follow(cmd *cobra.Command, kubeClient client.Client, namespace, name string, every time.Duration) error {
	phase, err := cli.Watch(cmd.Context(), cli.WatchRequest{
		Kube:      kubeClient,
		Namespace: namespace,
		Name:      name,
		Out:       cmd.OutOrStdout(),
		Every:     every,
	})
	if err != nil {
		return err
	}

	if phase != v1.RunSucceeded {
		return fmt.Errorf("%w: %s", errRunFailed, phase)
	}

	return nil
}
