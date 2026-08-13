package commands

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/graphene-ci/graphene/api/v1"
	"github.com/graphene-ci/graphene/internal/cli"
	"github.com/graphene-ci/graphene/internal/kube"
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
