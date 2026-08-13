package commands

import (
	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/env"
)

// newUp builds `graphene up`: the answer to "can I run anything right now".
func newUp() *cobra.Command {
	var opts env.Options

	cmd := &cobra.Command{
		Use:   "up",
		Short: "Проверить, что управляющий слой на месте",
		Long: "Сообщает готовность Temporal и Crossplane в текущем кластере.\n" +
			"С --wait ждёт, пока они поднимутся, вместо того чтобы сразу отказать.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			kubeconfig, err := cmd.Flags().GetString("kubeconfig")
			if err != nil {
				return err
			}

			probe, err := env.NewKubeProbe(kubeconfig)
			if err != nil {
				return err
			}

			return env.Wait(cmd.Context(), probe, env.Control(), cmd.OutOrStdout(), opts)
		},
	}

	cmd.Flags().BoolVar(&opts.Wait, "wait", false, "ждать готовности, а не отказывать сразу")
	cmd.Flags().DurationVar(&opts.Every, "every", 0, "как часто переспрашивать при --wait")

	return cmd
}
