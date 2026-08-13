// Package commands is the command tree of the graphene CLI. It lives in a
// package rather than in main so that tests can look at the tree, and so
// that the tree can be mounted by something other than this binary later.
package commands

import (
	"github.com/spf13/cobra"
)

// Root builds the command tree. Every milestone adds to it; nothing removes.
func Root() *cobra.Command {
	root := &cobra.Command{
		Use:   "graphene",
		Short: "Pipeline и инфраструктура одним кодом",
		Long: "graphene управляет установкой, пайплайнами и прогонами.\n" +
			"Одна программа описывает и то, что создать в мире, и то, что на этом выполнить.",
		SilenceUsage: true,
	}

	root.PersistentFlags().String("kubeconfig", "",
		"путь к kubeconfig; пусто — обычные правила (KUBECONFIG, ~/.kube/config)")

	root.AddCommand(newUp())

	return root
}
