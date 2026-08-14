// Package commands is the command tree of the graphene CLI. It lives in a
// package rather than in main so that tests can look at the tree, and so
// that the tree can be mounted by something other than this binary later.
package commands

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

// errRunFailed means the run this command followed did not succeed. The
// command fails with it so that whoever called us — a person or another CI
// system — learns the outcome from the exit code.
var errRunFailed = errors.New("прогон не удался")

// toolPath finds a tool next to this binary first. `make configure` puts
// every tool in bin/, and a graphene run from bin/ should use the ko that
// was pinned beside it rather than whatever the machine happens to have.
func toolPath(name string) string {
	self, err := os.Executable()
	if err != nil {
		return name
	}

	beside := filepath.Join(filepath.Dir(self), name)
	if _, err := os.Stat(beside); err == nil {
		return beside
	}

	return name
}

// defaultName is what a pipeline is called when nobody said: the directory
// it lives in.
func defaultName(dir string) string {
	cleaned := filepath.Base(filepath.Clean(dir))
	if cleaned == "." || cleaned == string(filepath.Separator) {
		return "pipeline"
	}

	return strings.ToLower(cleaned)
}

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

	root.PersistentFlags().StringP("namespace", "n", "default",
		"пространство имён, в котором живут записи")

	root.AddCommand(newUp(), newPush(), newServe(), newRun(), newCancel(), newWatch())

	return root
}
