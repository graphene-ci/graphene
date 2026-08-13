package commands_test

import (
	"testing"

	"github.com/graphene-ci/graphene/cmd/graphene/commands"
)

func TestRootCarriesUp(t *testing.T) {
	t.Parallel()

	up, _, err := commands.Root().Find([]string{"up"})
	if err != nil || up.Name() != "up" {
		t.Fatalf("нет команды up: %v", err)
	}

	if up.Flags().Lookup("wait") == nil {
		t.Fatal("у up нет флага --wait")
	}
}

func TestRootCarriesKubeconfigForEveryCommand(t *testing.T) {
	t.Parallel()

	if commands.Root().PersistentFlags().Lookup("kubeconfig") == nil {
		t.Fatal("нет сквозного флага --kubeconfig")
	}
}
