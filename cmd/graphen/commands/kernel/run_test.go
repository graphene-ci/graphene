package kernel_test

import (
	"testing"

	"github.com/graphene-ci/graphene/cmd/graphen/commands/kernel"
)

func TestRunValidatesFlags(t *testing.T) {
	t.Parallel()

	if err := kernel.Run(nil); err == nil {
		t.Fatal("Run(nil) returned no error")
	}

	if err := kernel.Run(&kernel.RunFlags{Config: "./graphen-kernel.yaml"}); err != nil {
		t.Fatalf("Run(valid flags): %v", err)
	}
}
