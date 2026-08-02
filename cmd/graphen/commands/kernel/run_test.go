package kernel_test

import (
	"strings"
	"testing"

	"github.com/graphene-ci/graphene/cmd/graphen/commands/kernel"
)

// Run reaches the real kernel: the unit test only pins the flag contract
// (a running kernel is covered end to end in internal/app/kernel).
func TestRunValidatesFlags(t *testing.T) {
	t.Parallel()

	if err := kernel.Run(nil); err == nil {
		t.Fatal("Run(nil) returned no error")
	}

	if err := kernel.Run(&kernel.RunFlags{Config: "  "}); err == nil {
		t.Fatal("Run(blank config) returned no error")
	}

	err := kernel.Run(&kernel.RunFlags{Config: "./does-not-exist.yaml"})
	if err == nil || !strings.Contains(err.Error(), "kernel run") {
		t.Fatalf("Run(missing config): got %v", err)
	}
}
