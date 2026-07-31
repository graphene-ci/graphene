package kernel

import "testing"

func TestRunValidatesFlags(t *testing.T) {
	t.Parallel()

	if err := Run(nil); err == nil {
		t.Fatal("Run(nil) returned no error")
	}

	if err := Run(&RunFlags{Config: "./graphen-kernel.yaml"}); err != nil {
		t.Fatalf("Run(valid flags): %v", err)
	}
}
