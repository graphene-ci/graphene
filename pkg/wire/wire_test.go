package wire

import "testing"

func TestQueueNames(t *testing.T) {
	if got := MachineRunQueue("m-1", "r-1"); got != "machine/m-1/run/r-1" {
		t.Fatalf("MachineRunQueue: %q", got)
	}
	if got := RunQueue("r-1"); got != "run/r-1" {
		t.Fatalf("RunQueue: %q", got)
	}
}
