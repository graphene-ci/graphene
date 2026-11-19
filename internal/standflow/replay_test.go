package standflow

import (
	"testing"
	"time"

	"go.temporal.io/sdk/worker"
)

// Stands are live across server upgrades: a history written by v0.2.37
// (accept, keep, extend, release, a TTL expiry, delete) must replay on the
// current code.
func TestReplayStandV0237(t *testing.T) {
	t.Parallel()
	replayer := worker.NewWorkflowReplayer()
	if err := New(time.Second).Register(replayer); err != nil {
		t.Fatal(err)
	}
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, "testdata/stand-v0.2.37.json"); err != nil {
		t.Fatal(err)
	}
}
