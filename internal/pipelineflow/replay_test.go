package pipelineflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/worker"
)

// A pipeline record lives for months, and its history is replayed by
// whatever worker serves it next. The fixture is the REAL history of the
// contour's pipeline as v0.2.23 wrote it — activation, a named firing
// decided by count-then-start, the run's lifetime — and every later
// version must replay it without a nondeterminism error, or the record
// answers "Workflow Task in failed state" to every command after the
// upgrade. Recapture the fixture only from an older version's contour
// (GRAPHENE_DUMP_PIPELINE_HISTORY), never from the current one: a
// fixture that already contains today's markers proves nothing.
func TestReplaysPipelineHistoryWrittenBeforeTheLookup(t *testing.T) {
	r := worker.NewWorkflowReplayer()
	require.NoError(t, New(time.Second).Register(r))
	require.NoError(t, r.ReplayWorkflowHistoryFromJSONFile(nil, "testdata/pipeline-v0.2.23-fire.json"))
}
