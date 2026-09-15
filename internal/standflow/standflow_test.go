package standflow

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/graphene-ci/pipeline/pkg/ref"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

// Hundreds of transfers must retain their holdings without copying all of
// them into every deduplicated command response carried by Continue-as-New.
func TestAcceptResponsesStayBounded(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	if err := New(0).Register(env); err != nil {
		t.Fatal(err)
	}
	const count = 400
	completed := 0
	for i := 0; i < count; i++ {
		env.RegisterDelayedCallback(func() {
			payload, err := json.Marshal(AcceptCmd{Ref: ref.OwnerRef("artifact/" + refName(i)), Keep: 30 * 24 * time.Hour, From: "run/test"})
			if err != nil {
				t.Fatal(err)
			}
			env.UpdateWorkflow("accept", fmt.Sprint(i), &testsuite.TestUpdateCallback{
				OnReject: func(err error) { t.Error(err) },
				OnComplete: func(value any, err error) {
					if err != nil {
						t.Error(err)
						return
					}
					data, err := json.Marshal(value)
					if err != nil {
						t.Fatal(err)
					}
					var result CommandRes
					if err := json.Unmarshal(data, &result); err != nil {
						t.Fatal(err)
					}
					if len(data) > 32 || result.Count != i+1 {
						t.Errorf("response %d: %d bytes, count %d", i, len(data), result.Count)
					}
					completed++
				},
			}, map[string]any{"requestId": fmt.Sprint(i), "payload": json.RawMessage(payload)})
		}, time.Duration(i+1)*time.Second)
	}
	env.RegisterDelayedCallback(func() {
		value, err := env.QueryWorkflow("describe")
		if err != nil {
			t.Fatal(err)
		}
		var described struct{ State State }
		if err := value.Get(&described); err != nil {
			t.Fatal(err)
		}
		if len(described.State.Holdings) != count || completed != count {
			t.Errorf("holdings=%d completed=%d", len(described.State.Holdings), completed)
		}
		for _, holding := range described.State.Holdings {
			if holding.From != "run/test" || holding.KeepUntil == nil {
				t.Errorf("holding lost retention/origin: %+v", holding)
			}
		}
		env.CancelWorkflow()
	}, (count+1)*time.Second)
	env.ExecuteWorkflow(string(Kind), map[string]any{"spec": Spec{PipelineId: "test"}})
}

func refName(i int) string { return fmt.Sprintf("run-%08d-config", i) }

// Operator-supplied history stays outside the repository. Replay verifies
// compatibility with an already running stand before a server deployment.
func TestReplayStandHistory(t *testing.T) {
	path := os.Getenv("GRAPHENE_STAND_REPLAY_HISTORY")
	if path == "" {
		t.Skip("set GRAPHENE_STAND_REPLAY_HISTORY to a Temporal history JSON")
	}
	replayer := worker.NewWorkflowReplayer()
	if err := New(30 * time.Second).Register(replayer); err != nil {
		t.Fatal(err)
	}
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path); err != nil {
		t.Fatal(err)
	}
}
