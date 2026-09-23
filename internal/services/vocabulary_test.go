package services

import (
	"encoding/json"
	"testing"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	"github.com/graphene-ci/temporal-entity/pkg/entity"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/converter"

	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

func visibilityRow(t *testing.T, workflowId string, status enums.WorkflowExecutionStatus, attrs map[string]any) *workflowpb.WorkflowExecutionInfo {
	t.Helper()
	dc := converter.GetDefaultDataConverter()
	fields := map[string]*commonpb.Payload{}
	for k, v := range attrs {
		p, err := dc.ToPayload(v)
		require.NoError(t, err)
		fields[k] = p
	}
	return &workflowpb.WorkflowExecutionInfo{
		Execution:        &commonpb.WorkflowExecution{WorkflowId: workflowId},
		Type:             &commonpb.WorkflowType{Name: "perf-nightly"},
		Status:           status,
		SearchAttributes: &commonpb.SearchAttributes{IndexedFields: fields},
	}
}

// One vocabulary: a run's phase comes out lowercase like every record's,
// and a closed entity workflow is a deleted record whatever its last upsert
// managed to say.
func TestVisibilityRowsSpeakOnePhaseVocabulary(t *testing.T) {
	run := resourceFromVisibility(visibilityRow(t, "run/nightly-1", enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, nil))
	require.Equal(t, "timed-out", run.GetPhase())
	require.Equal(t, "perf-nightly", run.GetLabels()["graphene.io/pipeline"])

	live := resourceFromVisibility(visibilityRow(t, "agent/db-1", enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
		map[string]any{entdefine.SearchAttrPhase.GetName(): "ready"}))
	require.Equal(t, "ready", live.GetPhase())

	closed := resourceFromVisibility(visibilityRow(t, "agent/db-1", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		map[string]any{entdefine.SearchAttrPhase.GetName(): "ready"}))
	require.Equal(t, "deleted", closed.GetPhase(), "a closed entity workflow is a deleted record")

	failedDelete := resourceFromVisibility(visibilityRow(t, "agent/db-1", enums.WORKFLOW_EXECUTION_STATUS_FAILED,
		map[string]any{entdefine.SearchAttrPhase.GetName(): "delete-failed"}))
	require.Equal(t, "delete-failed", failedDelete.GetPhase(), "a phase the record itself spelled out stays")
}

// A milestone the pipeline emitted is its own kind of event, named by the
// note, carrying the payload — not the signal envelope.
func TestMilestoneIsANoteEvent(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	note, err := dc.ToPayloads(map[string]json.RawMessage{
		"name":    json.RawMessage(`"stand.kept"`),
		"payload": json.RawMessage(`{"root":"agent/db-1"}`),
	})
	require.NoError(t, err)
	ev := translate(&historypb.HistoryEvent{
		EventId: 7,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
				SignalName: entity.NoteSignalName, Input: note,
			},
		},
	}, nil)
	require.Equal(t, "note", ev.GetKind())
	require.Equal(t, "stand.kept", ev.GetSubject())
	require.JSONEq(t, `{"root":"agent/db-1"}`, string(ev.GetInput()))

	other, err := dc.ToPayloads("x")
	require.NoError(t, err)
	plain := translate(&historypb.HistoryEvent{
		EventId: 8,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{SignalName: "poke", Input: other},
		},
	}, nil)
	require.Equal(t, "signal-received", plain.GetKind())
	require.Equal(t, "poke", plain.GetSubject())
}

// The selector path and the structural path agree on what "deleted" means.
func TestListQueryDeletedLiftsTheLiveFilter(t *testing.T) {
	q, err := listQuery("", selectorOf("agent", "deleted"))
	require.NoError(t, err)
	require.Equal(t, `EntityKind = 'agent' AND EntityPhase = 'deleted'`, q)
	q, err = listQuery("", selectorOf("agent", "ready"))
	require.NoError(t, err)
	require.Equal(t, `EntityKind = 'agent' AND EntityPhase = 'ready' AND ExecutionStatus = 'Running'`, q)
	q, err = listQuery("", selectorOf("run", "timed-out"))
	require.NoError(t, err)
	require.Equal(t, `EntityKind = 'run' AND ExecutionStatus = 'TimedOut'`, q)
	_, err = listQuery("", selectorOf("run", "Completed"))
	require.ErrorContains(t, err, "not a run phase")
}

func selectorOf(kind, phase string) *managementv1.Selector {
	return &managementv1.Selector{Kind: kind, Phase: phase}
}
