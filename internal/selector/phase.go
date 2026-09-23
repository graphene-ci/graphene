package selector

import (
	"fmt"
	"sort"
	"strings"

	"go.temporal.io/api/enums/v1"
)

// Phase of a run in the ONE vocabulary every record speaks: lowercase,
// hyphenated. Temporal's own enum names (Completed, TimedOut,
// WORKFLOW_EXECUTION_STATUS_RUNNING) never leave the door — a consumer
// knows one dictionary, for a run and an agent alike.
const (
	PhaseRunning        = "running"
	PhaseCompleted      = "completed"
	PhaseFailed         = "failed"
	PhaseCanceled       = "canceled"
	PhaseTerminated     = "terminated"
	PhaseTimedOut       = "timed-out"
	PhaseContinuedAsNew = "continued-as-new"
	// PhaseDeleted is where every entity record's life ends.
	PhaseDeleted = "deleted"
)

var runPhaseOf = map[enums.WorkflowExecutionStatus]string{
	enums.WORKFLOW_EXECUTION_STATUS_RUNNING:          PhaseRunning,
	enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:        PhaseCompleted,
	enums.WORKFLOW_EXECUTION_STATUS_FAILED:           PhaseFailed,
	enums.WORKFLOW_EXECUTION_STATUS_CANCELED:         PhaseCanceled,
	enums.WORKFLOW_EXECUTION_STATUS_TERMINATED:       PhaseTerminated,
	enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:        PhaseTimedOut,
	enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW: PhaseContinuedAsNew,
}

// executionStatusOf is the way back: a phase in a selector becomes the
// ExecutionStatus value visibility indexes.
var executionStatusOf = map[string]string{
	PhaseRunning:        "Running",
	PhaseCompleted:      "Completed",
	PhaseFailed:         "Failed",
	PhaseCanceled:       "Canceled",
	PhaseTerminated:     "Terminated",
	PhaseTimedOut:       "TimedOut",
	PhaseContinuedAsNew: "ContinuedAsNew",
}

// RunPhase renders an execution status as the run's phase.
func RunPhase(s enums.WorkflowExecutionStatus) string {
	if p, ok := runPhaseOf[s]; ok {
		return p
	}
	return strings.ToLower(strings.TrimPrefix(s.String(), "WORKFLOW_EXECUTION_STATUS_"))
}

// ExecutionStatus translates a run phase of a selector into visibility's
// ExecutionStatus value; an unknown word is refused with the words that
// would have worked.
func ExecutionStatus(phase string) (string, error) {
	if v, ok := executionStatusOf[phase]; ok {
		return v, nil
	}
	known := make([]string, 0, len(executionStatusOf))
	for k := range executionStatusOf {
		known = append(known, k)
	}
	sort.Strings(known)
	return "", fmt.Errorf("phase %q is not a run phase; one of %s", phase, strings.Join(known, ", "))
}
