package v1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RunPhase is where a run got to. It never goes backwards, and a run in a
// terminal phase stays: the record is the history, not a live process.
// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed;Canceled
type RunPhase string

// The phases a run passes through.
const (
	// RunPending means the record exists and the workflow has not started.
	RunPending RunPhase = "Pending"
	// RunRunning means the workflow is going.
	RunRunning RunPhase = "Running"
	// RunSucceeded means the pipeline returned without an error.
	RunSucceeded RunPhase = "Succeeded"
	// RunFailed means the pipeline returned an error or died for good.
	RunFailed RunPhase = "Failed"
	// RunCanceled means a person stopped it.
	RunCanceled RunPhase = "Canceled"
)

// Terminal answers whether nothing more will happen to a run in this phase.
func (p RunPhase) Terminal() bool {
	return p == RunSucceeded || p == RunFailed || p == RunCanceled
}

// RunSpec is the ask: this revision, with these parameters.
type RunSpec struct {
	// RevisionRef names the revision to execute — a revision and not a
	// pipeline, because a run is nailed to concrete code. Otherwise
	// "repeat this run" would mean "execute whatever is there now".
	RevisionRef LocalRef `json:"revisionRef"`

	// Params for this run, shaped by the revision's parameter schema.
	// +optional
	Params *apiextensionsv1.JSON `json:"params,omitempty"`

	// Cancel asks the run to stop.
	//
	// It lives in the spec rather than being a command because stopping a
	// run is a decision about the world, and decisions about the world are
	// records: a person, a UI and kubectl all say it the same way, and the
	// wish survives whoever expressed it going away.
	//
	// Asking to cancel is not the same as killing. A cancelled pipeline is
	// told to stop and gets to run its own teardown — that is what
	// run.Teardown on a disconnected context exists for. Killing is what
	// happens when the RECORD is deleted, and then we clean up ourselves.
	// +optional
	Cancel bool `json:"cancel,omitempty"`
}

// RunStatus is what became of the ask.
type RunStatus struct {
	// +optional
	Phase RunPhase `json:"phase,omitempty"`

	// WorkflowID is the run's workflow in Temporal. It always equals the
	// record's own name: that is what makes starting the workflow safe to
	// retry — the second attempt collides with the first instead of
	// starting a second run.
	// +optional
	WorkflowID string `json:"workflowID,omitempty"`

	// TemporalRunID identifies which attempt of that workflow is going
	// now. It changes on retry; WorkflowID does not.
	// +optional
	TemporalRunID string `json:"temporalRunID,omitempty"`

	// Result is what the pipeline returned. Small by design — a report
	// goes to artifact storage, not into a record every controller in the
	// cluster watches.
	// +optional
	Result *apiextensionsv1.JSON `json:"result,omitempty"`

	// Reason says why the run stopped, in the words of whoever knows.
	// +optional
	Reason string `json:"reason,omitempty"`

	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Run is one execution of a revision. It is not deleted when it finishes:
// it changes phase and stays as the history of what happened.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Revision",type=string,JSONPath=`.spec.revisionRef.name`
// +kubebuilder:printcolumn:name="Started",type=date,JSONPath=`.status.startedAt`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Run struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec RunSpec `json:"spec,omitempty"`
	// +optional
	Status RunStatus `json:"status,omitempty"`
}

// RunList is a list of runs.
//
// +kubebuilder:object:root=true
type RunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Run `json:"items"`
}
