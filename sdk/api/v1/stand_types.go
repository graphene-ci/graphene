package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StandSpec is what a run left standing.
//
// A stand exists because "keep this for a day" cannot mean "delay the
// teardown": a delay still ends in a teardown, and the person who comes in
// the morning to look at the machine that failed the test finds nothing.
// It has to mean the machines outlive the run — which means somebody else
// answers for them.
//
// That somebody is this record, and it has an end. The rule from FORM
// holds: an owner must have a reason to die. Without one, "keep for a day"
// becomes "keep forever", and forever is what an empty cloud account turns
// into a full one.
type StandSpec struct {
	// Until is when the stand stops standing. There is no "never".
	Until metav1.Time `json:"until"`

	// RunRef says which run left it. For a person reading `kubectl get`
	// and wondering what this is and why it is here.
	RunRef LocalRef `json:"runRef"`

	// Reason is why it was kept, in the words of whoever kept it.
	// +optional
	Reason string `json:"reason,omitempty"`
}

// StandStatus is what the system knows about the stand.
type StandStatus struct {
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Stand is what a run left behind on purpose.
//
// Everything it owns dies with it, through the cluster's own collector —
// the same mechanism that ties records to a run, pointed at a different
// owner.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Until",type=string,JSONPath=`.spec.until`
// +kubebuilder:printcolumn:name="Run",type=string,JSONPath=`.spec.runRef.name`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Stand struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec StandSpec `json:"spec,omitempty"`
	// +optional
	Status StandStatus `json:"status,omitempty"`
}

// StandList is a list of stands.
//
// +kubebuilder:object:root=true
type StandList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Stand `json:"items"`
}
