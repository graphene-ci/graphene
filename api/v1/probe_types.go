package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ProbeSpec is a record that becomes ready by itself after a while.
//
// It exists to check the wiring — record created, workflow woken by its
// readiness, run finished — without a cloud provider or an agent in the
// way. When something breaks, a Probe answers "is it the wiring or is it
// the provider" in one step.
type ProbeSpec struct {
	// After is how long to wait before declaring readiness. Zero means at
	// once.
	// +optional
	After metav1.Duration `json:"after,omitempty"`
}

// ProbeStatus carries readiness and nothing else.
type ProbeStatus struct {
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Probe is the throwaway kind M1 checks the wiring with.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Probe struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ProbeSpec `json:"spec,omitempty"`
	// +optional
	Status ProbeStatus `json:"status,omitempty"`
}

// ProbeList is a list of probes.
//
// +kubebuilder:object:root=true
type ProbeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Probe `json:"items"`
}
