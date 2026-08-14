package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ArtifactSpec is something a run produced and somebody will want later.
//
// The bytes are NOT here. A record that carried them would be a record the
// whole cluster copies on every watch, and a report is megabytes. What is
// here is where they are and what they are — enough to find them and to
// know they did not change.
type ArtifactSpec struct {
	// RunRef is who made it.
	RunRef LocalRef `json:"runRef"`

	// Name is what the pipeline called it.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Key is where the bytes are in storage. Derived from the run and the
	// name, so asking twice means one artifact rather than two.
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`

	// Until is when the bytes go away. Inherited if nobody said: the
	// pipeline's policy, and then the installation's. Finite at the top,
	// because storage that only grows is storage that one day stops.
	// +optional
	Until *metav1.Time `json:"until,omitempty"`
}

// ArtifactStatus is what came back from the machine that produced it.
type ArtifactStatus struct {
	// Digest is what the agent computed while uploading. It is how anyone
	// later can tell the bytes are the ones this run made.
	// +optional
	Digest string `json:"digest,omitempty"`

	// Size in bytes.
	// +optional
	Size int64 `json:"size,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Artifact is a report, a log bundle, a dump — anything a run leaves that
// is too big to be a value.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Run",type=string,JSONPath=`.spec.runRef.name`
// +kubebuilder:printcolumn:name="Size",type=integer,JSONPath=`.status.size`
// +kubebuilder:printcolumn:name="Until",type=string,JSONPath=`.spec.until`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Artifact struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ArtifactSpec `json:"spec,omitempty"`
	// +optional
	Status ArtifactStatus `json:"status,omitempty"`
}

// ArtifactList is a list of artifacts.
//
// +kubebuilder:object:root=true
type ArtifactList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Artifact `json:"items"`
}
