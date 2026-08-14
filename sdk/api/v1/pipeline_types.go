package v1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PipelineSpec is what a person decides about a pipeline as a whole,
// separately from any one version of its code.
type PipelineSpec struct {
	// Retention is how long finished runs of this pipeline are kept.
	// Empty means the system-wide setting applies.
	// +optional
	Retention *metav1.Duration `json:"retention,omitempty"`

	// ArtifactRetention is how long the bytes runs of this pipeline left
	// behind are kept. Separate from Retention because they are separate
	// costs: a run record is bytes in etcd, a report is megabytes in a
	// bucket, and the pipeline that wants its history for a year rarely
	// wants a year of dumps.
	// +optional
	ArtifactRetention *metav1.Duration `json:"artifactRetention,omitempty"`

	// Schedules start runs without anyone asking.
	// +optional
	// +listType=map
	// +listMapKey=name
	Schedules []Schedule `json:"schedules,omitempty"`
}

// Schedule is one regular start.
type Schedule struct {
	// Name is unique within the pipeline and names the schedule in logs
	// and in the runs it starts.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Cron says when, in the ordinary five-field notation.
	// +kubebuilder:validation:MinLength=1
	Cron string `json:"cron"`

	// Params the scheduled run starts with.
	// +optional
	Params *apiextensionsv1.JSON `json:"params,omitempty"`
}

// PipelineStatus is what the system knows about the pipeline.
type PipelineStatus struct {
	// LatestRevision names the revision pushed last. A run without an
	// explicit revision gets this one.
	// +optional
	LatestRevision string `json:"latestRevision,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Pipeline is a program that can be run, named and versioned as code.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=pl
// +kubebuilder:printcolumn:name="Latest",type=string,JSONPath=`.status.latestRevision`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Pipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec PipelineSpec `json:"spec,omitempty"`
	// +optional
	Status PipelineStatus `json:"status,omitempty"`
}

// PipelineList is a list of pipelines.
//
// +kubebuilder:object:root=true
type PipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Pipeline `json:"items"`
}

// PipelineRevisionSpec is one immutable version of a pipeline's code.
// Nothing here changes after creation: a run points at a revision, and
// "run it again" has to mean the same code, not whatever is there now.
type PipelineRevisionSpec struct {
	// PipelineRef names whose revision this is. Ownership is carried by
	// ownerReferences; this field is what a person and a selector read.
	PipelineRef LocalRef `json:"pipelineRef"`

	// Image is the pipeline's image BY DIGEST. A tag can be moved to
	// point at other bytes, and then repeating a run would stop meaning
	// repeating the same program.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// Queue is the Temporal task queue the worker of this revision
	// listens on. Unique per revision: a worker of an old revision must
	// not pick up work meant for a new one.
	// +kubebuilder:validation:MinLength=1
	Queue string `json:"queue"`
}

// PipelineRevisionStatus is what the revision's own worker reports about
// itself when it starts. Nobody else can know it: the parameter schema and
// the list of kinds live inside the pipeline's code.
type PipelineRevisionStatus struct {
	// Params is the JSON Schema of this revision's parameters.
	// +optional
	Params *apiextensionsv1.JSON `json:"params,omitempty"`

	// Requires lists the kinds a run of this revision applies. Missing
	// ones are refused before the run starts rather than halfway through.
	// +optional
	// +listType=atomic
	Requires []Requirement `json:"requires,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Requirement is one kind a pipeline needs the cluster to know.
type Requirement struct {
	// +kubebuilder:validation:MinLength=1
	Group string `json:"group"`
	// +kubebuilder:validation:MinLength=1
	Version string `json:"version"`
	// +kubebuilder:validation:MinLength=1
	Kind string `json:"kind"`
}

// PipelineRevision is one pushed version of a pipeline.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=plrev
// +kubebuilder:printcolumn:name="Pipeline",type=string,JSONPath=`.spec.pipelineRef.name`
// +kubebuilder:printcolumn:name="Queue",type=string,JSONPath=`.spec.queue`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type PipelineRevision struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec PipelineRevisionSpec `json:"spec,omitempty"`
	// +optional
	Status PipelineRevisionStatus `json:"status,omitempty"`
}

// PipelineRevisionList is a list of revisions.
//
// +kubebuilder:object:root=true
type PipelineRevisionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []PipelineRevision `json:"items"`
}
