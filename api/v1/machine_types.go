package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TaintEffect says what a taint does to work that does not tolerate it.
// +kubebuilder:validation:Enum=NoSchedule;NoExecute
type TaintEffect string

// What a taint can do.
const (
	// TaintNoSchedule keeps new work away and leaves running work alone.
	TaintNoSchedule TaintEffect = "NoSchedule"
	// TaintNoExecute also stops what is already running there.
	TaintNoExecute TaintEffect = "NoExecute"
)

// Taint keeps work off a machine unless it says it can live with this.
//
// Ours rather than corev1.Taint for the same reason as LocalRef: this
// package travels into the user's pipeline through pkg/pipeline, and three
// fields are not worth k8s.io/api in that import graph.
type Taint struct {
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
	// +optional
	Value  string      `json:"value,omitempty"`
	Effect TaintEffect `json:"effect"`
}

// MachineSpec is what a PERSON decides about a machine. Everything the
// agent knows is in the status: a machine describes itself, and what
// somebody wants of it is a different thing from what it is.
type MachineSpec struct {
	// Taints keep work off this machine unless it tolerates them.
	// +optional
	// +listType=atomic
	Taints []Taint `json:"taints,omitempty"`
}

// MachineStatus is what the agent reports about the machine it runs on.
type MachineStatus struct {
	// Queue is where this installation of the agent listens. Its name is
	// agent.InstallationQueue of the installation — a pure function, which
	// is what lets a pipeline schedule a step before the machine exists.
	// +optional
	Queue string `json:"queue,omitempty"`

	// Facts are what the agent found: os, arch, kernel, docker=27.3.
	// Arbitrary strings on purpose — a fact is whatever the machine
	// turned out to have, and the list is not ours to close.
	//
	// Selecting machines by fact needs them as labels, which means
	// projecting these into metadata under our prefix. That is M5's
	// business, together with selectors and tolerations; doing it now
	// would be a second writer of the same truth with no reader.
	// +optional
	Facts map[string]string `json:"facts,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Machine is something that connected.
//
// The agent writes it, the way a kubelet writes its Node: for a cloud VM
// nobody has to create one, the agent that comes up creates it. It has no
// owner, and that is deliberate — the hardware outlives the run that used
// it. A machine that belonged to a run would be deleted with it while the
// iron kept running with an agent on it and nobody to answer for it.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="OS",type=string,JSONPath=`.status.facts.os`
// +kubebuilder:printcolumn:name="Arch",type=string,JSONPath=`.status.facts.arch`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Machine struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Spec MachineSpec `json:"spec,omitempty"`
	// +optional
	Status MachineStatus `json:"status,omitempty"`
}

// MachineList is a list of machines.
//
// +kubebuilder:object:root=true
type MachineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Machine `json:"items"`
}
