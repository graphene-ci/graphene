package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SecretRef points at a value kept in a cluster secret.
//
// The value itself never appears in a record: what a pipeline writes is a
// name, and whoever needs the value resolves it at the moment of use and
// does not hand it back.
type SecretRef struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// Key inside the secret. Empty means the only key there.
	// +optional
	Key string `json:"key,omitempty"`
}

// MachineIntentSpec asks for an agent to appear on a machine that already
// exists.
//
// This is the other half of the promise: the system works with what it did
// not create. A machine is there, ssh to it is there — the installation is
// performed by the control plane rather than by whoever built the machine.
type MachineIntentSpec struct {
	// Address is host:port. Port may be omitted, and then it is 22.
	// +kubebuilder:validation:MinLength=1
	Address string `json:"address"`

	// User to log in as.
	// +kubebuilder:validation:MinLength=1
	User string `json:"user"`

	// Key names the secret holding the private key to log in with.
	Key SecretRef `json:"key"`

	// HostKey is the machine's public key, as one line of known_hosts or
	// authorized_keys:
	//
	//	ssh-keyscan -t ed25519 10.0.0.7
	//
	// Required, and deliberately so. Trust on first use is what a person
	// at a terminal does; this is a control plane opening a root shell and
	// feeding it a script with an installation token in it. Whoever
	// answers at that address without this would get both.
	// +kubebuilder:validation:MinLength=1
	HostKey string `json:"hostKey"`

	// Script is what to run there. The pipeline gets it from the SDK —
	// the same script a cloud machine receives through user-data, because
	// two scripts would drift and the rarer one would be the broken one.
	// +kubebuilder:validation:MinLength=1
	Script string `json:"script"`
}

// MachineIntentStatus says whether the agent got there.
type MachineIntentStatus struct {
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// MachineIntent is an agent asked for on a machine somebody else made.
//
// It is not the machine. The machine describes itself through Machine when
// its agent connects; this record only says "go there and install", and it
// is done once it has.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mi
// +kubebuilder:printcolumn:name="Address",type=string,JSONPath=`.spec.address`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type MachineIntent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec MachineIntentSpec `json:"spec,omitempty"`
	// +optional
	Status MachineIntentStatus `json:"status,omitempty"`
}

// MachineIntentList is a list of intents.
//
// +kubebuilder:object:root=true
type MachineIntentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []MachineIntent `json:"items"`
}
