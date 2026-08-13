// Package v1 holds the kinds graphene itself puts into the cluster: the
// pipeline and its revisions, the run, and the probe that M1 uses to check
// the wiring. Everything about the world outside — machines at providers,
// clusters, buckets — is somebody else's kind, applied through Crossplane.
//
// This package is imported by the user's pipeline through pkg/pipeline, so
// it stays on apimachinery and nothing heavier. controller-runtime and
// client-go do not belong here and a test enforces it.
//
// +kubebuilder:object:generate=true
// +groupName=graphene-ci.dev
package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Group is the API group every kind of ours lives in.
const Group = "graphene-ci.dev"

// Version is the API version of this package.
const Version = "v1"

// The scheme entry points below are package variables because that is the
// shape every kubernetes API package has and every consumer expects:
// v1.AddToScheme(scheme) is how a client learns our kinds. Making them
// functions would rename a convention for no gain.
//
//nolint:gochecknoglobals // convention of every kubernetes API package
var (
	// GroupVersion names this package's kinds.
	GroupVersion = schema.GroupVersion{Group: Group, Version: Version}

	// SchemeBuilder collects our kinds for registration.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme teaches a scheme every kind of ours.
	AddToScheme = SchemeBuilder.AddToScheme
)

// Resource qualifies an unqualified resource name with our group.
func Resource(resource string) schema.GroupResource {
	return GroupVersion.WithResource(resource).GroupResource()
}

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&Pipeline{}, &PipelineList{},
		&PipelineRevision{}, &PipelineRevisionList{},
		&Run{}, &RunList{},
		&Probe{}, &ProbeList{},
		&Machine{}, &MachineList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)

	return nil
}

// LocalRef points at another record in the same namespace by name.
//
// Ours rather than corev1.LocalObjectReference so that this package keeps
// depending on apimachinery alone: one field is not worth a module in the
// import graph of every user's pipeline.
type LocalRef struct {
	// Name of the record referred to.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}
