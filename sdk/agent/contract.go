// Package agent is the contract between the side that plans work and the
// side that performs it: activity names, the shapes of their arguments,
// and the signals that wake a workflow up.
//
// It is a package of its own for one reason. pkg/pipeline imports it to
// schedule work, and the system worker imports it to do the work — two
// binaries that must agree on every name and every field, and that must
// never depend on each other. So this package carries nothing heavy: no
// kubernetes client, no controller-runtime, no Temporal SDK. Names, types,
// and the rules for deriving one from another.
package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
	"time"
)

// Activity names. They are strings rather than function references because
// the two sides never link against each other, and because a name recorded
// in Temporal's history outlives the code that wrote it.
const (
	// ActivityApply puts a record into the cluster, owned by the run.
	ActivityApply = "graphene.Apply"
	// ActivityTeardown deletes everything the run still owns.
	ActivityTeardown = "graphene.Teardown"
	// ActivityKeep hands what the run owns to a stand that outlives it.
	ActivityKeep = "graphene.Keep"
)

// SystemQueue is where our activities run.
//
// It matters that this is not the pipeline's own queue. By default Temporal
// schedules an activity onto the queue of the workflow that asked for it —
// and then every pipeline image would have to carry a kubernetes client and
// the permission to write records. Applying to the cluster is our job, not
// the pipeline's, so it happens on our queue in our worker.
const SystemQueue = "graphene-system"

// SignalReady is the signal the operator sends when a record the run owns
// changes readiness. The workflow does not poll and does not hold a worker
// slot while a machine boots for three minutes: it sleeps in its history
// until this arrives.
const SignalReady = "graphene.ready"

// RunInput is what a pipeline's workflow is started with. The owner is
// passed rather than derived from the workflow id because ownership needs
// the run's UID, and an id is only a name.
type RunInput struct {
	Owner OwnerRef `json:"owner"`
	// Params as the person wrote them, shaped by the revision's schema.
	Params []byte `json:"params,omitempty"`
}

// OwnerRef points at the run that owns what an activity creates. The UID is
// what makes ownership real: a name can be reused by a later run, and a
// stale ownerReference would then hand somebody else's machines to it.
type OwnerRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid,omitempty"`
}

// ApplyInput asks for one record to exist.
type ApplyInput struct {
	// Name is the memo — what this thing is called INSIDE the run, chosen
	// by whoever wrote the pipeline. Activities run at-least-once, so the
	// second attempt must find the first attempt's record rather than
	// create a second one; the memo is what makes that possible.
	Name string `json:"name"`

	// Manifest is the record itself, as JSON. We do not parse it beyond
	// its apiVersion and kind: it is somebody else's kind, and looking
	// inside would mean knowing about their domain.
	Manifest []byte `json:"manifest"`

	// Owner is the run. What it owns dies with it.
	Owner OwnerRef `json:"owner"`
}

// ApplyOutput says what now exists.
type ApplyOutput struct {
	Ref ObjectRef `json:"ref"`
	// Created is false when the record was already there — that is the
	// normal answer on a retry, not a problem.
	Created bool `json:"created"`
}

// ObjectRef locates a record in the cluster completely.
type ObjectRef struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	UID        string `json:"uid,omitempty"`
}

// TeardownInput asks for everything a run owns to go away.
type TeardownInput struct {
	Owner OwnerRef `json:"owner"`

	// Refs is what the run created, as the run itself remembers it. The
	// workflow knows this without asking anybody: it is in its own
	// history. Searching the cluster instead would mean knowing every
	// kind a pipeline might have applied, which is the one thing we
	// promised not to know.
	//
	// ownerReferences are set on every record besides, so a Run deleted
	// by hand still takes its things with it. This is the eager path;
	// that is the safety net.
	Refs []ObjectRef `json:"refs,omitempty"`
}

// KeepInput asks for what the run made to outlive it.
//
// Not a delayed teardown: a delay still ends in a teardown, and whoever
// comes in the morning to look at the machine that failed finds nothing.
// The records change owner, and the new owner has an end of its own.
type KeepInput struct {
	Owner OwnerRef `json:"owner"`
	// Until is when the stand stops standing. There is no "never".
	Until time.Time `json:"until"`
	// Reason is why it was kept, for whoever finds it.
	// +optional
	Reason string `json:"reason,omitempty"`
	// +optional
	Refs []ObjectRef `json:"refs,omitempty"`
}

// KeepOutput names the stand that now answers for them.
type KeepOutput struct {
	Stand string `json:"stand"`
}

// TeardownOutput reports what was removed.
type TeardownOutput struct {
	Removed []ObjectRef `json:"removed,omitempty"`
}

// ReadySignal tells a workflow that one of its records changed readiness.
// It carries the memo rather than the cluster name so that the workflow can
// route it without knowing how names are derived.
type ReadySignal struct {
	Name   string `json:"name"`
	Ready  bool   `json:"ready"`
	Reason string `json:"reason,omitempty"`
	// Status is the record's status as JSON — what the pipeline reads to
	// learn an address, an id, whatever the provider filled in.
	Status []byte `json:"status,omitempty"`
}

// nameLimit is what kubernetes accepts for a metadata.name.
const nameLimit = 253

// suffixLen is how much of the memo's digest is kept. Eight hex characters
// is four bytes: enough that two memos in one run will not collide, short
// enough that a person reading `kubectl get` still sees their own name.
const suffixLen = 8

// unsafeChars is everything that cannot appear in a kubernetes name.
var unsafeChars = regexp.MustCompile(`[^a-z0-9-]+`)

// validName is the shape kubernetes requires of a metadata.name.
var validName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// ObjectName derives the record's name in the cluster from the run and the
// memo. It must be a pure function of those two: an activity that runs
// twice has to arrive at the same name both times, or the second attempt
// creates a second machine.
//
// The memo comes from a pipeline, which is code, so anything can be in it.
// The readable part is kept for the person reading `kubectl get`, and a
// digest of the original memo is appended so that two memos that clean up
// to the same text still get different records.
func ObjectName(owner OwnerRef, memo string) string {
	sum := sha256.Sum256([]byte(memo))
	suffix := hex.EncodeToString(sum[:])[:suffixLen]

	readable := unsafeChars.ReplaceAllString(strings.ToLower(memo), "-")
	readable = strings.Trim(readable, "-")

	head := owner.Name
	if readable != "" {
		head += "-" + readable
	}

	// The tail is fixed-length, so trimming the head is what keeps the
	// whole within the limit — and the digest keeps it unique even when
	// two long memos are trimmed to the same text.
	if room := nameLimit - len(suffix) - 1; len(head) > room {
		head = strings.Trim(head[:room], "-")
	}

	return head + "-" + suffix
}

// ValidName answers whether kubernetes would accept this as a name.
func ValidName(name string) bool {
	return name != "" && len(name) <= nameLimit && validName.MatchString(name)
}
