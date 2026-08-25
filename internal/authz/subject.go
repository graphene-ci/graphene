package authz

// Subjects and the decision itself. A subject is who the caller IS —
// a person from the identity provider, one of their groups, or a
// service account of this installation. Bindings map subjects to
// roles; the decision is additive and never denies.

import (
	"fmt"
	"strings"
)

// SubjectKind separates the three contours of identity.
type SubjectKind string

const (
	// SubjectUser is a person, named by the provider's subject claim.
	SubjectUser SubjectKind = "user"
	// SubjectGroup is a group claim of the provider — the usual way to
	// grant rights to a team without listing people.
	SubjectGroup SubjectKind = "group"
	// SubjectServiceAccount is a machine of this installation: an
	// agent, a run, a script.
	SubjectServiceAccount SubjectKind = "sa"
)

// Subject is "kind:name" on the wire ("user:alice", "group:platform",
// "sa:agent-edge-1").
type Subject struct {
	Kind SubjectKind `json:"kind"`
	Name string      `json:"name"`
}

// ParseSubject reads the wire form.
func ParseSubject(s string) (Subject, error) {
	kind, name, ok := strings.Cut(s, ":")
	if !ok || name == "" {
		return Subject{}, fmt.Errorf("subject %q: want user:<sub>, group:<name> or sa:<id>", s)
	}
	switch SubjectKind(kind) {
	case SubjectUser, SubjectGroup, SubjectServiceAccount:
		return Subject{Kind: SubjectKind(kind), Name: name}, nil
	default:
		return Subject{}, fmt.Errorf("subject kind %q: want user, group or sa", kind)
	}
}

// String renders the wire form.
func (s Subject) String() string { return string(s.Kind) + ":" + s.Name }

// Identity is the authenticated caller: who they are, which groups
// they bring, and which namespace they are acting in.
type Identity struct {
	Subject Subject
	// Groups are the provider's group claims; each is a subject a
	// binding may name.
	Groups []string
	// Namespace is where this call acts.
	Namespace string
}

// Subjects lists every subject a binding may match for this caller.
func (i Identity) Subjects() []Subject {
	out := make([]Subject, 0, len(i.Groups)+1)
	out = append(out, i.Subject)
	for _, g := range i.Groups {
		out = append(out, Subject{Kind: SubjectGroup, Name: g})
	}
	return out
}

// Binding grants one role to subjects, in one namespace ("*" for every
// namespace — how a cluster admin is made).
type Binding struct {
	Role      string    `json:"role"`
	Subjects  []Subject `json:"subjects"`
	Namespace string    `json:"namespace"`
}

// Matches reports whether this binding applies to the caller.
func (b Binding) Matches(id Identity) bool {
	if b.Namespace != "*" && b.Namespace != id.Namespace {
		return false
	}
	for _, want := range b.Subjects {
		for _, have := range id.Subjects() {
			if want == have {
				return true
			}
		}
	}
	return false
}

// Decision is the answer of one authorization check.
type Decision struct {
	Allowed bool
	// Reason explains a refusal in the caller's terms.
	Reason string
}

// Authorize answers whether the caller may do verb on kind, given the
// bindings that apply and the roles they name. Additive: allowed when
// ANY applicable role allows it.
func Authorize(id Identity, verb Verb, kind Kind, bindings []Binding, roles map[string]Rules) Decision {
	matched := 0
	for _, b := range bindings {
		if !b.Matches(id) {
			continue
		}
		matched++
		if rules, ok := roles[b.Role]; ok && rules.Allows(verb, kind) {
			return Decision{Allowed: true}
		}
	}
	if matched == 0 {
		return Decision{Reason: fmt.Sprintf("%s has no role in namespace %q", id.Subject, id.Namespace)}
	}
	return Decision{Reason: fmt.Sprintf("%s may not %s %s in namespace %q", id.Subject, verb, kind, id.Namespace)}
}
