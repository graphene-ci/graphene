// Package authz is the installation's authorization vocabulary and its
// decision function: a right is verb × kind × namespace, additive,
// with no object-level grain. The vocabulary lives here so a door
// never invents a permission name and a role can be validated when it
// is written, not when it is used.
package authz

import (
	"fmt"
	"sort"
	"strings"
)

// Verb is what a caller wants to do. The set is closed: a role naming
// an unknown verb is refused at write time.
type Verb string

// The verbs of the whole system. Reads are separated from writes, and
// the dangerous ones (delete, transfer, invoke, activate) stand alone
// so a role can grant work without granting destruction.
const (
	// VerbGet reads one record in full.
	VerbGet Verb = "get"
	// VerbList lists records of a kind.
	VerbList Verb = "list"
	// VerbWatch follows changes (events, logs, streams).
	VerbWatch Verb = "watch"
	// VerbCreate declares a record (a workspace, a namespace, a secret).
	VerbCreate Verb = "create"
	// VerbUpdate changes what exists without replacing it (a file in a
	// workspace, a variable's value).
	VerbUpdate Verb = "update"
	// VerbDelete removes a record — and with it everything it owns.
	VerbDelete Verb = "delete"
	// VerbTransfer gives a record to a new owner.
	VerbTransfer Verb = "transfer"
	// VerbInvoke sends a record its own command.
	VerbInvoke Verb = "invoke"
	// VerbRun starts a run.
	VerbRun Verb = "run"
	// VerbBuild materializes a source revision.
	VerbBuild Verb = "build"
	// VerbActivate makes a revision the version automatic starts use.
	VerbActivate Verb = "activate"
)

// AllVerbs is the closed set, sorted.
var AllVerbs = []Verb{
	VerbActivate, VerbBuild, VerbCreate, VerbDelete, VerbGet,
	VerbInvoke, VerbList, VerbRun, VerbTransfer, VerbUpdate, VerbWatch,
}

// Kind is what the verb acts on: a system record kind, or "*".
type Kind string

// The system kinds. Library kinds (k8s.*, docker.*) are matched by
// KindResource — a role grants them as one group, because their names
// are the user's own vocabulary, not ours.
const (
	KindPipeline       Kind = "pipeline"
	KindRevision       Kind = "revision"
	KindRun            Kind = "run"
	KindTrigger        Kind = "trigger"
	KindStand          Kind = "stand"
	KindAgent          Kind = "agent"
	KindArtifact       Kind = "artifact"
	KindSecret         Kind = "secret"
	KindVar            Kind = "var"
	KindNamespace      Kind = "namespace"
	KindRole           Kind = "role"
	KindRoleBinding    Kind = "rolebinding"
	KindServiceAccount Kind = "serviceaccount"
	// KindResource covers every OTHER record — the library and user
	// kinds a pipeline declares.
	KindResource Kind = "resource"
	// KindAll is the wildcard.
	KindAll Kind = "*"
)

// AllKinds is the closed set, sorted.
var AllKinds = []Kind{
	KindAgent, KindArtifact, KindNamespace, KindPipeline, KindResource,
	KindRevision, KindRole, KindRoleBinding, KindRun, KindSecret,
	KindServiceAccount, KindStand, KindTrigger, KindVar,
}

// KindOf maps a record reference ("k8s.../vm-1", "pipeline/x") to the
// kind a rule speaks about.
func KindOf(ref string) Kind {
	name, _, _ := strings.Cut(ref, "/")
	k := Kind(name)
	for _, known := range AllKinds {
		if k == known {
			return known
		}
	}
	// Everything else is somebody's own kind — one group.
	return KindResource
}

// SystemKinds are the kinds whose records live in the system
// namespace, not in the caller's: they describe the installation, not
// a project inside it.
var SystemKinds = []Kind{KindNamespace, KindRole, KindRoleBinding, KindServiceAccount}

// IsSystem reports whether this kind's records live in the system
// namespace.
func IsSystem(k Kind) bool {
	for _, s := range SystemKinds {
		if k == s {
			return true
		}
	}
	return false
}

// Rule grants verbs on kinds. Both lists may hold "*".
type Rule struct {
	Verbs []Verb `json:"verbs"`
	Kinds []Kind `json:"kinds"`
}

// Validate refuses a rule naming a verb or kind the system does not
// have — a typo must fail when the role is written, not silently grant
// nothing (or everything) later.
func (r Rule) Validate() error {
	if len(r.Verbs) == 0 || len(r.Kinds) == 0 {
		return fmt.Errorf("a rule needs at least one verb and one kind")
	}
	for _, v := range r.Verbs {
		if v == "*" {
			continue
		}
		if !knownVerb(v) {
			return fmt.Errorf("unknown verb %q; the system has %s", v, joinVerbs(AllVerbs))
		}
	}
	for _, k := range r.Kinds {
		if k == KindAll {
			continue
		}
		if !knownKind(k) {
			return fmt.Errorf("unknown kind %q; the system has %s", k, joinKinds(AllKinds))
		}
	}
	return nil
}

// Allows reports whether the rule grants this verb on this kind.
func (r Rule) Allows(verb Verb, kind Kind) bool {
	return hasVerb(r.Verbs, verb) && hasKind(r.Kinds, kind)
}

// Rules is a role's rule set — additive, like every sane permission
// model: something is allowed when ANY rule allows it, and nothing
// denies.
type Rules []Rule

// Allows reports whether any rule grants this.
func (rs Rules) Allows(verb Verb, kind Kind) bool {
	for _, r := range rs {
		if r.Allows(verb, kind) {
			return true
		}
	}
	return false
}

// Validate checks every rule.
func (rs Rules) Validate() error {
	if len(rs) == 0 {
		return fmt.Errorf("a role needs at least one rule")
	}
	for i, r := range rs {
		if err := r.Validate(); err != nil {
			return fmt.Errorf("rule %d: %w", i, err)
		}
	}
	return nil
}

// Builtin roles every installation starts with. They are ordinary
// roles — an installation may add its own or replace these.
var (
	// RoleAdmin does everything in its namespace, including handing out
	// rights.
	RoleAdmin = Rules{{Verbs: []Verb{"*"}, Kinds: []Kind{KindAll}}}

	// RoleDeveloper does the work: writes pipelines, builds them, runs
	// them, reads everything — but does not hand out rights and does
	// not read secret values (nobody does; they are write-only).
	RoleDeveloper = Rules{
		{
			Verbs: []Verb{VerbGet, VerbList, VerbWatch, VerbCreate, VerbUpdate, VerbBuild, VerbRun, VerbActivate, VerbInvoke, VerbTransfer, VerbDelete},
			Kinds: []Kind{KindPipeline, KindRevision, KindRun, KindTrigger, KindStand, KindArtifact, KindResource},
		},
		{
			Verbs: []Verb{VerbGet, VerbList, VerbWatch},
			Kinds: []Kind{KindAgent, KindSecret, KindVar, KindNamespace},
		},
		{Verbs: []Verb{VerbCreate, VerbUpdate}, Kinds: []Kind{KindSecret, KindVar}},
	}

	// RoleViewer reads and follows, changes nothing.
	RoleViewer = Rules{{
		Verbs: []Verb{VerbGet, VerbList, VerbWatch},
		Kinds: []Kind{KindAll},
	}}

	// RoleAgent is what a machine's account gets: it reports itself and
	// serves its own record, nothing else.
	RoleAgent = Rules{{
		Verbs: []Verb{VerbGet, VerbUpdate, VerbInvoke},
		Kinds: []Kind{KindAgent},
	}}

	// RoleRun is what a RUN's minted token gets: the pipeline's own
	// work — declaring the resources it needs, reading its inputs.
	RoleRun = Rules{
		{
			Verbs: []Verb{VerbGet, VerbList, VerbWatch, VerbCreate, VerbUpdate, VerbDelete, VerbTransfer, VerbInvoke},
			Kinds: []Kind{KindResource, KindArtifact, KindAgent, KindStand},
		},
		{Verbs: []Verb{VerbGet, VerbList, VerbWatch}, Kinds: []Kind{KindPipeline, KindRun, KindRevision}},
	}
)

// Builtins names the roles an installation is created with.
func Builtins() map[string]Rules {
	return map[string]Rules{
		"admin":     RoleAdmin,
		"developer": RoleDeveloper,
		"viewer":    RoleViewer,
		"agent":     RoleAgent,
		"run":       RoleRun,
	}
}

func knownVerb(v Verb) bool {
	for _, k := range AllVerbs {
		if k == v {
			return true
		}
	}
	return false
}

func knownKind(k Kind) bool {
	for _, known := range AllKinds {
		if known == k {
			return true
		}
	}
	return false
}

func hasVerb(list []Verb, v Verb) bool {
	for _, item := range list {
		if item == "*" || item == v {
			return true
		}
	}
	return false
}

func hasKind(list []Kind, k Kind) bool {
	for _, item := range list {
		if item == KindAll || item == k {
			return true
		}
	}
	return false
}

func joinVerbs(vs []Verb) string {
	out := make([]string, 0, len(vs))
	for _, v := range vs {
		out = append(out, string(v))
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}

func joinKinds(ks []Kind) string {
	out := make([]string, 0, len(ks))
	for _, k := range ks {
		out = append(out, string(k))
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}
