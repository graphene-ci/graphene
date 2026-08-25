package authz

import "testing"

// A role naming a verb or kind the system does not have must fail when
// it is WRITTEN: a typo that silently grants nothing is a trap.
func TestRuleValidation(t *testing.T) {
	if err := (Rule{Verbs: []Verb{"delete"}, Kinds: []Kind{"pipeline"}}).Validate(); err != nil {
		t.Fatalf("a valid rule must pass: %v", err)
	}
	if err := (Rule{Verbs: []Verb{"destroy"}, Kinds: []Kind{"pipeline"}}).Validate(); err == nil {
		t.Fatal("an unknown verb must be refused")
	}
	if err := (Rule{Verbs: []Verb{"delete"}, Kinds: []Kind{"piepline"}}).Validate(); err == nil {
		t.Fatal("an unknown kind must be refused")
	}
	if err := (Rules{}).Validate(); err == nil {
		t.Fatal("a role without rules must be refused")
	}
}

// The user's own kinds are one group: a role grants "resource", never
// a vocabulary we do not own.
func TestKindOf(t *testing.T) {
	for ref, want := range map[string]Kind{
		"pipeline/delivery":                         KindPipeline,
		"revision/delivery.abc":                     KindRevision,
		"k8s.compute.yandex-cloud.../Instance/vm-1": KindResource,
		"docker/hello":                              KindResource,
		"workspace/ws":                              KindWorkspace,
	} {
		if got := KindOf(ref); got != want {
			t.Fatalf("KindOf(%q) = %q, want %q", ref, got, want)
		}
	}
}

// Authorization is additive, scoped by namespace, and explains itself.
func TestAuthorize(t *testing.T) {
	roles := Builtins()
	alice := Identity{Subject: Subject{SubjectUser, "alice"}, Groups: []string{"platform"}, Namespace: "default"}
	bindings := []Binding{
		{Role: "developer", Subjects: []Subject{{SubjectGroup, "platform"}}, Namespace: "default"},
	}
	if d := Authorize(alice, VerbRun, KindPipeline, bindings, roles); !d.Allowed {
		t.Fatalf("a developer must run pipelines: %s", d.Reason)
	}
	if d := Authorize(alice, VerbCreate, KindRole, bindings, roles); d.Allowed {
		t.Fatal("a developer must not hand out rights")
	}
	// The same binding does not reach another namespace.
	other := alice
	other.Namespace = "prod"
	if d := Authorize(other, VerbRun, KindPipeline, bindings, roles); d.Allowed {
		t.Fatal("a namespace-scoped binding must not reach another namespace")
	}
	// A caller with no binding at all is told exactly that.
	bob := Identity{Subject: Subject{SubjectUser, "bob"}, Namespace: "default"}
	d := Authorize(bob, VerbList, KindRun, bindings, roles)
	if d.Allowed || d.Reason == "" {
		t.Fatalf("an unbound caller must be refused with a reason: %+v", d)
	}
}

// A run's minted token may do the pipeline's work and nothing else.
func TestRunRoleIsNarrow(t *testing.T) {
	roles := Builtins()
	run := Identity{Subject: Subject{SubjectServiceAccount, "run/x"}, Namespace: "default"}
	bindings := []Binding{{Role: "run", Subjects: []Subject{run.Subject}, Namespace: "default"}}
	if d := Authorize(run, VerbCreate, KindResource, bindings, roles); !d.Allowed {
		t.Fatalf("a run must declare its resources: %s", d.Reason)
	}
	for _, forbidden := range []struct {
		verb Verb
		kind Kind
	}{
		{VerbActivate, KindRevision}, {VerbCreate, KindSecret},
		{VerbDelete, KindPipeline}, {VerbCreate, KindRoleBinding},
	} {
		if d := Authorize(run, forbidden.verb, forbidden.kind, bindings, roles); d.Allowed {
			t.Fatalf("a run must not %s %s", forbidden.verb, forbidden.kind)
		}
	}
}
