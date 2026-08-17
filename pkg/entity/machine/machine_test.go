package machine

import (
	"testing"

	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/graphene/pkg/ref"
)

type fakeRegistry struct{ names []string }

func (f *fakeRegistry) RegisterWorkflow(any) {}
func (f *fakeRegistry) RegisterWorkflowWithOptions(_ any, opts workflow.RegisterOptions) {
	f.names = append(f.names, opts.Name)
}
func (f *fakeRegistry) RegisterDynamicWorkflow(any, workflow.DynamicRegisterOptions) {}

func TestDefinitionRegisters(t *testing.T) {
	reg := &fakeRegistry{}
	if err := Definition(Options{}).Register(reg); err != nil {
		t.Fatalf("register: %v", err)
	}
	if len(reg.names) != 1 || reg.names[0] != string(Kind) {
		t.Fatalf("registered names: %v", reg.names)
	}
}

func TestSpecValidate(t *testing.T) {
	cloud := &CloudSource{Provider: "yc"}
	ssh := &SSHSource{Host: "10.0.0.1", User: "root", KeyRef: ref.SecretRef{Name: "ssh-key"}}
	cases := []struct {
		name string
		spec Spec
		ok   bool
	}{
		{"cloud", Spec{Cloud: cloud}, true},
		{"ssh", Spec{SSH: ssh}, true},
		{"none", Spec{}, false},
		{"both", Spec{Cloud: cloud, SSH: ssh}, false},
		{"ssh without host", Spec{SSH: &SSHSource{User: "root"}}, false},
		{"cloud without provider", Spec{Cloud: &CloudSource{}}, false},
		{"bad owner", Spec{Cloud: cloud, Owner: "run"}, false},
		{"good owner", Spec{Cloud: cloud, Owner: ref.RunOwner("r-1")}, true},
	}
	for _, c := range cases {
		if err := c.spec.Validate(); (err == nil) != c.ok {
			t.Errorf("%s: err=%v want ok=%v", c.name, err, c.ok)
		}
	}
}

func TestOwned(t *testing.T) {
	if !(Spec{Cloud: &CloudSource{Provider: "yc"}}).Owned() {
		t.Error("cloud machine must be owned")
	}
	if (Spec{SSH: &SSHSource{Host: "h", User: "u"}}).Owned() {
		t.Error("recognized machine must not be owned")
	}
}
